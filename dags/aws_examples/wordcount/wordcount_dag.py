import tarfile
import os
import logging
import tempfile

from airflow import DAG
from airflow.contrib.operators.emr_create_job_flow_operator import EmrCreateJobFlowOperator
from airflow.contrib.operators.emr_add_steps_operator import EmrAddStepsOperator
from airflow.contrib.sensors.emr_step_sensor import EmrStepSensor
from airflow.contrib.operators.emr_terminate_job_flow_operator import EmrTerminateJobFlowOperator
from airflow.operators.python_operator import PythonOperator
from airflow.hooks.S3_hook import S3Hook
from util import date_util
from datetime import timedelta
from contextlib import closing

# NOTE: the connection must set a region in the extra field:
#    {"region_name": "us-west-2"}
AWS_CONNECTION_ID = 's3'
# NOTE: the connection must set the VisibleToAllUsers flag to true in the extra field if you want to see the cluster
# in the AWS console:
#    {"VisibleToAllUsers": true}
EMR_CONNECTION_ID = 'emr'
REGION = 'us-west-2'
S3_BUCKET_NAME = 'allbirds-astronomer-airflow-poc-data'
S3_BUCKET_PATH = f's3://{S3_BUCKET_NAME}'
S3N_BUCKET_PATH = f's3n://{S3_BUCKET_NAME}'

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 0,
    'retry_delay': timedelta(minutes=5)
}

SPARK_TEST_STEPS = [
    {
        'Name': 'calc_wordcount',
        'ActionOnFailure': 'CONTINUE',
        'HadoopJarStep': {
            'Jar': 'command-runner.jar',
            'Args': [
                'spark-submit',
                '/home/hadoop/wordcount/wordcount.py',
            ]
        }
    }
]

JOB_FLOW_OVERRIDES = {
    'Name': 'wordcount_cluster',
    'LogUri': f'{S3_BUCKET_PATH}/log.txt',
    'ReleaseLabel': 'emr-5.29.0',
    'Instances': {
        'InstanceGroups': [
            {
                'Name': 'Master nodes',
                'Market': 'ON_DEMAND',
                'InstanceRole': 'MASTER',
                'InstanceType': 'm4.large',
                'InstanceCount': 1,
            },
            {
                'Name': 'Slave nodes',
                'Market': 'ON_DEMAND',
                'InstanceRole': 'CORE',
                'InstanceType': 'm4.large',
                'InstanceCount': 1,
            }
        ],
        'KeepJobFlowAliveWhenNoSteps': True,
        'TerminationProtected': False
    },
    'Applications': [{
        'Name': 'Spark'
    }],
    'JobFlowRole': 'EMR_EC2_DefaultRole',
    'ServiceRole': 'EMR_DefaultRole',
    'VisibleToAllUsers': True,
    'BootstrapActions': [
        {
            'Name': 'bootstrap-cluster',
            'ScriptBootstrapAction': {
                'Path': f'{S3N_BUCKET_PATH}/emr-bootstrap.sh',
                'Args': [
                    f'{S3_BUCKET_PATH}',
                ]
            }
        },
    ]
}


def get_current_dag_dir():
    return os.path.dirname(os.path.abspath(__file__))


def build_tar_dependency() -> str:
    base_path = get_current_dag_dir()
    tar_base_path = tempfile.gettempdir()
    tar_path = os.path.join(tar_base_path, 'wordcount.tar.gz')

    if os.path.exists(tar_path):
        logging.info('Deleting previous dependencies... %s' % tar_path)
        os.remove(tar_path)

    # Create tar file
    with closing(tarfile.open(tar_path, 'w:gz')) as t_file:
        for f in os.listdir(base_path):
            file_path = os.path.join(base_path, f)
            if os.path.isdir(file_path) is False:
                t_file.add(file_path, arcname=f)

        for f in t_file.getnames():
            logging.info("Added %s to tar-file" % f)

        # t_file.close()
    return tar_path


def upload_cluster_dependencies_to_s3():
    base_path = get_current_dag_dir()

    s3_hook = S3Hook(aws_conn_id=AWS_CONNECTION_ID)

    upload_files_paths = [
        os.path.join(base_path, 'output', 'emr-bootstrap.sh'),
        os.path.join(base_path, 'output', 'lorem-ipsum.txt'),
        build_tar_dependency()
    ]

    for file_path in upload_files_paths:
        s3_hook.load_file(file_path,
                          key=os.path.basename(file_path),
                          bucket_name=S3_BUCKET_NAME,
                          replace=True,
                          encrypt=False)
        logging.info('Uploaded dependency %s to %s' % (file_path, S3_BUCKET_NAME))


with DAG(
        dag_id='wordcount_dag',
        default_args=default_args,
        dagrun_timeout=timedelta(hours=2),
        schedule_interval=None,
        start_date=date_util.get_dag_start_date(),
        catchup=False
) as dag:
    upload_cluster_dependencies = PythonOperator(
        task_id='upload_cluster_dependencies',
        python_callable=upload_cluster_dependencies_to_s3,
        dag=dag)

    create_emr_cluster_task_id = 'create_emr_cluster'
    create_emr_cluster = EmrCreateJobFlowOperator(
        task_id=create_emr_cluster_task_id,
        job_flow_overrides=JOB_FLOW_OVERRIDES,
        aws_conn_id=AWS_CONNECTION_ID,
        emr_conn_id=EMR_CONNECTION_ID,
        region_name=REGION
    )

    count_words_with_emr_task_id = 'count_words_with_emr'
    count_words_with_emr = EmrAddStepsOperator(
        task_id=count_words_with_emr_task_id,
        job_flow_id=f"{{{{ task_instance.xcom_pull(task_ids='{create_emr_cluster_task_id}', key='return_value') }}}}",
        aws_conn_id=AWS_CONNECTION_ID,
        steps=SPARK_TEST_STEPS,
    )

    wait_for_emr_to_count_words = EmrStepSensor(
        task_id='wait_for_emr_to_count_words',
        job_flow_id=f"{{{{ task_instance.xcom_pull('{create_emr_cluster_task_id}', key='return_value') }}}}",
        step_id=f"{{{{ task_instance.xcom_pull(task_ids='{count_words_with_emr_task_id}', key='return_value')[0] }}}}",
        aws_conn_id=AWS_CONNECTION_ID,
    )

    terminate_emr_cluster = EmrTerminateJobFlowOperator(
        task_id='terminate_emr_cluster',
        job_flow_id=f"{{{{ task_instance.xcom_pull(task_ids='{create_emr_cluster_task_id}', key='return_value') }}}}",
        aws_conn_id=AWS_CONNECTION_ID,
    )

    upload_cluster_dependencies >> create_emr_cluster >> count_words_with_emr \
        >> wait_for_emr_to_count_words >> terminate_emr_cluster