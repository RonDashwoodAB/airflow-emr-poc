# Install The Astro CLI
* Run to install the Astro CLI `curl -sSL https://install.astronomer.io | sudo bash`.
* To verify successful install, run `astro`. This should output information about the CLI.
 

# Local Astro Development
To start a local Astro/Airflow cluster for development, simply run `astro dev start` in the root of the project directory. After a few seconds, you'll be able to access the Airflow UI at `localhost:8080`.

For info on when you need to restart Airflow vs. what changes will be picked up automatically, see [here](https://www.astronomer.io/docs/cli-quickstart/#code-changes).

# Deploy To Remote Astro
## Authentication
Before you can deploy to a cluster, you need to authenticate.
1. Run `astro auth login gcp0001.us-east4.astronomer.io`. Leave blank if you are using OAuth (e.g., Google account authentication)
1. Paste the OAuth token when asked for it.

## Deploy
1. Determine the deployment you want to deploy to. To see the available deployments, run `astro deployment list`.
1. To deploy to a given deployment, run `astro deploy <deployment-name>`

# AWS Setup

## Service Account User
You need to create a service account user that will be used to create resources in AWS. The user should have a policy attached. Use the policy defined in the `iam-policy.json` file in the project. *NOTE:* The policy has IP address filtering, so you may need to adjust the IP addresses to the machine(s) you are using.

Create API credentials for the service account user. They will be needed in Airflow.

## Set Credentials In Airflow
To make the POC work, you need to configure credentials in `Admin->Connections` of Airflow. You need to create two connections.

### Amazon Web Services Connection
Create an `Amazon Web Services` type connection called `s3`. The `Login` field of the connection is the API key. The `Password` field of the connection is the API secret. Under the `Extra` section, enter the following json:
```json
{"region_name": "us-west-2"}
```
Make sure you use the region you plan to launch the EMR cluster in. 

### Elastic MapReduce Connection
Create an `Elastic MapReduce` type connection called `emr`. The `Login` field of the connection is the API key. The `Password` field of the connection is the API secret. Under the `Extra` section, enter the following json:
```json
{"VisibleToAllUsers": true}
```

# References
[Astro CLI Quickstart](https://www.astronomer.io/docs/cli-quickstart/)