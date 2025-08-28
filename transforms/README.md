Put your transform scripts here. The DAG expects a single zip of this folder uploaded to S3.

Use `./scripts/update-transforms.sh` to zip and upload them, and automatically update the Airflow variable `elephant_scripts_s3_uri` to point to the new S3 location.

