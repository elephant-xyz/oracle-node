Infra scripts and templates for the MWAA environment.

Files:

- `mwaa-public-network.yaml`: CloudFormation template for VPC, SQS, S3, and MWAA.
- `startup.sh`: MWAA startup script to install prerequisites on workers/schedulers.
- `pyproject.toml`: Python deps source used to compile `requirements.txt` for MWAA.
- `deploy.sh`: Script to create/update the stack, upload startup + requirements, and wire versions.
