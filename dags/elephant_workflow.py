"""
SQS-Triggered Elephant Workflow DAG
====================================

Event-driven workflow for processing elephant data using S3 event notifications
via SQS with parallel processing using dynamic task groups.
"""

import csv
import json
import logging
import shutil
import subprocess
import tempfile
import zipfile
from concurrent.futures import ThreadPoolExecutor
from datetime import timedelta
from pathlib import Path
from typing import Any, TypedDict

import pendulum
from airflow import DAG
from airflow.decorators import task, task_group
from airflow.io.path import ObjectStoragePath
from airflow.models import Variable
from airflow.providers.amazon.aws.sensors.sqs import SqsSensor

logger = logging.getLogger("airflow.task")


class WorkflowState(TypedDict):
    """State object passed between tasks"""

    input_zip_uri: str
    output_base_uri: str
    run_id: str
    seed_output_uri: str | None
    county_output_uri: str | None
    combined_output_uri: str | None
    # URIs for individual hashed zips produced by the hash step
    seed_hash_zip_uri: str | None
    county_hash_zip_uri: str | None
    hash_results_uri: str | None
    hash_csv_uri: str | None
    submission_csv_uri: str | None
    property_cid: str | None
    submission_result: dict[str, Any] | None
    error: str | None
    error_step: str | None


class FileProcessingInfo(TypedDict):
    """Information about a file to process from SQS"""

    input_zip_uri: str
    receipt_handle: str
    message_id: str
    bucket_name: str
    object_key: str


# Helper functions


def run_command(cmd: list[str], cwd: str, timeout: int = 300) -> subprocess.CompletedProcess[str]:
    """Execute a command with timeout and error handling"""
    logger.info(f"Running command: {' '.join(cmd)}")
    result = subprocess.run(cmd, capture_output=True, text=True, timeout=timeout, cwd=cwd)
    if result.returncode != 0:
        errors_csv = Path(cwd) / "submit_errors.csv"
        if errors_csv.exists():
            with open(errors_csv, "r") as f:
                reader = csv.DictReader(f)
                error_rows = list(reader)
            if error_rows:
                logger.error("Validation errors detected:")
                for row in error_rows:
                    logger.error(json.dumps(row))
                raise Exception("Validation failed: submit_errors.csv contains errors")
            # Remove empty errors file to avoid stale reads
            try:
                errors_csv.unlink(missing_ok=True)
            except Exception:
                pass
        cli_logs = Path(cwd) / "elephant-cli.log"
        if cli_logs.exists():
            with open(cli_logs, "r") as f:
                logger.error(f"elephant-cli logs: \n{f.read()}")
            cli_logs.unlink()

            raise Exception("elephant-cli failed: elephant-cli.log contains errors")
        logger.error(f"Command failed with stderr: {result.stderr}. \n . Strdout: {result.stdout}")
        raise Exception(f"Command failed: {result.stderr}")
    return result


def download_from_s3(s3_uri: str, local_path: Path, conn_id: str = "aws_default") -> None:
    """Download a file from S3 to local path"""
    logger.info(f"Downloading from S3: {s3_uri} to {local_path}")
    src = ObjectStoragePath(s3_uri, conn_id=conn_id)
    local_path.parent.mkdir(parents=True, exist_ok=True)
    with src.open("rb") as r, open(local_path, "wb") as w:
        count: int = w.write(r.read())
        if count == 0:
            raise Exception(f"Failed to download {s3_uri} to {local_path}")


def upload_to_s3(local_path: Path, s3_uri: str, conn_id: str = "aws_default") -> None:
    """Upload a file from local path to S3"""
    logger.info(f"Uploading to S3: {local_path} to {s3_uri}")
    dest = ObjectStoragePath(s3_uri, conn_id=conn_id)
    dest.parent.mkdir(parents=True, exist_ok=True)
    with open(local_path, "rb") as r, dest.open("wb") as w:
        count: int = w.write(r.read())
        if count == 0:
            raise Exception(f"Failed to upload {local_path} to {s3_uri}")


def extract_zip(zip_path: Path, extract_to: Path) -> None:
    """Extract a zip file to a directory"""
    logger.info(f"Extracting {zip_path} to {extract_to}")
    extract_to.mkdir(parents=True, exist_ok=True)
    with zipfile.ZipFile(zip_path, "r") as zip_ref:
        zip_ref.extractall(extract_to)


def create_zip(source_dir: Path, zip_path: Path) -> None:
    """Create a zip file from a directory"""
    logger.info(f"Creating zip {zip_path} from {source_dir}")
    zip_path.parent.mkdir(parents=True, exist_ok=True)
    with zipfile.ZipFile(zip_path, "w", zipfile.ZIP_DEFLATED) as zip_ref:
        for file_path in source_dir.rglob("*"):
            if file_path.is_file():
                arcname = file_path.relative_to(source_dir)
                zip_ref.write(file_path, arcname)


def find_file_with_extension(directory: Path, extension: str) -> Path | None:
    """Find the first file with given extension in directory"""
    for file_path in directory.rglob(f"*{extension}"):
        if file_path.is_file():
            return file_path
    return None


# Task implementations


@task(retries=2, retry_delay=timedelta(seconds=30))
def parse_sqs_messages(messages_input: Any) -> list[FileProcessingInfo]:
    """Parse SQS messages to extract S3 event information"""
    # Log the type and content of messages_input for debugging
    logger.info(f"Received messages_input of type: {type(messages_input).__name__}")

    # The SqsSensor returns messages directly as a list
    # If no messages, it may return None
    if messages_input is None:
        logger.info("No messages to parse from SQS (None input)")
        return []

    messages: list[dict[str, Any]]
    if isinstance(messages_input, list):
        messages = messages_input
        logger.info(f"Processing {len(messages)} messages from list input")
    elif isinstance(messages_input, dict):
        # Handle case where messages might be wrapped in a dict
        if "messages" in messages_input:
            messages = messages_input.get("messages") or []
            logger.info(f"Processing {len(messages)} messages from dict['messages'] input")
        else:
            logger.warning(f"Dict input without 'messages' key: {messages_input.keys()}")
            return []
    else:
        logger.warning(f"Unexpected SQS XCom format: {type(messages_input).__name__}; trying to iterate anyway")
        # Try to iterate anyway in case it's iterable
        try:
            messages = list(messages_input) if messages_input else []
            logger.info(f"Converted to list with {len(messages)} messages")
        except (TypeError, ValueError):
            logger.error(f"Cannot iterate over messages_input of type {type(messages_input).__name__}")
            return []

    files_to_process: list[FileProcessingInfo] = []

    for message in messages:
        try:
            body = json.loads(message.get("Body", "{}"))

            # Handle S3 event notification format
            if "Records" in body:
                for record in body["Records"]:
                    if "s3" in record:
                        s3_info = record["s3"]
                        bucket_name = s3_info["bucket"]["name"]
                        object_key = s3_info["object"]["key"]

                        # S3 event notifications already filter by prefix and suffix
                        # We just need to validate it's a .zip file as a safety check
                        if object_key.endswith(".zip"):
                            file_info = FileProcessingInfo(
                                input_zip_uri=f"s3://{bucket_name}/{object_key}",
                                receipt_handle=message["ReceiptHandle"],
                                message_id=message["MessageId"],
                                bucket_name=bucket_name,
                                object_key=object_key,
                            )
                            files_to_process.append(file_info)
                            logger.info(f"Found file to process: {object_key}")
                        else:
                            logger.info(f"Skipping non-zip file: {object_key}")
        except Exception as e:
            logger.error(f"Error parsing message {message.get('MessageId', 'unknown')}: {str(e)}")
            continue

    logger.info(f"Total files to process: {len(files_to_process)}")
    return files_to_process


@task_group()
def process_single_file(file_info: dict):
    """Process a single file through the entire pipeline"""

    @task(task_id="initialize_state", multiple_outputs=False)
    def initialize_state(file_data: dict) -> WorkflowState:
        """Initialize the workflow state from file info"""
        output_base_uri = Variable.get("elephant_output_base_uri", default_var="s3://elephant-outputs")

        # Create a unique run ID with timestamp, message ID, and UUID to prevent any conflicts
        from datetime import datetime
        import uuid

        timestamp = datetime.utcnow().isoformat()
        unique_suffix = str(uuid.uuid4())[:8]
        run_id = f"{timestamp}/{unique_suffix}"
        logger.info(f"Run ID: {run_id}")
        logger.info(f"Processing file: {file_data['input_zip_uri']}")

        state = WorkflowState(
            input_zip_uri=file_data["input_zip_uri"],
            output_base_uri=output_base_uri.rstrip("/"),
            run_id=run_id,
            seed_output_uri=None,
            county_output_uri=None,
            combined_output_uri=None,
            seed_hash_zip_uri=None,
            county_hash_zip_uri=None,
            hash_results_uri=None,
            hash_csv_uri=None,
            submission_csv_uri=None,
            property_cid=None,
            submission_result=None,
            error=None,
            error_step=None,
        )
        return state

    @task(task_id="prepare_submission", multiple_outputs=False, retries=2, retry_delay=timedelta(minutes=1))
    def prepare_submission(state: WorkflowState) -> WorkflowState:
        """Prepare submission CSV from hash results"""
        with tempfile.TemporaryDirectory() as tmpdir:
            try:
                tmp_path = Path(tmpdir)

                logger.info("Downloading hash results from S3")
                upload_results_csv = tmp_path / "upload_results.csv"

                hash_csv_uri = state.get("hash_csv_uri")
                if hash_csv_uri:
                    logger.info("Downloading combined hash CSV")
                    download_from_s3(hash_csv_uri, upload_results_csv)
                else:
                    logger.info("No combined CSV found, falling back to ZIP extraction")
                    combined_hash_zip = tmp_path / "combined_hash.zip"

                    if not state["hash_results_uri"]:
                        raise Exception("Missing hash results URI from previous task")

                    download_from_s3(state["hash_results_uri"], combined_hash_zip)
                    hash_dir = tmp_path / "hash_extracted"
                    extract_zip(combined_hash_zip, hash_dir)

                    csv_files = list(hash_dir.rglob("*.csv"))
                    logger.info(f"Found {len(csv_files)} CSV files")

                    combined_rows = []
                    headers = None

                    for csv_file in csv_files:
                        if "hash" in csv_file.name.lower() or "result" in csv_file.name.lower():
                            with open(csv_file, "r") as f:
                                reader = csv.DictReader(f)
                                if headers is None:
                                    headers = reader.fieldnames
                                for row in reader:
                                    combined_rows.append(row)

                    if not combined_rows:
                        raise Exception("No hash result CSVs found")

                    if headers:
                        with open(upload_results_csv, "w", newline="") as f:
                            writer = csv.DictWriter(f, fieldnames=headers)
                            writer.writeheader()
                            writer.writerows(combined_rows)

                # Upload the CSV to S3 for the submit task
                run_id_clean = state["run_id"].replace(":", "_").replace("+", "_")
                input_object_key = state["input_zip_uri"].split("/")[-1]
                submission_csv_s3_uri = (
                    f"{state['output_base_uri']}/{run_id_clean}/{input_object_key}/submission_ready.csv"
                )
                upload_to_s3(upload_results_csv, submission_csv_s3_uri)

                state["submission_csv_uri"] = submission_csv_s3_uri
                logger.info("Upload task completed successfully")

            except Exception as e:
                logger.error(f"Error in upload task: {str(e)}")
                state["error"] = str(e)
                state["error_step"] = "upload"
                error_log = {
                    "error": str(e),
                    "step": "upload",
                    "timestamp": pendulum.now("UTC").isoformat(),
                }
                error_path = Path(tmpdir) / "error.json"
                error_path.write_text(json.dumps(error_log))
                input_object_key = state["input_zip_uri"].split("/")[-1]
                error_s3_uri = (
                    f"{state['output_base_uri']}/{state['run_id']}/{input_object_key}/errors/upload_error.json"
                )
                upload_to_s3(error_path, error_s3_uri)
                raise

        return state

    @task(
        task_id="submit_to_blockchain",
        multiple_outputs=False,
        retries=2,
        retry_delay=timedelta(minutes=1),
        execution_timeout=timedelta(minutes=15),
    )
    def submit_to_blockchain(state: WorkflowState) -> WorkflowState:
        """Submit to blockchain contract"""
        with tempfile.TemporaryDirectory() as tmpdir:
            try:
                tmp_path = Path(tmpdir)

                # Get configuration from Airflow Variables (no defaults for security)
                domain = Variable.get("elephant_domain")
                api_key = Variable.get("elephant_api_key")
                oracle_key_id = Variable.get("elephant_oracle_key_id")
                from_address = Variable.get("elephant_from_address")
                rpc_url = Variable.get("elephant_rpc_url")

                logger.info("Downloading submission CSV from S3")
                submission_csv_uri = state.get("submission_csv_uri")
                if not submission_csv_uri:
                    raise Exception("Missing submission CSV URI from previous task")

                submission_csv_path = tmp_path / "submission.csv"
                download_from_s3(submission_csv_uri, submission_csv_path)

                logger.info("Submitting to contract using elephant-cli")
                cmd = [
                    "elephant-cli",
                    "submit-to-contract",
                    str(submission_csv_path),
                    "--domain",
                    domain,
                    "--api-key",
                    api_key,
                    "--oracle-key-id",
                    oracle_key_id,
                    "--from-address",
                    from_address,
                    "--rpc-url",
                    rpc_url,
                ]

                result = run_command(cmd, timeout=600, cwd=str(tmp_path))

                submission_result = {
                    "status": "success",
                    "stdout": result.stdout,
                    "stderr": result.stderr,
                    "timestamp": pendulum.now("UTC").isoformat(),
                }

                logger.info("Uploading submission logs")
                run_id_clean = state["run_id"].replace(":", "_").replace("+", "_")
                input_object_key = state["input_zip_uri"].split("/")[-1]

                submission_log_path = tmp_path / "submission_log.json"
                submission_log_path.write_text(json.dumps(submission_result, indent=2))

                submission_log_s3_uri = (
                    f"{state['output_base_uri']}/{run_id_clean}/{input_object_key}/submission_log.json"
                )
                upload_to_s3(submission_log_path, submission_log_s3_uri)

                state["submission_result"] = submission_result
                logger.info("Submit task completed successfully")

            except Exception as e:
                logger.error(f"Error in submit task: {str(e)}")
                state["error"] = str(e)
                state["error_step"] = "submit"
                error_log = {
                    "error": str(e),
                    "step": "submit",
                    "timestamp": pendulum.now("UTC").isoformat(),
                }
                error_path = Path(tmpdir) / "error.json"
                error_path.write_text(json.dumps(error_log))
                input_object_key = state["input_zip_uri"].split("/")[-1]
                error_s3_uri = (
                    f"{state['output_base_uri']}/{state['run_id']}/{input_object_key}/errors/submit_error.json"
                )
                upload_to_s3(error_path, error_s3_uri)
                raise

        return state

    @task(task_id="hash_files", multiple_outputs=False, retries=0)
    def hash_files(state: WorkflowState) -> WorkflowState:
        """Generate hashes for validated outputs"""
        with tempfile.TemporaryDirectory() as tmpdir:
            try:
                tmp_path = Path(tmpdir)

                # Consistently extract the input object key (filename)
                input_object_key = state["input_zip_uri"].split("/")[-1]

                # Download outputs from S3
                logger.info("Downloading validated outputs from S3")
                seed_zip_path = tmp_path / input_object_key / "seed_seed_output.zip"
                county_zip_path = tmp_path / input_object_key / "county_output.zip"

                if not state["seed_output_uri"] or not state["county_output_uri"]:
                    raise Exception("Missing seed or county output URIs from previous task")

                download_from_s3(state["seed_output_uri"], seed_zip_path)
                download_from_s3(state["county_output_uri"], county_zip_path)

                # Hash seed
                logger.info("Hashing seed using elephant-cli")
                seed_hash_zip = tmp_path / input_object_key / "seed_hash.zip"
                seed_hash_csv = tmp_path / input_object_key / "seed_hash.csv"

                cmd = [
                    "elephant-cli",
                    "hash",
                    str(seed_zip_path),
                    "--output-zip",
                    str(seed_hash_zip),
                    "--output-csv",
                    str(seed_hash_csv),
                ]
                run_command(cmd, timeout=300, cwd=str(tmp_path))

                # Extract propertyCid from hash CSV
                property_cid = None
                if seed_hash_csv.exists():
                    with open(seed_hash_csv, "r") as f:
                        reader = csv.DictReader(f)
                        for row in reader:
                            if "propertyCid" in row:
                                property_cid = row["propertyCid"]
                                break

                if not property_cid:
                    # Fallback: extract from the hashed zip
                    seed_hash_dir = tmp_path / "seed_hash_extracted"
                    extract_zip(seed_hash_zip, seed_hash_dir)

                    for item in seed_hash_dir.iterdir():
                        if item.is_dir() and item.name.startswith("bafkrei"):
                            property_cid = item.name
                            break

                if not property_cid:
                    raise Exception("Could not find propertyCid in hash results")

                logger.info(f"Extracted propertyCid: {property_cid}")

                # Hash county with propertyCid
                logger.info("Hashing county with propertyCid using elephant-cli")
                county_hash_zip = tmp_path / input_object_key / "county_hash.zip"
                county_hash_csv = tmp_path / input_object_key / "county_hash.csv"

                cmd = [
                    "elephant-cli",
                    "hash",
                    str(county_zip_path),
                    "--output-zip",
                    str(county_hash_zip),
                    "--output-csv",
                    str(county_hash_csv),
                    "--property-cid",
                    str(property_cid),
                ]
                run_command(cmd, timeout=300, cwd=str(tmp_path))

                # Combine hash results
                logger.info("Combining hash results and CSVs")
                combined_hash_dir = tmp_path / "combined_hash"
                combined_hash_dir.mkdir(parents=True, exist_ok=True)

                extract_zip(seed_hash_zip, combined_hash_dir / "seed")
                extract_zip(county_hash_zip, combined_hash_dir / "county")

                combined_hash_zip = tmp_path / "combined_hash.zip"
                create_zip(combined_hash_dir, combined_hash_zip)

                # Combine CSV files for submission
                combined_csv = tmp_path / "combined_hash.csv"
                combined_rows = []

                if seed_hash_csv.exists():
                    with open(seed_hash_csv, "r") as f:
                        reader = csv.DictReader(f)
                        combined_rows.extend(list(reader))

                if county_hash_csv.exists():
                    with open(county_hash_csv, "r") as f:
                        reader = csv.DictReader(f)
                        combined_rows.extend(list(reader))

                if combined_rows:
                    with open(combined_csv, "w", newline="") as f:
                        fieldnames = combined_rows[0].keys()
                        writer = csv.DictWriter(f, fieldnames=fieldnames)
                        writer.writeheader()
                        writer.writerows(combined_rows)

                # Upload to S3
                logger.info("Uploading hash results to S3")
                run_id_clean = state["run_id"].replace(":", "_").replace("+", "_")
                hash_s3_uri = f"{state['output_base_uri']}/{run_id_clean}/{input_object_key}/combined_hash.zip"
                csv_s3_uri = f"{state['output_base_uri']}/{run_id_clean}/{input_object_key}/combined_hash.csv"
                seed_hash_zip_s3_uri = f"{state['output_base_uri']}/{run_id_clean}/{input_object_key}/seed_hash.zip"
                county_hash_zip_s3_uri = f"{state['output_base_uri']}/{run_id_clean}/{input_object_key}/county_hash.zip"

                with ThreadPoolExecutor(max_workers=4) as executor:
                    futures = [
                        executor.submit(upload_to_s3, combined_hash_zip, hash_s3_uri),
                        executor.submit(upload_to_s3, combined_csv, csv_s3_uri),
                        executor.submit(upload_to_s3, seed_hash_zip, seed_hash_zip_s3_uri),
                        executor.submit(upload_to_s3, county_hash_zip, county_hash_zip_s3_uri),
                    ]
                    for f in futures:
                        f.result()

                # Update state
                state["hash_results_uri"] = hash_s3_uri
                state["hash_csv_uri"] = csv_s3_uri
                state["property_cid"] = str(property_cid)
                state["seed_hash_zip_uri"] = seed_hash_zip_s3_uri
                state["county_hash_zip_uri"] = county_hash_zip_s3_uri

                logger.info("Hash task completed successfully")

            except Exception as e:
                logger.error(f"Error in hash: {str(e)}")
                state["error"] = str(e)
                state["error_step"] = "hash"
                error_log = {
                    "error": str(e),
                    "step": "hash",
                    "timestamp": pendulum.now("UTC").isoformat(),
                }
                error_path = Path(tmpdir) / "error.json"
                error_path.write_text(json.dumps(error_log))
                input_object_key = state["input_zip_uri"].split("/")[-1]
                error_s3_uri = f"{state['output_base_uri']}/{state['run_id']}/{input_object_key}/errors/hash_error.json"
                upload_to_s3(error_path, error_s3_uri)
                raise

        return state

    @task(
        task_id="upload_hashed_results",
        multiple_outputs=False,
        retries=2,
        retry_delay=timedelta(minutes=1),
        execution_timeout=timedelta(minutes=10),
    )
    def upload_hashed_results(state: WorkflowState) -> WorkflowState:
        """Upload hashed seed and county zips using elephant-cli upload"""
        with tempfile.TemporaryDirectory() as tmpdir:
            try:
                tmp_path = Path(tmpdir)
                input_object_key = state["input_zip_uri"].split("/")[-1]

                # Required secret for upload to Pinata
                pinata_jwt = Variable.get("elephant_pinata_jwt")
                if not pinata_jwt:
                    raise Exception("Airflow Variable 'elephant_pinata_jwt' is missing or empty")

                seed_hash_uri = state.get("seed_hash_zip_uri")
                county_hash_uri = state.get("county_hash_zip_uri")

                if not seed_hash_uri or not county_hash_uri:
                    raise Exception("Missing seed or county hash zip URIs from previous task")

                # Download hashed zips locally
                seed_hash_zip_local = tmp_path / input_object_key / "seed_hash.zip"
                county_hash_zip_local = tmp_path / input_object_key / "county_hash.zip"
                download_from_s3(seed_hash_uri, seed_hash_zip_local)
                download_from_s3(county_hash_uri, county_hash_zip_local)

                # Perform uploads via elephant-cli
                logger.info("Uploading hashed seed using elephant-cli upload")
                result_seed = run_command(
                    [
                        "elephant-cli",
                        "upload",
                        str(seed_hash_zip_local),
                        "--pinata-jwt",
                        pinata_jwt,
                    ],
                    timeout=600,
                    cwd=str(tmp_path),
                )

                logger.info("Uploading hashed county using elephant-cli upload")
                result_county = run_command(
                    [
                        "elephant-cli",
                        "upload",
                        str(county_hash_zip_local),
                        "--pinata-jwt",
                        pinata_jwt,
                    ],
                    timeout=600,
                    cwd=str(tmp_path),
                )

                # Persist upload logs
                run_id_clean = state["run_id"].replace(":", "_").replace("+", "_")
                seed_upload_log = tmp_path / "seed_upload_log.json"
                county_upload_log = tmp_path / "county_upload_log.json"
                seed_upload_log.write_text(
                    json.dumps(
                        {
                            "status": "success",
                            "stdout": result_seed.stdout,
                            "stderr": result_seed.stderr,
                            "timestamp": pendulum.now("UTC").isoformat(),
                        },
                        indent=2,
                    )
                )
                county_upload_log.write_text(
                    json.dumps(
                        {
                            "status": "success",
                            "stdout": result_county.stdout,
                            "stderr": result_county.stderr,
                            "timestamp": pendulum.now("UTC").isoformat(),
                        },
                        indent=2,
                    )
                )

                seed_upload_log_s3_uri = (
                    f"{state['output_base_uri']}/{run_id_clean}/{input_object_key}/seed_upload_log.json"
                )
                county_upload_log_s3_uri = (
                    f"{state['output_base_uri']}/{run_id_clean}/{input_object_key}/county_upload_log.json"
                )

                with ThreadPoolExecutor(max_workers=2) as executor:
                    futures = [
                        executor.submit(upload_to_s3, seed_upload_log, seed_upload_log_s3_uri),
                        executor.submit(upload_to_s3, county_upload_log, county_upload_log_s3_uri),
                    ]
                    for f in futures:
                        f.result()

                logger.info("Upload of hashed results completed successfully")
            except Exception as e:
                logger.error(f"Error in upload_hashed_results: {str(e)}")
                state["error"] = str(e)
                state["error_step"] = "upload_hashed_results"
                error_log = {
                    "error": str(e),
                    "step": "upload_hashed_results",
                    "timestamp": pendulum.now("UTC").isoformat(),
                }
                error_path = Path(tmpdir) / "error.json"
                error_path.write_text(json.dumps(error_log))
                input_object_key = state["input_zip_uri"].split("/")[-1]
                error_s3_uri = f"{state['output_base_uri']}/{state['run_id']}/{input_object_key}/errors/upload_hashed_results_error.json"
                upload_to_s3(error_path, error_s3_uri)
                raise

        return state

    @task(
        task_id="transform_and_validate",
        multiple_outputs=False,
        execution_timeout=timedelta(minutes=1),
    )
    def transform_and_validate(state: WorkflowState) -> WorkflowState:
        """Transform and validate seed and county data"""
        with tempfile.TemporaryDirectory() as tmpdir:
            try:
                tmp_path = Path(tmpdir)
                input_object_key = state["input_zip_uri"].split("/")[-1]

                # Download and unzip input
                logger.info("Downloading and unzipping input")
                input_zip_path = tmp_path / input_object_key / "input.zip"
                download_from_s3(state["input_zip_uri"], input_zip_path)

                input_dir = tmp_path / input_object_key / "input"
                extract_zip(input_zip_path, input_dir)

                # Find CSV and HTML files
                csv_file = find_file_with_extension(input_dir, ".csv")
                html_file = find_file_with_extension(input_dir, ".html")

                if not csv_file or not html_file:
                    raise Exception("Input zip must contain both CSV and HTML files")

                logger.info(f"Found CSV: {csv_file.name}, HTML: {html_file.name}")

                scripts_path = tmp_path / "scripts.zip"
                scripts_s3_uri = Variable.get("elephant_scripts_s3_uri")
                if not scripts_s3_uri:
                    raise Exception("Missing scripts S3 URI from Airflow Variable 'elephant_scripts_s3_uri'")
                download_from_s3(scripts_s3_uri, scripts_path)
                # Transform the seed
                logger.info("Transforming seed")
                seed_output_zip = tmp_path / "seed_seed_output.zip"

                cmd = [
                    "elephant-cli",
                    "transform",
                    "--input-zip",
                    str(input_zip_path),
                    "--output-zip",
                    str(seed_output_zip),
                ]
                run_command(cmd, timeout=300, cwd=str(tmp_path))

                # Unzip seed output
                seed_output_dir = tmp_path / "seed_output"
                extract_zip(seed_output_zip, seed_output_dir)

                # Find the unnormalized_address.json and property_seed.json
                unnormalized_address = None
                property_seed = None

                for json_file in seed_output_dir.rglob("*.json"):
                    if "unnormalized_address" in json_file.name:
                        unnormalized_address = json_file
                    elif "property_seed" in json_file.name:
                        property_seed = json_file

                if not unnormalized_address or not property_seed:
                    raise Exception("Seed output missing required JSON files")

                # Prepare county input
                logger.info("Preparing county input")
                county_input_dir = tmp_path / "county_input"
                county_input_dir.mkdir(parents=True, exist_ok=True)

                shutil.copy2(unnormalized_address, county_input_dir / unnormalized_address.name)
                shutil.copy2(property_seed, county_input_dir / property_seed.name)
                shutil.copy2(html_file, county_input_dir / html_file.name)

                # Zip county input
                county_input_zip = tmp_path / "county_input.zip"
                create_zip(county_input_dir, county_input_zip)

                # Transform county
                logger.info("Transforming county")
                county_output_zip = tmp_path / "county_output.zip"

                cmd = [
                    "elephant-cli",
                    "transform",
                    "--input-zip",
                    str(county_input_zip),
                    "--output-zip",
                    str(county_output_zip),
                    "--scripts-zip",
                    str(scripts_path),
                ]
                run_command(cmd, timeout=300, cwd=str(tmp_path))

                # Validate outputs using elephant-cli
                logger.info("Validating seed output")
                cmd = ["elephant-cli", "validate", str(seed_output_zip)]
                # Run validation in tmp_path so elephant-cli writes submit_errors.csv there
                run_command(cmd, timeout=300, cwd=str(tmp_path))
                # Fail fast if submit_errors.csv contains any data rows (beyond header)
                errors_csv = tmp_path / "submit_errors.csv"
                if errors_csv.exists():
                    with open(errors_csv, "r") as f:
                        reader = csv.DictReader(f)
                        error_rows = list(reader)
                    if error_rows:
                        logger.error("Seed validation errors detected:")
                        for row in error_rows:
                            logger.error(json.dumps(row))
                        raise Exception("Seed validation failed: submit_errors.csv contains errors")
                    # Remove empty errors file to avoid stale reads
                    try:
                        errors_csv.unlink(missing_ok=True)  # type: ignore[arg-type]
                    except Exception:
                        pass

                logger.info("Validating county output")
                cmd = ["elephant-cli", "validate", str(county_output_zip)]
                run_command(cmd, timeout=300, cwd=str(tmp_path))
                # Fail fast if submit_errors.csv contains any data rows (beyond header)
                errors_csv = tmp_path / "submit_errors.csv"
                if errors_csv.exists():
                    with open(errors_csv, "r") as f:
                        reader = csv.DictReader(f)
                        error_rows = list(reader)
                    if error_rows:
                        logger.error("County validation errors detected:")
                        for row in error_rows:
                            logger.error(json.dumps(row))
                        raise Exception("County validation failed: submit_errors.csv contains errors")
                    try:
                        errors_csv.unlink(missing_ok=True)  # type: ignore[arg-type]
                    except Exception:
                        pass

                # Upload to S3
                logger.info("Uploading to S3")
                run_id_clean = state["run_id"].replace(":", "_").replace("+", "_")

                seed_s3_uri = f"{state['output_base_uri']}/{run_id_clean}/{input_object_key}/seed_seed_output.zip"
                county_s3_uri = f"{state['output_base_uri']}/{run_id_clean}/{input_object_key}/county_output.zip"

                # Parallel uploads for efficiency
                with ThreadPoolExecutor(max_workers=2) as executor:
                    futures = [
                        executor.submit(upload_to_s3, seed_output_zip, seed_s3_uri),
                        executor.submit(upload_to_s3, county_output_zip, county_s3_uri),
                    ]
                    for future in futures:
                        future.result()

                # Update state
                state["seed_output_uri"] = seed_s3_uri
                state["county_output_uri"] = county_s3_uri
                state["combined_output_uri"] = None

                logger.info("Transform and validate completed successfully")

            except Exception as e:
                logger.error(f"Error in transform_and_validate: {str(e)}")
                state["error"] = str(e)
                state["error_step"] = "transform_and_validate"
                # Upload error log
                error_log = {
                    "error": str(e),
                    "step": "transform_and_validate",
                    "timestamp": pendulum.now("UTC").isoformat(),
                }
                error_path = Path(tmpdir) / "error.json"
                error_path.write_text(json.dumps(error_log))
                input_object_key = state["input_zip_uri"].split("/")[-1]
                error_s3_uri = f"{state['output_base_uri']}/{state['run_id']}/{input_object_key}/errors/transform_validate_error.json"
                upload_to_s3(error_path, error_s3_uri)
                raise

        return state

    # Initialize the workflow state and chain the tasks
    # Pass file_info to the first task, then chain the rest
    state = initialize_state(file_info)
    state = transform_and_validate(state)
    state = hash_files(state)
    state = upload_hashed_results(state)
    state = prepare_submission(state)
    final_state = submit_to_blockchain(state)
    return final_state


# Create the DAG

default_args = {
    "owner": "elephant",
    "depends_on_past": False,
    "email_on_failure": True,
    "email_on_retry": False,
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
}

with DAG(
    dag_id="elephant_workflow",
    default_args=default_args,
    description="SQS-triggered Elephant workflow with dynamic task groups",
    schedule=timedelta(minutes=1),
    start_date=pendulum.datetime(2024, 1, 1, tz="UTC"),
    catchup=False,
    max_active_runs=100,
    tags=["elephant", "sqs-triggered", "production"],
    doc_md=__doc__,
) as dag:
    # Poll SQS for messages
    sqs_sensor = SqsSensor(
        task_id="poll_sqs_queue",
        sqs_queue="{{ var.value.elephant_sqs_queue_url }}",
        max_messages=int(Variable.get("elephant_batch_size", default_var=10)),
        num_batches=1,
        wait_time_seconds=20,
        visibility_timeout=3600,
        aws_conn_id="aws_default",
        mode="poke",
        poke_interval=30,
        timeout=30,
        soft_fail=True,
        do_xcom_push=True,
        retries=0,
        delete_message_on_reception=True,
    )

    # Parse SQS messages to extract file information from the sensor's XCom
    # The SqsSensor pushes its result to XCom, we pass it directly to the next task
    files_to_process = parse_sqs_messages(sqs_sensor.output["messages"])

    # Process each file in parallel using dynamic task groups
    processing_results = process_single_file.expand(file_info=files_to_process)


if __name__ == "__main__":
    dag.test()
    print("DAG test completed successfully")
