import re
import unicodedata
from datetime import datetime, timezone

import pandas as pd
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import BranchPythonOperator, PythonOperator
from airflow.sensors.filesystem import FileSensor
from airflow.utils.task_group import TaskGroup

from common.config import FINAL_DATASET, FINAL_FILE, PROCESSED_DIR, RAW_FILE, WORK_DIR

TMP_REPLACED_NULLS = WORK_DIR / "01_nulls_replaced.csv"
TMP_SORTED = WORK_DIR / "02_sorted_by_created_date.csv"

EMPTY_LOG_SCRIPT = "/opt/airflow/scripts/log_empty_file.sh"


def ensure_directories() -> None:
    WORK_DIR.mkdir(parents=True, exist_ok=True)
    PROCESSED_DIR.mkdir(parents=True, exist_ok=True)


def decide_branch() -> str:
    """Choose the empty-file branch or the processing branch."""
    df = pd.read_csv(RAW_FILE)
    return "log_empty_file" if df.empty else "data_processing.replace_nulls"


def replace_null_values() -> None:
    """Replace null-like values in text columns with the hyphen placeholder."""
    ensure_directories()
    df = pd.read_csv(RAW_FILE)

    object_columns = df.select_dtypes(include=["object"]).columns
    for column in object_columns:
        df[column] = (
            df[column]
            .replace(r"(?i)^\s*null\s*$", "-", regex=True)
            .fillna("-")
        )

    df.to_csv(TMP_REPLACED_NULLS, index=False)


def sort_by_created_date() -> None:
    """Rename the source timestamp column and sort rows by created_date."""
    df = pd.read_csv(TMP_REPLACED_NULLS)

    if "at" in df.columns:
        df = df.rename(columns={"at": "created_date"})
    else:
        raise ValueError("Expected column 'at' was not found in the input data.")

    df["created_date"] = pd.to_datetime(df["created_date"], errors="coerce")
    df = df.sort_values(by="created_date", kind="mergesort").reset_index(drop=True)
    df["created_date"] = df["created_date"].dt.strftime("%Y-%m-%d %H:%M:%S")

    df.to_csv(TMP_SORTED, index=False)


def clean_content() -> None:
    """Keep only letters, digits, punctuation marks and whitespace in the content column.

    Strips decorative/symbol characters (emoji, pictographs, dingbats, etc.)
    while preserving actual review text -- including numbers, since reviews
    like "10/10" or "5 stars" carry meaningful content that isn't a "junk
    character" in the sense the task means.
    """
    df = pd.read_csv(TMP_SORTED)

    def _clean_text(value: object) -> object:
        if pd.isna(value):
            return value

        text = str(value)
        cleaned_chars = []
        for char in text:
            category = unicodedata.category(char)
            # L* = letters, N* = numbers/digits, P* = punctuation.
            if char.isspace() or category.startswith(("L", "N", "P")):
                cleaned_chars.append(char)

        cleaned = re.sub(r"\s+", " ", "".join(cleaned_chars)).strip()
        return cleaned if cleaned else "-"

    if "content" in df.columns:
        df["content"] = df["content"].apply(_clean_text)

    df.to_csv(FINAL_FILE, index=False)


with DAG(
    dag_id="process_reviews",
    start_date=datetime(2024, 1, 1, tzinfo=timezone.utc),
    schedule=None,
    catchup=False,
    is_paused_upon_creation=False,
    tags=["reviews", "csv", "processing"],
    default_args={"owner": "airflow"},
) as dag:
    wait_for_file = FileSensor(
        task_id="wait_for_raw_file",
        filepath=str(RAW_FILE),
        fs_conn_id="fs_default",
        poke_interval=30,
        timeout=60 * 60,
        mode="poke",
    )

    branch = BranchPythonOperator(
        task_id="branch_on_empty_file",
        python_callable=decide_branch,
    )

    log_empty_file = BashOperator(
        task_id="log_empty_file",
        # Invoke via "bash <path>" explicitly rather than the bare path: the
        # BashOperator runs this as `bash -c "<bash_command>"`, and a bare
        # path there requires the script's execute bit to be set on disk
        # (git doesn't reliably preserve +x on checkout, e.g. via a
        # Windows clone or when core.fileMode is off). Prefixing with
        # "bash " makes this work regardless of the file's permission bits.
        bash_command=f"bash {EMPTY_LOG_SCRIPT}",
        env={"INPUT_FILE": str(RAW_FILE)},
    )

    with TaskGroup(group_id="data_processing") as data_processing:
        replace_nulls = PythonOperator(
            task_id="replace_nulls",
            python_callable=replace_null_values,
        )

        sort_created_date = PythonOperator(
            task_id="sort_by_created_date",
            python_callable=sort_by_created_date,
        )

        clean_content_task = PythonOperator(
            task_id="clean_content",
            python_callable=clean_content,
            outlets=[FINAL_DATASET],
        )

        replace_nulls >> sort_created_date >> clean_content_task

    wait_for_file >> branch
    branch >> log_empty_file
    branch >> data_processing
