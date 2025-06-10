"""Module for transforming data from landing zone to bronze layer."""

import json
import os
import logging
from typing import List, Dict, Any, Tuple
from datetime import datetime
from deltalake import DeltaTable
from deltalake.writer import write_deltalake

import pandas as pd

from brewery_etl.transformations.utils.constants import BRONZE_PATH
from brewery_etl.transformations.utils.metrics import brewery_metrics, ETLMetricsContext
from brewery_etl.transformations.utils.helpers import (
    load_json_file,
    calculate_file_size,
    calculate_directory_size,
    add_ingestion_metadata
)

logger = logging.getLogger(__name__)

def landing_to_bronze(**kwargs: Any) -> str:
    """
    Process data from landing zone to bronze layer in Delta format using pandas

    Args:
        **kwargs: Additional keyword arguments passed to metrics context

    Returns:
        str: Path to bronze layer
    """
    with ETLMetricsContext(brewery_metrics, 'transform', **kwargs) as metrics:
        files_processed = metrics.register_metric(
            'counter',
            'brewery_etl_transform_files_processed_total',
            'Total number of landing files processed'
        )

        delta_write_duration = metrics.register_metric(
            'histogram',
            'brewery_etl_transform_delta_write_duration_seconds',
            'Time taken to write to Delta format'
        )

        schema_fields_count = metrics.register_metric(
            'gauge',
            'brewery_etl_transform_schema_fields_count',
            'Number of fields in the schema'
        )

        os.makedirs(BRONZE_PATH, exist_ok=True)

        landing_files = _get_landing_files(kwargs, metrics)
        if not landing_files:
            logger.warning("No landing files found from previous task")
            return BRONZE_PATH

        files_processed.inc(len(landing_files))

        all_data, total_file_size = _process_landing_files(landing_files, metrics)

        metrics.data_processed_bytes.labels(operation='transform').set(total_file_size)

        if not all_data:
            logger.warning("No data to process")
            return BRONZE_PATH

        df = pd.DataFrame(all_data)
        df = add_ingestion_metadata(df)

        schema_fields_count.set(len(df.columns))

        _write_to_bronze(df, metrics, delta_write_duration)

        return BRONZE_PATH


def _get_landing_files(kwargs: Dict[str, Any], metrics: Any) -> List[str]:
    """
    Get landing files from previous task
    
    Args:
        kwargs: Keyword arguments containing task instance
        metrics: Metrics context
        
    Returns:
        List of landing file paths
    """
    try:
        ti = kwargs.get('ti')
        if not ti:
            raise ValueError("Task instance not found in kwargs")

        landing_files = ti.xcom_pull(task_ids='extract_brewery_data')

        if landing_files:
            metrics.operations_total.labels(operation='transform_file_retrieval', status='success').inc()
            return landing_files
        metrics.operations_total.labels(operation='transform_file_retrieval',status='failure').inc()
        return []
    except ValueError as e:
        metrics.operations_total.labels(operation='transform_file_retrieval', status='failure').inc()
        logger.error("Missing or invalid parameter: %s", str(e))
        return []
    except AttributeError as e:
        metrics.operations_total.labels(operation='transform_file_retrieval', status='failure').inc()
        logger.error("Task instance missing required attribute: %s", str(e))
        return []
    except Exception as e:
        metrics.operations_total.labels(operation='transform_file_retrieval', status='failure').inc()
        logger.error("Unexpected error retrieving landing files: %s", str(e))
        return []


def _process_landing_files(landing_files: List[str], metrics: Any) -> Tuple[List[Dict[str, Any]], int]:
    """
    Process landing files and collect data
    
    Args:
        landing_files: List of landing file paths
        metrics: Metrics context
        
    Returns:
        Tuple containing list of data records and total file size
    """
    all_data = []
    total_file_size = 0

    for file_path in landing_files:
        try:
            data = load_json_file(file_path)

            file_size = calculate_file_size(file_path)
            total_file_size += file_size

            records_in_file = len(data)
            all_data.extend(data)

            metrics.records_processed_total.labels(operation='transform').inc(records_in_file)

            metrics.operations_total.labels(operation='transform_file_read', status='success').inc()

            logger.info("Processed file %s with %d records", file_path, records_in_file)

        except FileNotFoundError as e:
            logger.error("File not found %s: %s", file_path, str(e))
            raise
        except json.JSONDecodeError as e:
            logger.error("Invalid JSON in file %s: %s", file_path, str(e))
            raise
        except Exception as e:
            logger.error("Error processing file %s: %s", file_path, str(e))
            raise
        finally:
            metrics.operations_total.labels(operation='transform_file_read', status='failure').inc()

    return all_data, total_file_size


def _write_to_bronze(df: pd.DataFrame, metrics: Any, duration_metric: Any) -> None:
    """
    Write dataframe to bronze layer
    
    Args:
        df: DataFrame to write
        metrics: Metrics context
        duration_metric: Metric for tracking write duration
        
    Raises:
        Exception: If writing fails
    """
    delta_write_start = datetime.now()

    try:
        write_deltalake(BRONZE_PATH, df, mode="overwrite")

        delta_write_duration_seconds = (datetime.now() - delta_write_start).total_seconds()
        duration_metric.observe(delta_write_duration_seconds)

        metrics.operations_total.labels(operation='transform_delta_write', status='success').inc()

        bronze_df = DeltaTable(BRONZE_PATH).to_pandas()
        bronze_records = len(bronze_df)

        bronze_size = calculate_directory_size(BRONZE_PATH)
        metrics.data_processed_bytes.labels(operation='load').set(bronze_size)

        logger.info("Successfully loaded %d records to bronze layer", bronze_records)

    except ValueError as e:
        logger.error("Schema or parameter error writing to Delta format: %s", str(e))
        raise
    except IOError as e:
        logger.error("I/O error writing to Delta format: %s", str(e))
        raise
    except Exception as e:
        logger.error("Unexpected error writing to Delta format: %s", str(e))
        raise
    finally:
        metrics.operations_total.labels(operation='transform_delta_write', status='failure').inc()
