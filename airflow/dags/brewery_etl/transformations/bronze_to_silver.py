"""Module for transforming data from bronze to silver layer."""

import os
import logging
from typing import Any, Tuple
from datetime import datetime
from deltalake import DeltaTable
from deltalake.writer import write_deltalake
import pandas as pd

from brewery_etl.transformations.utils.constants import (
    BRONZE_PATH, QUARANTINE_PATH, SILVER_PATH, KEY_FIELDS, STRING_COLUMNS)
from brewery_etl.transformations.utils.metrics import brewery_metrics, ETLMetricsContext
from brewery_etl.transformations.utils.helpers import (
    standardize_brewery_types,
    standardize_website_urls,
    validate_schema,
    standardize_location_fields,
    add_processing_metadata,
    calculate_directory_size,
    convert_string_columns
)

logger = logging.getLogger(__name__)

def bronze_to_silver(**kwargs: Any) -> str:
    """
    Process data from bronze to silver layer with transformations and partitioning 
    by location using pandas

    Args:
        **kwargs: Additional keyword arguments passed to metrics context

    Returns:
        str: Path to silver layer
    """
    with ETLMetricsContext(brewery_metrics, 'silver', **kwargs) as metrics:
        partitions_created = metrics.register_metric(
            'gauge',
            'brewery_etl_silver_partitions_created',
            'Number of partitions created in silver layer'
        )

        delta_write_duration = metrics.register_metric(
            'histogram',
            'brewery_etl_silver_delta_write_duration_seconds',
            'Time taken to write to Delta format in silver layer'
        )

        rows_discarded = metrics.register_metric(
            'counter',
            'brewery_etl_silver_rows_discarded_total',
            'Total number of rows discarded due to missing key values',
            ['reason']
        )

        os.makedirs(QUARANTINE_PATH, exist_ok=True)
        os.makedirs(SILVER_PATH, exist_ok=True)

        df = _load_bronze_data(metrics)

        logger.info("Original DataFrame schema:")
        logger.info(df.dtypes)

        total_records = len(df)
        metrics.records_processed_total.labels(operation='silver').inc(total_records)

        df = _remove_invalid_records(df, KEY_FIELDS, rows_discarded)

        transformation_start = datetime.now()

        df = convert_string_columns(df, STRING_COLUMNS)
        df = standardize_location_fields(df)
        df = standardize_brewery_types(df)
        df = standardize_website_urls(df)
        df = add_processing_metadata(df)

        transformation_duration = (datetime.now() - transformation_start).total_seconds()
        metrics.processing_duration_seconds.labels(
            operation='silver_transformation'
        ).observe(transformation_duration)

        unique_locations = df['location'].nunique()
        partitions_created.set(unique_locations)

        if not df.empty:
            _write_to_silver(df, metrics, delta_write_duration, len(df))
            logger.info("Successfully processed data to silver layer")
        else:
            logger.warning("No valid records to write to silver layer after filtering")

        return SILVER_PATH


def _load_bronze_data(metrics: Any) -> pd.DataFrame:
    """
    Load data from bronze layer with metrics tracking

    Args:
        metrics: Metrics context for tracking operations

    Returns:
        DataFrame loaded from bronze layer

    Raises:
        Exception: If loading fails
    """
    start_time = datetime.now()
    try:
        df = DeltaTable(BRONZE_PATH).to_pandas()

        bronze_read_duration = (datetime.now() - start_time).total_seconds()
        metrics.processing_duration_seconds.labels(
            operation='silver_bronze_read').observe(bronze_read_duration)

        metrics.operations_total.labels(operation='silver_bronze_read', status='success').inc()

        validate_schema(df, KEY_FIELDS)

        return df
    except Exception as e:
        metrics.operations_total.labels(operation='silver_bronze_read', status='failure').inc()
        logger.error("Error reading from bronze layer: %s", str(e))
        raise


def _write_to_silver(df: pd.DataFrame, metrics: Any, duration_metric: Any, total_records: int) -> None:
    """
    Write dataframe to silver layer with metrics tracking

    Args:
        df: DataFrame to write
        metrics: Metrics context for tracking operations
        duration_metric: Metric for tracking write duration
        total_records: Total number of records being written

    Raises:
        Exception: If writing fails
    """
    delta_write_start = datetime.now()
    try:
        write_deltalake(SILVER_PATH, df, partition_by=["location"], mode="overwrite")

        delta_write_duration_seconds = (datetime.now() - delta_write_start).total_seconds()
        duration_metric.observe(delta_write_duration_seconds)

        metrics.operations_total.labels(operation='silver_delta_write',status='success').inc()

        silver_size = calculate_directory_size(SILVER_PATH)
        metrics.data_processed_bytes.labels(operation='silver').set(silver_size)

        logger.info("Successfully wrote all %d records to silver layer", total_records)
    except Exception as e:
        metrics.operations_total.labels(operation='silver_delta_write', status='failure').inc()

        logger.error("Error writing to silver layer: %s", str(e))
        for col in df.columns:
            logger.error("Column %s: %s, null count: %d", col, df[col].dtype, df[col].isna().sum())
            if df[col].dtype == 'object':
                logger.error("Sample values: %s", df[col].dropna().head(3).tolist())
        raise


def _remove_invalid_records(df: pd.DataFrame, key_fields: list,
                            rows_discarded_metric: Any) -> Tuple[pd.DataFrame, pd.DataFrame]:
    """
    Identify and quarantine records with missing key values.

    Args:
        df: DataFrame to process
        key_fields: List of fields that must not be null
        rows_discarded_metric: Prometheus metric for tracking discarded rows
        
    Returns:
        Tuple of (valid_df, quarantined_df)
    """
    null_mask = df[key_fields].isna().any(axis=1)
    quarantine_df = df[null_mask].copy()
    valid_df = df[~null_mask].copy()

    if not quarantine_df.empty:
        quarantine_df['quarantine_reason'] = 'missing_key_values'
        quarantine_df['quarantine_timestamp'] = datetime.now().isoformat()

        for field in key_fields:
            field_nulls = df[field].isna().sum()
            if field_nulls > 0:
                rows_discarded_metric.labels(reason=f"null_{field}").inc(field_nulls)

        try:
            write_deltalake(QUARANTINE_PATH, quarantine_df, mode="append")
            logger.info("Quarantined %d rows with missing key values", {len(quarantine_df)})
        except ValueError as e:
            logger.error("Schema or parameter error when writing quarantined data: %s", str(e))
        except IOError as e:
            logger.error("I/O error when writing to quarantine path: %s", str(e))
        except Exception as e:
            logger.error("Unexpected error when writing quarantined data: %s", str(e))

    rows_discarded_metric.labels(reason="total").inc(len(quarantine_df))

    logger.info("Retained %d rows, discarded %d rows", len(valid_df), len(quarantine_df))

    return valid_df
