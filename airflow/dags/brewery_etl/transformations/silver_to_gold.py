"""Module for transforming data from silver to gold layer with aggregations."""

import os
import logging
from typing import Dict, Any, Optional
from datetime import datetime

import pandas as pd

from brewery_etl.transformations.utils.constants import GOLD_PATH, SILVER_PATH
from brewery_etl.transformations.utils.metrics import brewery_metrics, ETLMetricsContext
from brewery_etl.transformations.utils.helpers import (
    read_delta_table,
    write_delta_table,
    read_partitioned_data,
    create_aggregation
)

logger = logging.getLogger(__name__)


def silver_to_gold(**kwargs: Any) -> str:
    """
    Process data from silver to gold layer with aggregations using pandas

    Args:
        **kwargs: Additional keyword arguments passed to metrics context

    Returns:
        str: Path to gold layer
    """
    with ETLMetricsContext(brewery_metrics, 'load', **kwargs) as metrics:
        aggregation_count = metrics.register_metric(
            'gauge',
            'brewery_etl_gold_aggregation_count',
            'Number of aggregations created in gold layer'
        )

        aggregation_records = metrics.register_metric(
            'gauge',
            'brewery_etl_gold_aggregation_records',
            'Number of records in each aggregation',
            ['aggregation_name']
        )

        os.makedirs(GOLD_PATH, exist_ok=True)
        logger.info("Checking silver path: %s", os.path.exists(SILVER_PATH))

        df = _load_silver_data(metrics)

        if df is None or len(df) == 0:
            logger.warning("No data found in silver layer")
            return GOLD_PATH

        df_size = df.memory_usage(deep=True).sum()
        metrics.data_processed_bytes.labels(operation='load').set(df_size)

        logger.info("DataFrame schema:")
        logger.info(df.dtypes)
        logger.info("DataFrame columns: %s", df.columns.tolist())

        metrics.register_metric(
            'gauge',
            'brewery_etl_gold_schema_fields_count',
            'Number of fields in the schema'
        ).set(len(df.columns))

        aggregations = _create_aggregations(df, metrics)
        aggregation_count.set(len(aggregations))

        for name, agg_df in aggregations.items():
            aggregation_records.labels(aggregation_name=name).set(len(agg_df))
            _write_aggregation(name, agg_df, metrics)

        logger.info("Successfully created gold layer aggregations")
        return GOLD_PATH


def _load_silver_data(metrics: Any) -> Optional[pd.DataFrame]:
    """
    Load data from silver layer, handling both single table and partitioned approaches

    Args:
        metrics: Metrics context
        
    Returns:
        DataFrame with silver data or None if no data found
    """
    try:
        logger.info("Attempting to read silver layer as a single Delta table")
        df = read_delta_table(SILVER_PATH, metrics, 'silver_read', 'silver')

        metrics.records_processed_total.labels(operation='load').inc(len(df))
        return df

    except FileNotFoundError as e:
        logger.warning("Silver layer path not found: %s", str(e))
        logger.info("Attempting to read individual partitions")
        return read_partitioned_data(SILVER_PATH, "location=*", metrics, 'silver')


def _create_aggregations(df: pd.DataFrame, metrics: Any) -> Dict[str, pd.DataFrame]:
    """
    Create aggregations from DataFrame

    Args:
        df: Source DataFrame
        metrics: Metrics context
        
    Returns:
        Dictionary mapping aggregation names to DataFrames
    """
    logger.info("Creating aggregations")
    start_time = datetime.now()

    aggregations = {
        "by_type_location": create_aggregation(
            df, ["brewery_type", "location", "state", "city"], "brewery_count"
        ),
        "by_location": create_aggregation(
            df, ["location", "state", "city"], "brewery_count"
        )
    }

    aggregation_duration = (datetime.now() - start_time).total_seconds()
    metrics.processing_duration_seconds.labels(operation='aggregation').observe(aggregation_duration)

    for name, agg_df in aggregations.items():
        logger.info("Created aggregation %s with %d records", name, len(agg_df))

    return aggregations


def _write_aggregation(name: str, df: pd.DataFrame, metrics: Any) -> None:
    """
    Write aggregation to gold layer

    Args:
        name: Aggregation name
        df: Aggregation DataFrame
        metrics: Metrics context
    """
    output_path = f"{GOLD_PATH}/{name}"
    try:
        write_delta_table(output_path, df, metrics, name, 'gold')
    except IOError as e:
        logger.error("I/O error writing aggregation %s: %s", name, str(e))
    except ValueError as e:
        logger.error("Schema or parameter error writing aggregation %s: %s", name, str(e))
    except Exception as e:
        logger.error("Unexpected error writing aggregation %s: %s", name, str(e))