"""Helper functions for brewery ETL processes."""

import glob
import json
import logging
import os
import re
import pandas as pd
import requests
import shutil
import time

from typing import List, Dict, Any, Optional, Union
from datetime import datetime
from deltalake import DeltaTable
from deltalake.writer import write_deltalake

from brewery_etl.transformations.utils.constants import API_TIMEOUT, STANDARD_BREWERY_TYPES

logger = logging.getLogger(__name__)


def validate_schema(df: pd.DataFrame, expected_columns: List[str]) -> bool:
    """
    Validate dataframe has required columns

    Args:
        df: DataFrame to validate
        expected_columns: List of required column names

    Returns:
        bool: True if validation passes

    Raises:
        ValueError: If required columns are missing
    """
    missing = set(expected_columns) - set(df.columns)
    if missing:
        raise ValueError(f"Missing required columns: {missing}")
    return True


def standardize_location_fields(df: pd.DataFrame) -> pd.DataFrame:
    """
    Standardize location fields for consistency

    Args:
        df: DataFrame with location fields

    Returns:
        DataFrame with standardized location fields
    """
    df['state'] = df['state'].str.upper()
    df['city'] = df['city'].str.upper()
    df['country'] = df['country'].str.upper()
    df['location'] = df['country']
    return df


def standardize_brewery_types(df: pd.DataFrame) -> pd.DataFrame:
    """
    Standardize brewery types to a consistent set of values.

    Args:
        df: DataFrame with brewery_type column
        
    Returns:
        DataFrame with standardized brewery types
    """
    if 'brewery_type' not in df.columns:
        return df

    result_df = df.copy()

    def standardize_type(brewery_type):
        if pd.isna(brewery_type) or brewery_type is None:
            return 'unknown'
        brewery_type = brewery_type.lower().strip()
        return STANDARD_BREWERY_TYPES.get(brewery_type, 'other')

    result_df['brewery_type'] = result_df['brewery_type'].apply(standardize_type)

    type_counts = result_df['brewery_type'].value_counts()
    logger.info("Brewery type distribution after standardization: %s", type_counts.to_dict())

    return result_df


def standardize_website_urls(df: pd.DataFrame) -> pd.DataFrame:
    """
    Standardize website URLs by ensuring they have proper http/https prefixes.

    Args:
        df: DataFrame with website_url column
        
    Returns:
        DataFrame with standardized website URLs
    """
    if 'website_url' not in df.columns:
        return df

    result_df = df.copy()
    def standardize_url(url):
        if pd.isna(url) or url is None or url.strip() == '':
            return None
        url = url.strip()
        if not re.match(r'^https?://', url):
            url = 'http://' + url
        return url

    result_df['website_url'] = result_df['website_url'].apply(standardize_url)

    return result_df


def check_duplicate_ids(df: pd.DataFrame, metrics: Any) -> int:
    """
    Check for duplicate IDs in the dataframe

    Args:
        df: DataFrame to check
        metrics: Metrics context for recording duplicates

    Returns:
        int: Number of duplicate IDs found
    """
    duplicate_ids = df['id'].duplicated().sum()
    if duplicate_ids > 0:
        logger.warning("Found %d duplicate IDs", duplicate_ids)
        metrics.register_metric('gauge', 'brewery_etl_silver_duplicate_ids').set(duplicate_ids)
    return duplicate_ids


def add_processing_metadata(df: pd.DataFrame, version: str = '1.0') -> pd.DataFrame:
    """
    Add processing metadata to dataframe

    Args:
        df: DataFrame to enhance
        version: ETL version string

    Returns:
        DataFrame with added metadata
    """
    df['processed_at'] = datetime.now().isoformat()
    df['etl_version'] = version
    return df


def calculate_directory_size(directory_path: str) -> int:
    """
    Calculate total size of files in a directory

    Args:
        directory_path: Path to directory

    Returns:
        int: Total size in bytes
    """
    total_size = 0
    for dirpath, _, filenames in os.walk(directory_path):
        for filename in filenames:
            file_path = os.path.join(dirpath, filename)
            if os.path.isfile(file_path):
                total_size += os.path.getsize(file_path)
    return total_size


def fill_null_values(df: pd.DataFrame, fill_values: Dict[str, Any]) -> pd.DataFrame:
    """
    Fill null values in dataframe with specified values

    Args:
        df: DataFrame with null values
        fill_values: Dictionary mapping column names to fill values

    Returns:
        DataFrame with filled values
    """
    return df.fillna(fill_values)


def convert_string_columns(df: pd.DataFrame, columns: List[str]) -> pd.DataFrame:
    """
    Convert specified columns to string type

    Args:
        df: DataFrame to process
        columns: List of column names to convert

    Returns:
        DataFrame with converted columns
    """
    for col in columns:
        if col in df.columns:
            df[col] = df[col].astype(str)
    return df


def prepare_landing_directory(landing_path: str) -> None:
    """
    Prepare landing directory by cleaning up existing files and creating directory

    Args:
        landing_path: Path to landing directory
    """
    if os.path.exists(landing_path):
        logger.info("Cleaning up existing files in %s", landing_path)
        shutil.rmtree(landing_path)

    os.makedirs(landing_path, exist_ok=True)


def make_api_request(url: str, params: Dict[str, Any], metrics: Any, api_requests_total: Any,
                     api_retries: Any, max_retries: int = 3) -> Union[requests.Response, None]:
    """
    Make API request with retry logic and metrics tracking

    Args:
        url: API URL
        params: Request parameters
        metrics: Metrics context for tracking
        api_requests_total: Pre-registered metric for API requests
        api_retries: Pre-registered metric for API retries
        max_retries: Maximum number of retry attempts
        
    Returns:
        Response object from successful request
        
    Raises:
        Exception: If all retry attempts fail
    """
    api_requests_total.inc()

    retry_count = 0
    while retry_count < max_retries:
        try:
            start_time = time.time()
            response = requests.get(url, params=params, timeout=API_TIMEOUT)
            response_time = time.time() - start_time

            metrics.processing_duration_seconds.labels(operation='api_request').observe(response_time)

            response.raise_for_status()

            metrics.operations_total.labels(operation='api_request',status='success').inc()

            return response
        except (requests.exceptions.RequestException, requests.exceptions.HTTPError) as e:
            retry_count += 1
            api_retries.inc()

            logger.warning("API request failed (attempt %d/%d): %s", retry_count, max_retries, str(e))

            if retry_count >= max_retries:
                metrics.operations_total.labels(operation='api_request',status='failure').inc()
                raise Exception(f'Failed to fetch data after {max_retries} retries: {str(e)}') from e
            time.sleep(5)


def save_json_data(data: List[Dict[str, Any]], output_file: str) -> int:
    """
    Save JSON data to file

    Args:
        data: Data to save
        output_file: Output file path
        
    Returns:
        Size of saved file in bytes
    """
    with open(output_file, 'w', encoding='utf-8') as file_handle:
        json.dump(data, file_handle)

    return os.path.getsize(output_file)


def load_json_file(file_path: str) -> List[Dict[str, Any]]:
    """
    Load JSON data from file

    Args:
        file_path: Path to JSON file
        
    Returns:
        List of dictionaries containing the JSON data
        
    Raises:
        Exception: If file cannot be read or parsed
    """
    try:
        with open(file_path, 'r', encoding='utf-8') as file_handle:
            return json.load(file_handle)
    except Exception as e:
        logger.error("Failed to load JSON file %s: %s", file_path, str(e))
        raise


def calculate_file_size(file_path: str) -> int:
    """
    Calculate size of a file in bytes

    Args:
        file_path: Path to file
        
    Returns:
        Size of file in bytes
    """
    return os.path.getsize(file_path)


def add_ingestion_metadata(df: pd.DataFrame) -> pd.DataFrame:
    """
    Add ingestion metadata to dataframe

    Args:
        df: DataFrame to enhance
        
    Returns:
        DataFrame with added metadata
    """
    df["ingestion_timestamp"] = datetime.now()
    return df


def read_delta_table(path: str, metrics: Any, operation_name: str = 'delta_read',
                     layer_name: str = 'data') -> pd.DataFrame:
    """
    Read a Delta table with metrics tracking

    Args:
        path: Path to Delta table
        metrics: Metrics context
        operation_name: Name of the operation for metrics
        layer_name: Name of the data layer (bronze, silver, gold)
        
    Returns:
        DataFrame from Delta table
        
    Raises:
        Exception: If reading fails
    """
    start_time = datetime.now()
    try:
        df = DeltaTable(path).to_pandas()
        read_duration = (datetime.now() - start_time).total_seconds()

        metrics.processing_duration_seconds.labels(operation=operation_name).observe(read_duration)

        metrics.operations_total.labels(operation=operation_name, status='success').inc()

        logger.info("Successfully read %d records from %s layer at %s", len(df), layer_name, path)
        return df
    except Exception as e:
        metrics.operations_total.labels(
            operation=operation_name, 
            status='failure'
        ).inc()
        logger.error("Error reading %s layer at %s: %s", layer_name, path, str(e))
        raise


def write_delta_table(path: str, df: pd.DataFrame, metrics: Any, operation_name: str = 'delta_write',
                      layer_name: str = 'data', mode: str = "overwrite", 
                      partition_by: Optional[List[str]] = None) -> None:
    """
    Write DataFrame to Delta table with metrics tracking

    Args:
        path: Path to write Delta table
        df: DataFrame to write
        metrics: Metrics context
        operation_name: Name of the operation for metrics
        layer_name: Name of the data layer (bronze, silver, gold)
        mode: Write mode (overwrite, append, etc.)
        partition_by: List of columns to partition by
        
    Raises:
        Exception: If writing fails
    """
    start_time = datetime.now()
    try:
        if partition_by:
            write_deltalake(path, df, mode=mode, partition_by=partition_by)
        else:
            write_deltalake(path, df, mode=mode)

        write_duration = (datetime.now() - start_time).total_seconds()

        duration_metric = metrics.register_metric(
            'histogram',
            f'brewery_etl_{layer_name}_delta_write_duration_seconds',
            f'Time taken to write to Delta format in {layer_name} layer',
            ['operation']
        )

        duration_metric.labels(operation=operation_name).observe(write_duration)

        metrics.operations_total.labels(operation=f'{layer_name}_{operation_name}', status='success').inc()

        logger.info("Successfully wrote %d records to %s layer at %s", len(df), layer_name, path)

        try:
            data_size = calculate_directory_size(path)
            metrics.data_processed_bytes.labels(operation=layer_name).set(data_size)
        except FileNotFoundError as e:
            logger.warning("Directory not found when calculating size for %s: %s", path, str(e))
        except Exception as e:
            logger.warning("Could not calculate size for %s: %s", path, str(e))

    except Exception as e:
        metrics.operations_total.labels(
            operation=f'{layer_name}_{operation_name}', 
            status='failure'
        ).inc()
        logger.error("Error writing to %s layer at %s: %s", layer_name, path, str(e))
        raise


def _find_partitions(base_path: str, pattern: str) -> List[str]:
    """
    Find partitions in a directory using glob pattern

    Args:
        base_path: Base directory path
        pattern: Glob pattern for partitions
        
    Returns:
        List of partition paths
    """
    partition_paths = glob.glob(f"{base_path}/{pattern}")
    logger.info("Found %d partitions matching pattern %s", len(partition_paths), pattern)
    return partition_paths


def read_partitioned_data(base_path: str, partition_pattern: str, metrics: Any,
                          operation_prefix: str = 'partition') -> Optional[pd.DataFrame]:
    """
    Read data from partitioned Delta tables

    Args:
        base_path: Base path containing partitions
        partition_pattern: Pattern to match partitions (e.g., "location=*")
        metrics: Metrics context
        operation_prefix: Prefix for operation names in metrics
        
    Returns:
        Combined DataFrame from all partitions or None if no data found
    """
    partition_paths = _find_partitions(base_path, partition_pattern)

    metrics.register_metric(
        'gauge',
        f'brewery_etl_{operation_prefix}_partition_count',
        'Number of partitions processed'
    ).set(len(partition_paths))

    dfs = []
    successful_partitions = 0
    total_records = 0

    for path in partition_paths:
        try:
            logger.info("Reading partition: %s", path)
            partition_df = read_delta_table(path, metrics, f'{operation_prefix}_read', operation_prefix)

            records_in_partition = len(partition_df)
            total_records += records_in_partition
            dfs.append(partition_df)
            successful_partitions += 1

        except FileNotFoundError as e:
            logger.error("Partition path not found %s: %s", path, str(e))
        except IOError as e:
            logger.error("I/O error reading partition %s: %s", path, str(e))
        except Exception as e:
            logger.error("Unexpected error reading partition %s: %s", path, str(e))

    if dfs:
        df = pd.concat(dfs, ignore_index=True)
        logger.info("Combined %d records from %d partitions", len(df), len(partition_paths))

        metrics.records_processed_total.labels(operation=operation_prefix).inc(total_records)

        metrics.register_metric(
            'gauge',
            f'brewery_etl_{operation_prefix}_successful_partitions',
            'Number of successfully processed partitions'
        ).set(successful_partitions)

        return df
    logger.warning("No data found in partitions at %s", base_path)
    return None


def create_aggregation(df: pd.DataFrame, group_by_columns: List[str],
                       count_column_name: str = "count") -> pd.DataFrame:
    """
    Create an aggregation by grouping and counting

    Args:
        df: DataFrame to aggregate
        group_by_columns: Columns to group by
        count_column_name: Name for the count column
        
    Returns:
        Aggregated DataFrame
    """
    return df.groupby(group_by_columns).size().reset_index(name=count_column_name)
