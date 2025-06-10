"""Module for extracting brewery data from API and saving to landing zone."""

import time
import logging
from typing import List, Any
from datetime import datetime

from brewery_etl.transformations.utils.constants import API_PER_PAGE_LIMIT, LANDING_PATH, API_BASE_URL
from brewery_etl.transformations.utils.metrics import brewery_metrics, ETLMetricsContext
from brewery_etl.transformations.utils.helpers import (
    prepare_landing_directory,
    make_api_request,
    save_json_data
)

logger = logging.getLogger(__name__)


def extract_brewery_data(**kwargs: Any) -> List[str]:
    """
    Extract data from Open Brewery DB API with pagination and save to landing zone

    Args:
        **kwargs: Additional keyword arguments passed to metrics context

    Returns:
        list: List of output file paths
    """
    with ETLMetricsContext(brewery_metrics, 'extract', **kwargs) as metrics:
        pages_total = metrics.register_metric(
            'gauge',
            'brewery_etl_extract_pages_total',
            'Total number of pages extracted'
        )

        files_total = metrics.register_metric(
            'gauge',
            'brewery_etl_extract_files_total',
            'Total number of files created'
        )

        api_requests_total = metrics.register_metric(
            'counter',
            'brewery_etl_extract_api_requests_total',
            'Total number of API requests made'
        )

        api_retries = metrics.register_metric(
            'counter',
            'brewery_etl_extract_api_retries_total',
            'Total number of API request retries'
        )

        prepare_landing_directory(LANDING_PATH)

        timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')

        return _extract_paginated_data(
            metrics=metrics,
            timestamp=timestamp,
            pages_total=pages_total,
            files_total=files_total,
            api_requests_total=api_requests_total,
            api_retries=api_retries
        )


def _extract_paginated_data(metrics: Any, timestamp: str, pages_total: Any, files_total: Any,
                            api_requests_total: Any, api_retries: Any) -> List[str]:
    """
    Extract paginated data from API

    Args:
        metrics: Metrics context
        timestamp: Timestamp string for file naming
        pages_total: Metric for tracking total pages
        files_total: Metric for tracking total files
        
    Returns:
        List of output file paths
    """
    page = 1
    total_breweries = 0
    more_pages = True
    output_files = []
    total_file_size = 0


    while more_pages:
        params = {
            "per_page": API_PER_PAGE_LIMIT,
            "page": page
        }

        response = make_api_request(
            API_BASE_URL,
            params,
            metrics,
            api_requests_total=api_requests_total,
            api_retries=api_retries
        )
        breweries_page = response.json()

        if breweries_page:
            output_file = f"{LANDING_PATH}/breweries_{timestamp}_page{page}.json"
            file_size_bytes = save_json_data(breweries_page, output_file)

            total_file_size += file_size_bytes
            output_files.append(output_file)

            records_in_page = len(breweries_page)
            total_breweries += records_in_page
            metrics.records_processed_total.labels(operation='extract').inc(records_in_page)

            logger.info("Extracted page %d with %d breweries to %s", page, records_in_page, output_file)

            if records_in_page < API_PER_PAGE_LIMIT:
                more_pages = False
            else:
                page += 1
        else:
            more_pages = False

        time.sleep(0.5)

    metrics.data_processed_bytes.labels(operation='extract').set(total_file_size)

    pages_total.set(page)
    files_total.set(len(output_files))

    logger.info("Successfully extracted %d breweries across %d files", total_breweries, len(output_files))

    return output_files
