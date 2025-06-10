"""Tests for the landing_to_bronze function."""

from datetime import datetime
from unittest.mock import patch, MagicMock

import pandas as pd
from brewery_etl.transformations.landing_to_bronze import landing_to_bronze

class TestLandingToBronze:
    """Test suite for the landing_to_bronze function."""

    @patch("brewery_etl.transformations.landing_to_bronze.ETLMetricsContext")
    @patch("brewery_etl.transformations.landing_to_bronze._get_landing_files")
    @patch("brewery_etl.transformations.landing_to_bronze._process_landing_files")
    @patch("brewery_etl.transformations.landing_to_bronze.add_ingestion_metadata")
    @patch("brewery_etl.transformations.landing_to_bronze._write_to_bronze")
    @patch("os.makedirs")
    def test_landing_to_bronze(self, mock_makedirs, mock_write, mock_add_metadata, 
                            mock_process, mock_get_files, mock_metrics):
        """Test the landing_to_bronze function."""
        mock_context = MagicMock()
        mock_metrics.return_value.__enter__.return_value = mock_context
        mock_get_files.return_value = ["file1.json", "file2.json"]
        mock_process.return_value = ([{"id": 1, "name": "Brewery1"},
                                    {"id": 2, "name": "Brewery2"}], 1000)
        mock_add_metadata.return_value = pd.DataFrame({
            "id": [1, 2], 
            "name": ["Brewery1", "Brewery2"],
            "ingestion_timestamp": ["2025-06-09", "2025-06-09"]
        })

        _ = landing_to_bronze(execution_date=datetime(2025, 6, 9))

        mock_makedirs.assert_called_once()
        mock_get_files.assert_called_once()
        mock_process.assert_called_once_with(["file1.json", "file2.json"], mock_context)
        mock_add_metadata.assert_called_once()
        mock_write.assert_called_once()

        assert mock_context.register_metric.call_count == 3
