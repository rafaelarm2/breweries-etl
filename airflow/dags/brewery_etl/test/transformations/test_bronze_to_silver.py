"""Tests for the bronze_to_silver function."""

from datetime import datetime
from unittest.mock import patch, MagicMock

import pandas as pd
from brewery_etl.transformations.bronze_to_silver import bronze_to_silver

class TestBronzeToSilver:
    """Test suite for the bronze_to_silver function."""

    @patch("brewery_etl.transformations.bronze_to_silver.ETLMetricsContext")
    @patch("brewery_etl.transformations.bronze_to_silver._load_bronze_data")
    @patch("brewery_etl.transformations.bronze_to_silver.convert_string_columns")
    @patch("brewery_etl.transformations.bronze_to_silver.standardize_location_fields")
    @patch("brewery_etl.transformations.bronze_to_silver.add_processing_metadata")
    @patch("brewery_etl.transformations.bronze_to_silver._write_to_silver")
    @patch("os.makedirs")
    def test_bronze_to_silver(self, mock_makedirs, mock_write, mock_add_metadata, mock_standardize,
                              mock_convert, mock_load, mock_metrics):
        """Test the bronze_to_silver function."""

        mock_context = MagicMock()
        mock_metrics.return_value.__enter__.return_value = mock_context

        test_df = pd.DataFrame({
            "id": [1, 2],
            "name": ["Brewery1", "Brewery2"],
            "country": ["USA", "Canada"],
            "brewery_type": ["micro", "nano"],
            "state": ["WY", "ON"],
            "city": ["city1", "city2"]
        })
        mock_load.return_value = test_df
        mock_convert.return_value = test_df
        mock_standardize.return_value = test_df

        transformed_df = test_df.copy()
        transformed_df["location"] = transformed_df["country"]
        mock_add_metadata.return_value = transformed_df

        _ = bronze_to_silver(execution_date=datetime(2025, 6, 9))

        mock_load.assert_called_once()
        mock_convert.assert_called_once()
        mock_standardize.assert_called_once()
        mock_add_metadata.assert_called_once()
        mock_write.assert_called_once()
        assert mock_context.register_metric.call_count == 3
