"""Tests for the silver_to_gold function."""

from unittest.mock import patch, MagicMock
from datetime import datetime

import pandas as pd
from brewery_etl.transformations.silver_to_gold import silver_to_gold

class TestSilverToGold:
    """Test suite for the silver_to_gold function."""

    @patch("brewery_etl.transformations.silver_to_gold.ETLMetricsContext")
    @patch("brewery_etl.transformations.silver_to_gold._load_silver_data")
    @patch("brewery_etl.transformations.silver_to_gold._create_aggregations")
    @patch("brewery_etl.transformations.silver_to_gold._write_aggregation")
    @patch("os.makedirs")
    def test_silver_to_gold(self, mock_makedirs, mock_write, mock_create_aggs, 
                        mock_load, mock_metrics):
        """Test the silver_to_gold function."""
        mock_context = MagicMock()
        mock_metrics.return_value.__enter__.return_value = mock_context

        test_df = pd.DataFrame({
            "id": [1, 2, 3],
            "name": ["Brewery1", "Brewery2", "Brewery3"],
            "country": ["USA", "Canada", "USA"],
            "location": ["USA", "Canada", "USA"],
            "brewery_type": ["micro", "brewpub", "micro"]
        })
        mock_load.return_value = test_df

        aggs = {
            "brewery_by_country": pd.DataFrame({
                "country": ["USA", "Canada"],
                "count": [2, 1]
            }),
            "brewery_by_type": pd.DataFrame({
                "brewery_type": ["micro", "brewpub"],
                "count": [2, 1]
            })
        }
        mock_create_aggs.return_value = aggs

        _ = silver_to_gold(execution_date=datetime(2025, 6, 9))

        mock_makedirs.assert_called_once()
        mock_load.assert_called_once()
        mock_create_aggs.assert_called_once_with(test_df, mock_context)
        assert mock_write.call_count == 2
        assert mock_context.register_metric.call_count >= 2
