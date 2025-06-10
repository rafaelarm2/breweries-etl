"""Tests for the extract_brewery_data function."""

from unittest.mock import patch, MagicMock
from datetime import datetime

from brewery_etl.transformations.extract_brewery_data import extract_brewery_data


class TestExtractBreweryData:
    """Test suite for the extract_brewery_data function."""

    @patch("brewery_etl.transformations.extract_brewery_data.ETLMetricsContext")
    @patch("brewery_etl.transformations.extract_brewery_data.prepare_landing_directory")
    @patch("brewery_etl.transformations.extract_brewery_data._extract_paginated_data")
    def test_extract_brewery_data(self, mock_extract, mock_prepare, mock_metrics):
        """Test the extract_brewery_data function."""

        mock_context = MagicMock()
        mock_metrics.return_value.__enter__.return_value = mock_context
        mock_extract.return_value = ["file1.json", "file2.json"]

        result = extract_brewery_data(execution_date=datetime(2025, 6, 9))

        mock_prepare.assert_called_once()
        mock_extract.assert_called_once()
        assert result == ["file1.json", "file2.json"]
        assert mock_context.register_metric.call_count == 4
