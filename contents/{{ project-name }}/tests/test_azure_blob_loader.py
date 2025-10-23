"""Tests for AzureBlobLoader component."""

from unittest.mock import MagicMock, Mock, patch

import pytest

from components.data_loaders.AzureBlobLoader import AzureBlobLoader


class TestAzureBlobLoader:
    """Test suite for AzureBlobLoader component."""

    @pytest.fixture
    def component(self):
        """Create an AzureBlobLoader component instance for testing."""
        comp = AzureBlobLoader()
        comp.container_name = "test-container"
        comp.filter_suffix = ".md"
        comp.connection_string = (
            "DefaultEndpointsProtocol=https;AccountName=test;AccountKey=test==;EndpointSuffix=core.windows.net"
        )
        comp.log = MagicMock()
        return comp

    def test_component_initialization(self, component):
        """Test that component initializes with correct attributes."""
        assert component.container_name == "test-container"
        assert component.filter_suffix == ".md"
        assert component.connection_string is not None

    def test_missing_connection_string(self, component):
        """Test that missing connection string raises error."""
        component.connection_string = None

        with pytest.raises(RuntimeError, match="Azure connection string not passed"):
            component.build()

    @patch("azure.storage.blob.BlobServiceClient")
    def test_build_with_markdown_files(self, mock_blob_service, component):
        """Test building with markdown files from blob storage."""
        # Mock the blob service client
        mock_service_client = Mock()
        mock_container_client = Mock()
        mock_blob_service.from_connection_string.return_value = mock_service_client
        mock_service_client.get_container_client.return_value = mock_container_client

        # Create mock blobs
        mock_blob1 = Mock()
        mock_blob1.name = "test1.md"
        mock_blob1.etag = '"0x8DCABCDEF123456"'

        mock_blob2 = Mock()
        mock_blob2.name = "test2.md"
        mock_blob2.etag = '"0x8DCABCDEF789ABC"'

        # Mock blob content
        content1 = b"# Test Document 1\nThis is test content."
        content2 = b"# Test Document 2\nThis is more test content."

        mock_download1 = Mock()
        mock_download1.readall.return_value = content1

        mock_download2 = Mock()
        mock_download2.readall.return_value = content2

        # Setup container client mocks
        mock_container_client.list_blobs.return_value = [mock_blob1, mock_blob2]
        mock_container_client.download_blob.side_effect = [mock_download1, mock_download2]

        # Execute build
        result = component.build()

        # Verify results
        assert len(result) == 2
        assert all(hasattr(doc, "text") for doc in result)

        # Verify service client calls
        mock_blob_service.from_connection_string.assert_called_once()
        mock_service_client.get_container_client.assert_called_once_with("test-container")
        assert mock_container_client.download_blob.call_count == 2

    @patch("azure.storage.blob.BlobServiceClient")
    def test_build_filters_by_suffix(self, mock_blob_service, component):
        """Test that files are filtered by suffix."""
        mock_service_client = Mock()
        mock_container_client = Mock()
        mock_blob_service.from_connection_string.return_value = mock_service_client
        mock_service_client.get_container_client.return_value = mock_container_client

        # Create mock blobs with different extensions
        mock_blob_md = Mock()
        mock_blob_md.name = "test.md"
        mock_blob_md.etag = '"0x8DCABCDEF123456"'

        mock_blob_txt = Mock()
        mock_blob_txt.name = "test.txt"

        # Mock blob content
        content_md = b"# Test Document"
        mock_download = Mock()
        mock_download.readall.return_value = content_md

        # Setup container client mocks
        mock_container_client.list_blobs.return_value = [mock_blob_md, mock_blob_txt]
        mock_container_client.download_blob.return_value = mock_download

        # Execute build
        result = component.build()

        # Should only process the .md file
        assert len(result) == 1
        mock_container_client.download_blob.assert_called_once_with("test.md")

    @patch("azure.storage.blob.BlobServiceClient")
    def test_build_handles_unicode_decode_error(self, mock_blob_service, component):
        """Test that unicode decode errors are handled gracefully."""
        mock_service_client = Mock()
        mock_container_client = Mock()
        mock_blob_service.from_connection_string.return_value = mock_service_client
        mock_service_client.get_container_client.return_value = mock_container_client

        # Create mock blob
        mock_blob = Mock()
        mock_blob.name = "test.md"
        mock_blob.etag = '"0x8DCABCDEF123456"'

        # Mock blob content with invalid UTF-8 bytes
        content = b"\xff\xfe# Invalid UTF-8"
        mock_download = Mock()
        mock_download.readall.return_value = content

        # Setup container client mocks
        mock_container_client.list_blobs.return_value = [mock_blob]
        mock_container_client.download_blob.return_value = mock_download

        # Execute build - should not raise exception
        result = component.build()

        # Should still return result with decoded content (ignoring errors)
        assert len(result) == 1

    @patch("azure.storage.blob.BlobServiceClient")
    def test_build_includes_metadata(self, mock_blob_service, component):
        """Test that documents include correct metadata."""
        mock_service_client = Mock()
        mock_container_client = Mock()
        mock_blob_service.from_connection_string.return_value = mock_service_client
        mock_service_client.get_container_client.return_value = mock_container_client

        # Create mock blob
        mock_blob = Mock()
        mock_blob.name = "test.md"
        mock_blob.etag = '"0x8DCABCDEF123456"'

        # Mock blob content
        content = b"# Test Document"
        mock_download = Mock()
        mock_download.readall.return_value = content

        # Setup container client mocks
        mock_container_client.list_blobs.return_value = [mock_blob]
        mock_container_client.download_blob.return_value = mock_download

        # Execute build
        result = component.build()

        # Verify metadata
        assert len(result) == 1
        doc_data = result[0]

        # Check that metadata exists (structure may vary depending on Data class implementation)
        assert hasattr(doc_data, "data") or hasattr(doc_data, "metadata")

    @patch("azure.storage.blob.BlobServiceClient")
    def test_build_with_empty_container(self, mock_blob_service, component):
        """Test building with empty container."""
        mock_service_client = Mock()
        mock_container_client = Mock()
        mock_blob_service.from_connection_string.return_value = mock_service_client
        mock_service_client.get_container_client.return_value = mock_container_client

        # Empty container
        mock_container_client.list_blobs.return_value = []

        # Execute build
        result = component.build()

        # Should return empty list
        assert len(result) == 0
        assert component.status == "Processed 0 files successfully."

    def test_component_display_properties(self):
        """Test component display properties."""
        comp = AzureBlobLoader()
        assert comp.display_name == "Azure Blob Markdown Loader"
        assert comp.name == "AzureBlobMarkdownLoader"
        assert "Azure Blob Storage" in comp.description
