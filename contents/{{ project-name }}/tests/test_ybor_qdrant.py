"""Tests for YborQdrant component."""

import hashlib
import uuid
from unittest.mock import MagicMock, Mock, patch

import pytest
from langchain.schema import Document

from components.vectorstores.YborQdrant import YborQdrantComponent


class TestYborQdrantComponent:
    """Test suite for YborQdrant component."""

    @pytest.fixture
    def component(self):
        """Create a YborQdrant component instance for testing."""
        comp = YborQdrantComponent()
        comp.collection_name = "test_collection"
        comp.host = "localhost"
        comp.port = 6333
        comp.grpc_port = 6334
        comp.api_key = None
        comp.prefix = None
        comp.timeout = None
        comp.path = None
        comp.url = None
        comp.distance_func = "Cosine"
        comp.content_payload_key = "page_content"
        comp.metadata_payload_key = "metadata"
        comp.operation_mode = "upsert"
        comp.id_strategy = "etag"
        comp.preserve_existing = True
        comp.prefer_grpc = False
        comp.number_of_results = 4
        comp.search_query = ""
        comp.ingest_data = []
        comp.log = MagicMock()
        return comp

    def test_generate_point_id_content_hash(self, component):
        """Test ID generation using content hash strategy."""
        doc = Document(page_content="test content", metadata={})
        component.id_strategy = "content_hash"

        point_id = component._generate_point_id(doc, "content_hash")

        expected_hash = hashlib.md5("test content".encode("utf-8")).hexdigest()
        assert point_id == expected_hash

    def test_generate_point_id_source_path(self, component):
        """Test ID generation using source path strategy."""
        doc = Document(page_content="test", metadata={"source": "/path/to/file.txt"})
        component.id_strategy = "source_path"

        point_id = component._generate_point_id(doc, "source_path")

        expected_hash = hashlib.md5("/path/to/file.txt".encode("utf-8")).hexdigest()
        assert point_id == expected_hash

    def test_generate_point_id_etag(self, component):
        """Test ID generation using etag strategy."""
        etag = "0x8DCABCDEF123456"
        doc = Document(page_content="test", metadata={"etag": etag})
        component.id_strategy = "etag"

        point_id = component._generate_point_id(doc, "etag")

        # Should convert hex etag to integer
        assert isinstance(point_id, int)
        assert point_id > 0

    def test_generate_point_id_checksum(self, component):
        """Test ID generation using checksum strategy."""
        checksum = "abc123def456"
        doc = Document(page_content="test", metadata={"checksum": checksum})
        component.id_strategy = "checksum"

        point_id = component._generate_point_id(doc, "checksum")

        # Should hash the checksum to get a safe integer
        assert isinstance(point_id, int)
        assert point_id > 0

    def test_generate_point_id_auto_uuid(self, component):
        """Test ID generation using auto_uuid strategy."""
        doc = Document(page_content="test", metadata={})
        component.id_strategy = "auto_uuid"

        point_id = component._generate_point_id(doc, "auto_uuid")

        # Should generate a new UUID as integer
        assert isinstance(point_id, int)
        assert point_id > 0

    @patch("components.vectorstores.YborQdrant.QdrantClient")
    def test_create_collection_if_not_exists_new(self, mock_client_class, component):
        """Test collection creation when it doesn't exist."""
        mock_client = Mock()
        mock_client.get_collection.side_effect = Exception("Collection not found")
        mock_client.create_collection.return_value = None

        component._create_collection_if_not_exists(mock_client, "test_collection", 384)

        mock_client.get_collection.assert_called_once_with("test_collection")
        mock_client.create_collection.assert_called_once()

    @patch("components.vectorstores.YborQdrant.QdrantClient")
    def test_create_collection_if_not_exists_existing(self, mock_client_class, component):
        """Test collection creation when it already exists."""
        mock_client = Mock()
        mock_client.get_collection.return_value = {"status": "ok"}

        component._create_collection_if_not_exists(mock_client, "test_collection", 384)

        mock_client.get_collection.assert_called_once_with("test_collection")
        mock_client.create_collection.assert_not_called()

    @patch("components.vectorstores.YborQdrant.QdrantClient")
    def test_get_existing_point_ids(self, mock_client_class, component):
        """Test retrieving existing point IDs from collection."""
        mock_client = Mock()
        mock_point1 = Mock()
        mock_point1.id = "id1"
        mock_point2 = Mock()
        mock_point2.id = "id2"

        # Simulate two pages of results
        mock_client.scroll.side_effect = [
            ([mock_point1], "offset1"),
            ([mock_point2], None),
        ]

        existing_ids = component._get_existing_point_ids(mock_client, "test_collection")

        assert existing_ids == {"id1", "id2"}
        assert mock_client.scroll.call_count == 2

    def test_operation_mode_values(self, component):
        """Test that operation_mode accepts valid values."""
        valid_modes = ["upsert", "overwrite", "append"]
        for mode in valid_modes:
            component.operation_mode = mode
            assert component.operation_mode == mode

    def test_id_strategy_values(self, component):
        """Test that id_strategy accepts valid values."""
        valid_strategies = ["content_hash", "source_path", "etag", "checksum", "auto_uuid"]
        for strategy in valid_strategies:
            component.id_strategy = strategy
            assert component.id_strategy == strategy

    def test_distance_func_values(self, component):
        """Test that distance_func accepts valid values."""
        valid_functions = ["Cosine", "Euclidean", "Dot Product"]
        for func in valid_functions:
            component.distance_func = func
            assert component.distance_func == func
