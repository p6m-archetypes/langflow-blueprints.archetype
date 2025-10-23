import hashlib
import uuid
from typing import Dict, List, Optional

from langchain.embeddings.base import Embeddings
from langchain_qdrant import Qdrant
from qdrant_client import QdrantClient
from qdrant_client.models import Distance, PointStruct, VectorParams

from langflow.base.vectorstores.model import LCVectorStoreComponent, check_cached_vector_store
from langflow.helpers.data import docs_to_data
from langflow.io import (
    BoolInput,
    DropdownInput,
    HandleInput,
    IntInput,
    SecretStrInput,
    StrInput,
)
from langflow.schema import Data


class YborQdrantComponent(LCVectorStoreComponent):
    display_name = "Ybor Qdrant"
    description = "Advanced Qdrant Vector Store with multiple operation modes: upsert (prevent duplicates), overwrite (replace collection), and append (always add new)"
    icon = "Qdrant"

    inputs = [
        StrInput(name="collection_name", display_name="Collection Name", required=True),
        StrInput(name="host", display_name="Host", value="localhost", advanced=True),
        IntInput(name="port", display_name="Port", value=6333, advanced=True),
        IntInput(name="grpc_port", display_name="gRPC Port", value=6334, advanced=True),
        SecretStrInput(name="api_key", display_name="API Key", advanced=True),
        StrInput(name="prefix", display_name="Prefix", advanced=True),
        IntInput(name="timeout", display_name="Timeout", advanced=True),
        StrInput(name="path", display_name="Path", advanced=True),
        StrInput(name="url", display_name="URL", advanced=True),
        DropdownInput(
            name="distance_func",
            display_name="Distance Function",
            options=["Cosine", "Euclidean", "Dot Product"],
            value="Cosine",
            advanced=True,
        ),
        StrInput(name="content_payload_key", display_name="Content Payload Key", value="page_content", advanced=True),
        StrInput(name="metadata_payload_key", display_name="Metadata Payload Key", value="metadata", advanced=True),
        DropdownInput(
            name="operation_mode",
            display_name="Operation Mode",
            options=["upsert", "overwrite", "append"],
            value="upsert",
            info="upsert: Update existing/add new (prevents duplicates), overwrite: Replace entire collection, append: Always add as new points",
        ),
        DropdownInput(
            name="id_strategy",
            display_name="ID Generation Strategy",
            options=["content_hash", "source_path", "etag", "checksum", "auto_uuid"],
            value="etag",
            info="Strategy to generate unique IDs for documents (only used in upsert/overwrite modes)",
            advanced=True,
        ),
        BoolInput(
            name="preserve_existing",
            display_name="Preserve Existing Points",
            value=True,
            info="In overwrite mode: if true, keeps existing points not in current batch; if false, deletes entire collection first",
            advanced=True,
        ),
        BoolInput(
            name="prefer_grpc",
            display_name="Prefer gRPC",
            value=False,
            info="Use gRPC connection (supports TLS/SSL better than HTTP). Recommended when using API keys with remote servers.",
            advanced=True,
        ),
        *LCVectorStoreComponent.inputs,
        HandleInput(name="embedding", display_name="Embedding", input_types=["Embeddings"]),
        IntInput(
            name="number_of_results",
            display_name="Number of Results",
            info="Number of results to return.",
            value=4,
            advanced=True,
        ),
    ]

    def _generate_point_id(self, document, strategy: str):
        """Generate a deterministic ID based on the chosen strategy."""
        if strategy == "content_hash":
            # Use MD5 hash of content for ID
            content = document.page_content.encode("utf-8")
            return hashlib.md5(content).hexdigest()

        elif strategy == "source_path":
            # Use source path from metadata
            source = document.metadata.get("source", "")
            if source:
                return hashlib.md5(source.encode("utf-8")).hexdigest()
            return str(uuid.uuid4())

        elif strategy == "etag":
            # Use Azure Blob etag if available, convert to safe integer
            etag = document.metadata.get("etag", "")
            if etag:
                # Clean etag: remove quotes, handle hex prefixes
                cleaned_etag = etag.replace('"', "").replace("0x", "").replace("0X", "")

                # Convert hex etag to integer, but ensure it stays within 64-bit bounds
                if len(cleaned_etag) > 0 and all(c in "0123456789abcdefABCDEF" for c in cleaned_etag):
                    try:
                        # Convert hex to integer, but keep it within safe range
                        hex_value = int(cleaned_etag, 16)
                        # Check if it fits in 64-bit unsigned integer range (0 to 2^64-1)
                        if 0 < hex_value <= (2**64 - 1):
                            return hex_value
                        else:
                            # If too large, hash the etag to get a safe integer
                            return abs(hash(etag)) % (2**63 - 1)
                    except (ValueError, OverflowError):
                        # If conversion fails, hash the etag
                        return abs(hash(etag)) % (2**63 - 1)
                else:
                    # If etag is not hex, hash it to get an integer
                    return abs(hash(etag)) % (2**63 - 1)

            # Fallback to UUID converted to integer if no etag
            return abs(hash(str(uuid.uuid4()))) % (2**63 - 1)

        elif strategy == "checksum":
            # Use Azure Blob checksum if available, convert to safe integer
            checksum = document.metadata.get("checksum", "")
            if checksum:
                # Always hash the checksum to ensure it fits in 64-bit range
                # This ensures consistent, bounded integers regardless of checksum format
                return abs(hash(checksum)) % (2**63 - 1)
            return abs(hash(str(uuid.uuid4()))) % (2**63 - 1)

        elif strategy == "auto_uuid":
            # Always generate new UUID as integer (useful for append mode)
            return abs(hash(str(uuid.uuid4()))) % (2**63 - 1)

        else:
            return abs(hash(str(uuid.uuid4()))) % (2**63 - 1)

    def _create_collection_if_not_exists(self, client: QdrantClient, collection_name: str, vector_size: int):
        """Create collection if it doesn't exist."""
        try:
            client.get_collection(collection_name)
            self.log(f"Collection '{collection_name}' already exists")
        except Exception:
            self.log(f"Creating collection '{collection_name}' with vector size {vector_size}")

            distance_mapping = {
                "Cosine": Distance.COSINE,
                "Euclidean": Distance.EUCLID,
                "Dot Product": Distance.DOT,
            }

            client.create_collection(
                collection_name=collection_name,
                vectors_config=VectorParams(
                    size=vector_size,
                    distance=distance_mapping.get(self.distance_func, Distance.COSINE),
                ),
            )

    def _get_existing_point_ids(self, client: QdrantClient, collection_name: str) -> set:
        """Get all existing point IDs in the collection."""
        try:
            # Scroll through all points to get their IDs
            existing_ids = set()
            offset = None

            while True:
                points, next_offset = client.scroll(
                    collection_name=collection_name,
                    limit=100,
                    offset=offset,
                    with_payload=False,
                    with_vectors=False,
                )

                for point in points:
                    existing_ids.add(str(point.id))

                if next_offset is None:
                    break
                offset = next_offset

            return existing_ids
        except Exception as e:
            self.log(f"Error getting existing point IDs: {e}")
            return set()

    def _perform_upsert_operation(self, client: QdrantClient, collection_name: str, documents: list):
        """Perform upsert operation - update existing points or add new ones."""
        self.log(f"🔄 UPSERT MODE: Processing {len(documents)} documents with ID strategy: {self.id_strategy}")

        # Get embedding dimensions from first document
        first_embedding = self.embedding.embed_query(documents[0].page_content)
        vector_size = len(first_embedding)

        # Ensure collection exists
        self._create_collection_if_not_exists(client, collection_name, vector_size)

        # Prepare points for upsert
        points = []
        for i, doc in enumerate(documents):
            try:
                # Generate deterministic ID
                point_id = self._generate_point_id(doc, self.id_strategy)

                # Validate point ID
                if not point_id or len(str(point_id)) == 0:
                    self.log(f"⚠️ Generated empty ID for doc {i}, using fallback UUID")
                    point_id = str(uuid.uuid4()).replace("-", "")

                # Get embedding for document
                vector = self.embedding.embed_query(doc.page_content)

                # Create point with metadata
                payload = {
                    self.content_payload_key: doc.page_content,
                    self.metadata_payload_key: doc.metadata,
                }

                point = PointStruct(
                    id=point_id,
                    vector=vector,
                    payload=payload,
                )
                points.append(point)

                # Log with more details for debugging
                source = doc.metadata.get("source", "unknown")
                etag = doc.metadata.get("etag", "no-etag")
                checksum = doc.metadata.get("checksum", "no-checksum")
                self.log(f"📝 Prepared point ID: {point_id} (type: {type(point_id).__name__}) from source: {source}")
                if self.id_strategy == "etag":
                    self.log(f"   📋 ETag: {etag}")
                elif self.id_strategy == "checksum":
                    self.log(f"   📋 Checksum: {checksum}")

            except Exception as e:
                self.log(f"❌ Error preparing point {i}: {e}")
                # Create fallback point with UUID
                fallback_id = str(uuid.uuid4()).replace("-", "")
                try:
                    vector = self.embedding.embed_query(doc.page_content)
                    payload = {
                        self.content_payload_key: doc.page_content,
                        self.metadata_payload_key: doc.metadata,
                    }
                    point = PointStruct(
                        id=fallback_id,
                        vector=vector,
                        payload=payload,
                    )
                    points.append(point)
                    self.log(f"🔄 Used fallback ID: {fallback_id[:12]}... for doc {i}")
                except Exception as e2:
                    self.log(f"❌ Failed to create fallback point {i}: {e2}")
                    continue

        # Perform upsert operation
        operation_info = client.upsert(
            collection_name=collection_name,
            points=points,
            wait=True,
        )

        self.log(f"✅ Upsert completed. Operation info: {operation_info}")

    def _perform_overwrite_operation(self, client: QdrantClient, collection_name: str, documents: list):
        """Perform overwrite operation - replace specified documents or entire collection."""
        self.log(f"🔄 OVERWRITE MODE: Processing {len(documents)} documents")

        # Get embedding dimensions from first document
        first_embedding = self.embedding.embed_query(documents[0].page_content)
        vector_size = len(first_embedding)

        if not self.preserve_existing:
            # Delete and recreate entire collection
            self.log("🗑️ Deleting entire collection for complete overwrite")
            try:
                client.delete_collection(collection_name)
            except:
                pass  # Collection might not exist

            self._create_collection_if_not_exists(client, collection_name, vector_size)
        else:
            # Preserve existing points, just overwrite specific ones
            self.log("🔄 Selective overwrite - preserving existing points not in current batch")
            self._create_collection_if_not_exists(client, collection_name, vector_size)

        # Prepare points with deterministic IDs (so we can overwrite specific documents)
        points = []
        for doc in documents:
            point_id = self._generate_point_id(doc, self.id_strategy)
            vector = self.embedding.embed_query(doc.page_content)

            payload = {
                self.content_payload_key: doc.page_content,
                self.metadata_payload_key: doc.metadata,
            }

            point = PointStruct(
                id=point_id,
                vector=vector,
                payload=payload,
            )
            points.append(point)

            self.log(
                f"📝 Prepared overwrite point ID: {point_id[:8]}... from source: {doc.metadata.get('source', 'unknown')}"
            )

        # Perform upsert (which will overwrite existing points with same IDs)
        operation_info = client.upsert(
            collection_name=collection_name,
            points=points,
            wait=True,
        )

        self.log(f"✅ Overwrite completed. Operation info: {operation_info}")

    def _perform_append_operation(self, client: QdrantClient, collection_name: str, documents: list):
        """Perform append operation - always add as new points with unique IDs."""
        self.log(f"🔄 APPEND MODE: Adding {len(documents)} documents as new points")

        # Get embedding dimensions from first document
        first_embedding = self.embedding.embed_query(documents[0].page_content)
        vector_size = len(first_embedding)

        # Ensure collection exists
        self._create_collection_if_not_exists(client, collection_name, vector_size)

        # Prepare points with unique UUIDs (always new points)
        points = []
        for doc in documents:
            # Always generate new UUID for append mode
            point_id = str(uuid.uuid4())
            vector = self.embedding.embed_query(doc.page_content)

            payload = {
                self.content_payload_key: doc.page_content,
                self.metadata_payload_key: doc.metadata,
            }

            point = PointStruct(
                id=point_id,
                vector=vector,
                payload=payload,
            )
            points.append(point)

            self.log(
                f"📝 Prepared new point ID: {point_id[:8]}... from source: {doc.metadata.get('source', 'unknown')}"
            )

        # Perform upsert with unique IDs (effectively append)
        operation_info = client.upsert(
            collection_name=collection_name,
            points=points,
            wait=True,
        )

        self.log(f"✅ Append completed. Operation info: {operation_info}")

    @check_cached_vector_store
    def build_vector_store(self) -> Qdrant:
        qdrant_kwargs = {
            "collection_name": self.collection_name,
            "content_payload_key": self.content_payload_key,
            "metadata_payload_key": self.metadata_payload_key,
        }

        server_kwargs = {
            "host": self.host or None,
            "port": int(self.port),
            "grpc_port": int(self.grpc_port),
            "api_key": self.api_key,
            "prefix": self.prefix,
            "timeout": int(self.timeout) if self.timeout else None,
            "path": self.path or None,
            "url": self.url or None,
        }

        # Remove None values
        server_kwargs = {k: v for k, v in server_kwargs.items() if v is not None}

        # Add gRPC preference if specified (helps with TLS/SSL and API key warnings)
        if self.prefer_grpc:
            server_kwargs["prefer_grpc"] = True

        # Convert DataFrame to Data if needed using parent's method
        self.ingest_data = self._prepare_ingest_data()

        documents = []
        for _input in self.ingest_data or []:
            if isinstance(_input, Data):
                documents.append(_input.to_lc_document())
            else:
                documents.append(_input)

        if not isinstance(self.embedding, Embeddings):
            msg = "Invalid embedding object"
            raise TypeError(msg)

        # Create QdrantClient for direct operations
        client = QdrantClient(**server_kwargs)

        if documents:
            self.log(f"🚀 Starting {self.operation_mode.upper()} operation with {len(documents)} documents")

            # Route to appropriate operation based on mode
            if self.operation_mode == "upsert":
                self._perform_upsert_operation(client, self.collection_name, documents)
            elif self.operation_mode == "overwrite":
                self._perform_overwrite_operation(client, self.collection_name, documents)
            elif self.operation_mode == "append":
                self._perform_append_operation(client, self.collection_name, documents)
            else:
                raise ValueError(f"Unknown operation mode: {self.operation_mode}")

            # Create Qdrant vector store from existing collection
            qdrant = Qdrant(client=client, embeddings=self.embedding, **qdrant_kwargs)

        else:
            # No documents, create empty vector store
            self.log("No documents to process, creating empty vector store")
            qdrant = Qdrant(embeddings=self.embedding, client=client, **qdrant_kwargs)

        # Get final collection stats
        try:
            collection_info = client.get_collection(self.collection_name)
            self.log(f"📊 Final collection stats: {collection_info.points_count} total points")
        except:
            pass

        return qdrant

    def search_documents(self) -> list[Data]:
        vector_store = self.build_vector_store()

        if self.search_query and isinstance(self.search_query, str) and self.search_query.strip():
            docs = vector_store.similarity_search(
                query=self.search_query,
                k=self.number_of_results,
            )

            data = docs_to_data(docs)
            self.status = data
            return data
        return []
