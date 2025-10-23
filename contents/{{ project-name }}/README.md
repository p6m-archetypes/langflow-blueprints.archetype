# Langflow Custom Components

Custom Langflow components for working with Azure Blob Storage and Qdrant vector stores.

## Components

### Azure Blob Markdown Loader

Loads markdown (or other) files from an Azure Blob Storage container, retrieving new or changed files with content checksums and ETags for change detection.

**Features:**
- Configurable file suffix filtering (e.g., `.md`, `.pdf`)
- Automatic change detection using ETags and MD5 checksums
- Full metadata preservation (source path, ETag, checksum)

### Ybor Qdrant Vector Store

Advanced Qdrant vector store component with multiple operation modes for flexible document management.

**Operation Modes:**
- **Upsert**: Update existing documents or add new ones (prevents duplicates)
- **Overwrite**: Replace entire collection or specific documents
- **Append**: Always add documents as new points

**ID Generation Strategies:**
- `content_hash`: MD5 hash of document content
- `source_path`: Hash of source file path
- `etag`: Use Azure Blob ETags (recommended for Azure integration)
- `checksum`: Use content checksums
- `auto_uuid`: Generate random UUIDs

## Installation

### Using uv (recommended)

```bash
# Install dependencies
uv sync

# Install with development tools
uv sync --all-extras
```

### Using pip

```bash
pip install -e .
```

## Configuration

### Azure Blob Storage

Set your Azure Storage connection string:

```bash
export AZURE_STORAGE_CONNECTION_STRING="your-connection-string-here"
```

Or provide it directly in the Langflow component input.

### Qdrant

Configure Qdrant connection in the component:
- **Local**: Set host to `localhost`, port to `6333`
- **Remote**: Provide URL and API key
- **TLS/SSL**: Enable "Prefer gRPC" for better security

## Usage in Langflow

1. Import the components into your Langflow instance
2. Configure the Azure Blob Loader to connect to your storage container
3. Connect the loader output to the Qdrant vector store input
4. Configure your operation mode and ID strategy
5. Run your flow

### Example Flow

```
Azure Blob Loader → Embeddings → Ybor Qdrant → Search/Query
```

## Development

### Running Tests

```bash
uv run pytest
```

### Linting

```bash
# Check code
uv run ruff check .

# Format code
uv run ruff format .
```

### Type Checking

```bash
uv run mypy components/
```

## Project Structure

```
.
├── components/
│   ├── data_loaders/
│   │   └── AzureBlobLoader.py
│   └── vectorstores/
│       └── YborQdrant.py
├── flows/
│   ├── LLMRouting.json
│   └── MainFlow.json
├── pyproject.toml
└── README.md
```

## Requirements

- Python 3.10+
- Langflow 1.0+
- Azure Storage account (for blob loading)
- Qdrant instance (local or cloud)

## License

[Add your license here]
