import hashlib
from typing import List

from langchain_core.documents import Document
from langflow.custom import Component
from langflow.io import MessageTextInput, Output, SecretStrInput
from langflow.schema import Data
from pydantic.v1 import SecretStr


class AzureBlobLoader(Component):
    display_name = "Azure Blob Markdown Loader"
    name = "AzureBlobMarkdownLoader"
    description = "Loads markdown files from an Azure Blob Storage container, only retrieving new or changed files."
    icon = "cloud-download"
    # Uses AZURE_STORAGE_CONNECTION_STRING from environment for authentication
    inputs = [
        MessageTextInput(
            name="filter_suffix",
            display_name="File format to filter the files ",
            info="Suffix to filter the files example .md or .pdf etc",
            value=".",
            tool_mode=True,
        ),

        MessageTextInput(
            name="container_name",
            display_name="Azure Blob Container name ",
            info="Name of the Azure blob container on which the listing happens",
            value="",
            required=True,
        ),
        SecretStrInput(
            name="connection_string",
            display_name="Azure connection string",
            info="Connection string to connect to Blob store",
            advanced=False,
            value="AZURE_STORAGE_CONNECTION_STRING",
            required=True,
        ),
    ]

    outputs = [
        Output(display_name="data", name="data", method="build"),
        # Output(display_name="DataFrame", name="dataframe", method="as_dataframe")
    ]

    def build(self) -> List[Data]:
        # Connect to Azure Blob Storage
        container = self.container_name
        Suffix = self.filter_suffix
        conn_str = SecretStr(self.connection_string).get_secret_value() if self.connection_string else None
        if not conn_str:
            raise RuntimeError("Azure connection string not passed. Please pass it in Input")
        from azure.storage.blob import BlobServiceClient
        service_client = BlobServiceClient.from_connection_string(conn_str)
        container_client = service_client.get_container_client(container)

        documents = []
        self.log(f"Following blobs will be processed {container_client.list_blobs()}", )
        # List blobs (filter by prefix if provided) and download .md files
        for blob in container_client.list_blobs():
            if not blob.name.lower().endswith(Suffix):
                continue
            content_bytes = container_client.download_blob(blob.name).readall()
            try:
                text = content_bytes.decode("utf-8")
            except UnicodeDecodeError:
                text = content_bytes.decode("utf-8", errors="ignore")
            checksum = hashlib.md5(content_bytes).hexdigest()  # MD5 checksum for content
            etag = blob.etag.strip('"') if blob.etag else None  # strip quotes from ETag

            self.log(f"Fetched content from {blob.name}" )
            self.log(f"decoded content is {text}")

            # Create Document with content and metadata (source path, etag, checksum)
            doc = Document(page_content=text, metadata={"source": blob.name, "etag": etag, "checksum": checksum})

            documents.append(Data.from_document(doc))
        self.status = f"Processed {len(documents)} files successfully."
        return documents
