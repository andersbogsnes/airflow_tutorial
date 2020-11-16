from azure.storage.blob import BlobServiceClient
from azure.core.exceptions import ResourceExistsError

with open("test_file.txt", mode="w") as f:
    f.write("Hello from my test file")

client = BlobServiceClient(
    account_url="https://andersdatalake.blob.core.windows.net",
    credential="my_token",
)

try:
    container = client.create_container("raw")
except ResourceExistsError:
    container = client.get_container_client("raw")

blob = container.get_blob_client("anders_demo.txt")

with open("test_file.txt", mode="rb") as f:
    blob.upload_blob(f, overwrite=True)

with open("test_file2.txt", mode="wb") as f:
    stream = blob.download_blob()
    f.write(stream.readall())

container.delete_container()