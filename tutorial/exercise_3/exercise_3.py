import fsspec
from adlfs import AzureBlobFileSystem

fs: AzureBlobFileSystem = fsspec.filesystem("az",
                                            account_name="andersdatalake",
                                            account_key="my_account_key")

with open("hello_fsspec.txt", mode="w") as f:
    f.write("Hello from fsspec!\n")

fs.put("hello_fsspec.txt", "/raw/hello_fsspec.txt", overwrite=True)

fs.download("/raw/hello_fsspec.txt", "hello_fsspec2.txt")

fs.rm("raw/hello_fsspec.txt")