import gzip
import pathlib
from typing import Union

import fsspec
import requests

from config import auth


def download_url(url: str, out_file: Union[str, pathlib.Path]) -> pathlib.Path:
    out_file = pathlib.Path(out_file)
    if out_file.exists():
        return out_file
    r = requests.get(url)
    out_file.write_bytes(r.content)
    return out_file


def unzip(file_name: pathlib.Path) -> pathlib.Path:
    out_file = file_name.with_suffix("")
    if out_file.exists():
        return out_file

    with gzip.open(file_name, mode="rt") as f:
        out_file.write_text(f.read())

    return out_file


def upload(file_name: pathlib.Path, container: str) -> None:
    fs = fsspec.filesystem("az", account_name=auth.account_name, account_key=auth.account_key)
    output_file = f"/{container}/{file_name.name}"
    if not fs.exists(output_file):
        fs.put(str(file_name), output_file)
