import datetime as dt

import click
import fsspec
from halo import Halo

import fetch_raw_data
import transform_data
from config import settings


@click.group()
def cli():
    pass


@cli.command()
@click.option("-d", "--date", type=click.DateTime(formats=["%Y-%m-%d"]))
def upload_file(date: dt.datetime):
    url = settings.listing_url(date)
    raw_file = settings.tmp_dir / f"listings_{date:%Y_%m_%d}.csv.gz"

    spinner = Halo()

    spinner.start("Downloading file")
    out_file = fetch_raw_data.download_url(url, raw_file)
    spinner.succeed("File Downloaded!")

    spinner.start("Unzipping file")
    unzipped_file = fetch_raw_data.unzip(out_file)
    spinner.succeed("Unzipped file!")

    spinner.start("Uploading file")
    fetch_raw_data.upload(unzipped_file, container=settings.container_name)
    spinner.succeed("File Uploaded!")
    click.secho("All Done!")


@cli.command()
@click.option("-d", "--date", type=click.DateTime(formats=["%Y-%m-%d"]))
def transform(date: dt.datetime):
    fs = fsspec.filesystem("az", **settings.auth.dict())
    spinner = Halo()

    spinner.start("Fetching raw data")
    df = transform_data.load_raw_data(settings.listing_location(date), fs=fs)
    spinner.succeed("Fetched raw data!")

    spinner.start("Calculating feature")
    df = transform_data.calculate_beds_per_accommodates(df)
    df = transform_data.add_date(df, date)
    spinner.succeed("Feature calculated")

    spinner.start("Uploading feature")
    transform_data.upload_feature("beds_per_accommodates", df, fs=fs)
    spinner.succeed("Feature uploaded")

    click.secho("Data is transformed!")


if __name__ == '__main__':
    cli()
