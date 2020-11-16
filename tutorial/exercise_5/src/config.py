import datetime as dt

from pydantic import BaseSettings, DirectoryPath, HttpUrl


class AzureAuth(BaseSettings):
    account_key: str
    account_name: str

    class Config:
        env_file = "../.env"

    @property
    def connection_string(self):
        return f"DefaultEndpointsProtocol=https;" \
               f"AccountName={self.account_name};" \
               f"AccountKey={self.account_key};" \
               f"EndpointSuffix=core.windows.net"


class Settings(BaseSettings):
    base_url: HttpUrl = "http://data.insideairbnb.com/denmark/hovedstaden/copenhagen"
    tmp_dir: DirectoryPath = "/tmp"
    container_name: str = "/raw"
    auth: AzureAuth
    features_container_name: str = "/features"

    def listing_url(self, date: dt.datetime) -> str:
        return f"{self.base_url}/{date:%Y-%m-%d}/data/listings.csv.gz"

    def listing_location(self, date: dt.datetime) -> str:
        return f"{self.container_name}/listings_{date:%Y_%m_%d}.csv"

    def feature_location(self, feature_name: str) -> str:
        return f"{self.features_container_name}/{feature_name}.parquet"


auth = AzureAuth()
settings = Settings(auth=auth)
