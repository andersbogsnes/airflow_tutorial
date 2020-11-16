from typing import List

import click
from azure.identity import DefaultAzureCredential
from azure.mgmt.containerregistry import ContainerRegistryManagementClient
from azure.mgmt.containerregistry.models import Registry, Sku as ContainerSku
from azure.mgmt.storage import StorageManagementClient
from azure.mgmt.storage.models import StorageAccountCreateParameters, Sku as StorageSku

from cred_adapter import CredentialWrapper

RESOURCE_GROUP = "alm_Sandbox_ML_Pipeline_Test_RG"

NAMES = [
    "emil",
    "stefan",
    "niels",
    "philip",
    "rene",
    "anders",
]

ACCOUNT_NAMES = [f"{name}datalake" for name in NAMES]
ACR_NAMES = [f"{name}featureimages" for name in NAMES]

SUBSCRIPTION_ID = "5da27c63-b4bd-4896-9ef1-bb4fdc140b85"


def make_datalakes(names: List[str], client: StorageManagementClient):
    params = StorageAccountCreateParameters(
        sku=StorageSku(
            name="Standard_LRS"
        ),
        kind="StorageV2",
        location="West Europe",
    )
    for name in names:
        click.secho(f"Creating {name}")
        res = client.storage_accounts.begin_create(resource_group_name=RESOURCE_GROUP,
                                                   account_name=name,
                                                   parameters=params)
        res.wait()
        keys = client.storage_accounts.list_keys(RESOURCE_GROUP, name)
        key = keys.as_dict()["keys"][0]["value"]
        with open("keys.txt", "a") as f:
            f.write(f"{name}: {key}\n")


def delete_datalakes(names: List[str], client: StorageManagementClient):
    for name in names:
        click.secho(f"Deleting {name}")
        client.storage_accounts.delete(RESOURCE_GROUP,
                                       account_name=name)


def make_acr(names: List[str], client: ContainerRegistryManagementClient):
    params = Registry(
        location="West Europe",
        sku=ContainerSku(
            name="Standard"
        ),
        admin_user_enabled=True
    )
    for name in names:
        click.secho(f"Creating {name}")
        res = client.registries.create(resource_group_name=RESOURCE_GROUP,
                                       registry_name=name,
                                       registry=params)
        res.wait()
        auth = client.registries.list_credentials(RESOURCE_GROUP, name).as_dict()
        with open("acr.txt", mode="a") as f:
            username = auth["username"]
            password = auth["passwords"][0]["value"]
            f.write(f"username: {username} password: {password} loginserver: {name}.azurecr.io\n")


def delete_acr(names: List[str], client: ContainerRegistryManagementClient):
    for name in names:
        click.secho(f"Deleting {name}")
        client.registries.delete(RESOURCE_GROUP,
                                 registry_name=name)


@click.group()
def cli():
    pass


@cli.command()
@click.option("-d", "--delete", default=False, is_flag=True)
def datalake(delete):
    client = StorageManagementClient(DefaultAzureCredential(),
                                     subscription_id=SUBSCRIPTION_ID)
    if delete:
        click.secho("Deleting datalakes...")
        if click.confirm("Do you want to delete the datalakes?", abort=True):
            delete_datalakes(ACCOUNT_NAMES, client)
    else:
        click.secho("Making datalakes...")
        make_datalakes(ACCOUNT_NAMES, client)
    click.secho("Done!")


@cli.command()
@click.option("-d", "--delete", default=False, is_flag=True)
def acr(delete):
    creds = CredentialWrapper(DefaultAzureCredential())
    client = ContainerRegistryManagementClient(credentials=creds,
                                               subscription_id=SUBSCRIPTION_ID)
    if delete:
        delete_acr(ACR_NAMES, client)

    else:
        make_acr(ACR_NAMES, client)


if __name__ == '__main__':
    cli()
