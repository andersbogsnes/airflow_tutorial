# Setup for Airflow tutorial

Make sure you've setup Azure credentials and that the RG is the correct one

## Generate the Datalakes
Run `python make_azure.py datalake`. 


## Generate ACR
Run `python make_azure.py acr`

## Teardown

Run `python make_azure.py datalake --delete`

Run `python make_azure.py acr --delete`