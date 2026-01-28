---
title: Azure Blob Storage
sidebar_position: 4
---

# Azure Blob Storage

[Azure Blob Storage](https://azure.microsoft.com/en-us/products/storage/blobs) (Azure Blob Storage) is a massively scalable and secure object storage for cloud-native workloads, archives, data lakes, HPC, and machine learning.

## Configurations setup

To enabled Azure Blob Storage as remote storage, there are some required configurations that must be added to Fluss' `server.yaml`:

```yaml
# The dir that used to be as the remote storage of Fluss, use the Azure Data Lake Storage URI
remote.data.dir: abfs://flus@flussblob.dfs.core.windows.net/path
# the access key for the azure blob storage account
fs.azure.account.key: 09a295d5-3da5-4435-a660-f438b331ade8
# The oauth account provider type for Token-based Authentication
fs.azure.account.oauth.provider.type: org.apache.fluss.fs.abfs.token.DynamicTemporaryAzureCredentialsProvider
# The oauth2 client id for Token-based Authentication
fs.azure.account.oauth2.client.id: ed953f8a-d5e9-481c-b355-62794f178f66
# The oauth2 client secret for Token-based Authentication
fs.azure.account.oauth2.client.secret: ec29f904-64f6-4372-831a-dc28ec818683
# The oauth2 endpoint to generate access tokens for Token-based Authentication
fs.azure.account.oauth2.client.endpoint: https://login.microsoftonline.com/154b1d91-2d07-4e3a-beb6-9261ab4926ab/oauth2/token
```
