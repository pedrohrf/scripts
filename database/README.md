# postgresTableToDynamo
This script is used to migrate a table from Postgresql to Dynamodb

## Dependencies
- Python 3.7
- pip
- boto3
- pygresql

## Environment variables
export DATABASE_URL=""
export DATABASE_PORT=""
export DATABASE_NAME=""
export DATABASE_USERNAME=""
export DATABASE_PASSWORD=""
export DYNAMO_DB_ENDPOINT_URL="http://0.0.0.0:8000"
export DYNAMO_DB_USER=""
export DYNAMO_DB_PASSWD=""

## Command line arguments
from_table: name of the table from postgres
to_table: name of the table from dynamo
values: columns which you wish to migrate
key: column which will be the key on dynamo
local: (true or false) if true will use the environment variables to configure your connection with dynamo, else will use aws signature

## Example
python postgresTableToDynamo.py --from_table company --key cnpj --values owner,phone --to_table companies
