import os
import argparse
from pg import DB
import boto3

DATABASE_URL = os.environ['DATABASE_URL']
DATABASE_PORT = os.environ.get('DATABASE_PORT', '5432')
DATABASE_NAME = os.environ['DATABASE_NAME']
DATABASE_USERNAME = os.environ['DATABASE_USERNAME']
DATABASE_PASSWORD = os.environ['DATABASE_PASSWORD']

AWS_REGION = os.environ.get('AWS_REGION') or 'us-east-1'
DYNAMO_DB_ENDPOINT_URL = os.environ['DYNAMO_DB_ENDPOINT_URL']
DYNAMO_DB_USER = os.environ['DYNAMO_DB_USER']
DYNAMO_DB_PASSWD = os.environ['DYNAMO_DB_PASSWD']


def get_data(from_table, key, value):
    pg_db = DB(
        dbname=DATABASE_NAME,
        host=DATABASE_URL,
        port=int(DATABASE_PORT),
        user=DATABASE_USERNAME,
        passwd=DATABASE_PASSWORD
    )
    result = pg_db.query("select " + key + " , " + value + " from " + from_table + "")
    return [convert_tuples(tuple, key, value) for tuple in result.getresult()]


def convert_tuples(tuple, key, value):
    value_list = value.split(',')
    ret = {key: tuple[0]}
    i = 1
    for v in value_list:
        ret[v] = remove_empty_strings(tuple[i])
        i += 1
    return ret


def remove_empty_strings(obj):
    for k, v in obj.items():
        if isinstance(v, dict):
            obj[k] = remove_empty_strings(v)
        elif isinstance(v, list):
            obj[k] = [remove_empty_strings(elem) if isinstance(elem, dict) else elem for elem in v]
        elif v == '':
            obj[k] = None
    return obj or None


class DynamoDB:
    def __init__(self, table, key, is_local):
        self.table = table
        self.key = key
        config = self.get_config(is_local)
        self.dynamodb = boto3.resource('dynamodb', **config)
        self.setup_table()

    def get_config(self, is_local):
        config = {'region_name': AWS_REGION}
        if is_local:
            config['endpoint_url'] = DYNAMO_DB_ENDPOINT_URLDYNAMO_DB_ENDPOINT_URL
            config['aws_access_key_id'] = DYNAMO_DB_USER
            config['aws_secret_access_key'] = DYNAMO_DB_PASSWD
        return config

    def create_table(self):
        self.dynamodb.create_table(
            TableName=self.table,
            KeySchema=[
                {
                    'AttributeName': self.key,
                    'KeyType': 'HASH'
                }
            ],
            AttributeDefinitions=[
                {
                    'AttributeName': self.key,
                    'AttributeType': 'S',
                }
            ],
            ProvisionedThroughput={
                'ReadCapacityUnits': 5,
                'WriteCapacityUnits': 5
            },
        )

    def setup_table(self):
        try:
            self.create_table()
        except BaseException:
            print('table already exists')
        finally:
            self.table = self.dynamodb.Table(self.table)

    def write_settings(self, data):
        for dt in data:
            with self.table.batch_writer() as batch:
                batch.put_item(Item=dt)


if __name__ == '__main__':
    p = argparse.ArgumentParser()
    p.add_argument('--from_table', type=str, required=True)
    p.add_argument('--to_table', type=str, required=True)
    p.add_argument('--values', type=str, required=True)
    p.add_argument('--local', type=bool, default=False)
    p.add_argument('--key', type=str, required=True)

    arg = p.parse_args()
    data = get_data(arg.from_table, arg.key, arg.values)
    dynamo = DynamoDB(arg.to_table, arg.key, arg.local)
    dynamo.write_settings(data)
