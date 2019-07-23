import boto3
import pprint
import pandas as pd

from dynamodb_data_type_unwrapper import DataTypeUnwrapper
from util import SimpleCounter, Yaml

accessKeyId, secretAccessKey, region, TABLE_NAME, csv_file_name = Yaml.get_config()


class DynamodbClient:
    def __init__(self):
        self.client = boto3.client('dynamodb',
                                   aws_access_key_id=accessKeyId,
                                   aws_secret_access_key=secretAccessKey,
                                   region_name=region)
        self.pp = pprint.PrettyPrinter(indent=1)
        self.available_tables = self.list_tables()
        self.description = self.describe_table()
        self.counter = SimpleCounter(self.description['ItemCount'])

    def load_table_from_dynamodb(self):
        scanned_items = self._scan()
        so_far = scanned_items['Items']

        while scanned_items.get('LastEvaluatedKey'):
            self.counter.add_counts_and_print(scanned_items['ScannedCount'])

            scanned_items = self._scan(scanned_items['LastEvaluatedKey'])
            so_far.extend(scanned_items['Items'])

        return so_far

    def _scan(self, last_evaluated_key=None):
        if last_evaluated_key:
            return self.client.scan(TableName=TABLE_NAME,
                                    ExclusiveStartKey=last_evaluated_key)
        return self.client.scan(TableName=TABLE_NAME)

    def log_info(self):
        print('------------- Available tables from the DynamoDB: -------------')
        self.pp.pprint(self.available_tables)

        print('------------- Current table chosen from config : {} -------------'
              .format(TABLE_NAME))

        print('------------- Describing the table -------------')
        self.pp.pprint(self.description)

    def list_tables(self):
        return self.client.list_tables()['TableNames']

    def describe_table(self):
        return self.client.describe_table(TableName=TABLE_NAME)['Table']

    @staticmethod
    def download():
        dynamodb_client = DynamodbClient()
        dynamodb_client.log_info()

        confirm = input('*** Enter [Y/y] if all is acceptable, else hit enter : ')
        if confirm in ['Y', 'y']:
            separator_is_tab = input('*** If tab(\\t) is wanted instead of a '
                                     'comma(,) for the csv file, enter [Y/y], '
                                     'else hit enter : ')
            separator = ',' if separator_is_tab not in ['Y', 'y'] else '\t'

            print('------------- Starting the download ---------------')
            data_points = dynamodb_client.load_table_from_dynamodb()
            data_points = DataTypeUnwrapper.convert_items(data_points)

            print('\n------------- Finished downloading ---------------')
            print('------------- Saving the downloads ---------------')
            pd.DataFrame(data_points).to_csv(csv_file_name,
                                             sep=separator,
                                             index=False)


if __name__ == '__main__':
    DynamodbClient.download()
