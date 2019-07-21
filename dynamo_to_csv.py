import boto3
import pprint
import pandas as pd
import yaml

yaml_dict = yaml.load(open('config.yaml', 'r'))

accessKeyId = yaml_dict['accessKeyId']
secretAccessKey = yaml_dict['secretAccessKey']
region = yaml_dict['region']
TABLE_NAME = yaml_dict['TABLE_NAME']
csv_file_name = yaml_dict['csv_file_name']


class SimpleCounter:
    def __init__(self, total_count):
        self.total_count = total_count
        self.current_count = 0

    def add_count(self, count):
        self.current_count += count

    def add_counts_and_print(self, count):
        self.add_count(count)
        print('\r------------- Current progression: {}% loaded from DB'
              .format(self), end='')

    def __str__(self):
        return '{:6.2f}'.format(self.current_count / self.total_count * 100)


class Client:
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

    def print(self):
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


class ConvertUtil:
    marker = ','

    @classmethod
    def convert(cls, possibly_dict):
        def is_single_dict(given_item):
            return isinstance(given_item, dict) and len(given_item) == 1

        def get_list_output(input_list):
            if isinstance(input_list, dict):
                raise ValueError
            return cls.marker.join(ConvertUtil.convert(val)
                                   for val in input_list)

        if not (isinstance(possibly_dict, dict) or isinstance(possibly_dict, list)):
            return str(possibly_dict)

        if isinstance(possibly_dict, list):
            return '[' + get_list_output(possibly_dict) + ']'

        is_single = is_single_dict(possibly_dict)
        so_far = []
        for key, value in possibly_dict.items():
            if key in ['S', 'N', 'BOOL', 'NULL', 'M']:
                so_far += [ConvertUtil.convert(value)]
            elif key in ['SS', 'NS', 'BS']:
                so_far += ['{' + ConvertUtil.convert(value) + '}']
            elif key in ['L']:
                so_far += ['[' + get_list_output(value) + ']']
            else:
                so_far += [str(key) + ': ' + ConvertUtil.convert(value)]

        result = cls.marker.join(so_far)

        return '{' + result + '}' if not is_single else result

    @classmethod
    def convert_items(cls, given_list):
        '''
        :param given_list: Assumes an iterable from DynamoDB response resulted from key 'Item'
        '''
        assert isinstance(given_list, list)

        for idx, given_dict in enumerate(given_list):
            converted = {key: ConvertUtil.convert(value)
                         for key, value in given_dict.items()}
            # Warning: inplace operation, not functional...
            given_list[idx] = converted

        return given_list


if __name__ == '__main__':
    client = Client()
    client.print()

    confirm = input('*** Enter [Y/y] if all is acceptable, else hit enter : ')
    if confirm in ['Y', 'y']:
        separator_is_tab = input('*** If tab(\\t) is wanted instead of a '
                                 'comma(,) for the csv file, enter [Y/y], '
                                 'else hit enter : ')
        separator = ',' if separator_is_tab not in ['Y', 'y'] else '\t'

        print('------------- Starting the download ---------------')
        data_points = client.load_table_from_dynamodb()
        data_points = ConvertUtil.convert_items(data_points)

        print('\n------------- Finished downloading ---------------')
        print('------------- Saving the downloads ---------------')
        pd.DataFrame(data_points).to_csv(csv_file_name,
                                         sep=separator,
                                         index=False)
