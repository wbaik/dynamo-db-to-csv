import yaml


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


class Yaml:
    @staticmethod
    def get_config():
        yaml_dict = yaml.load(open('config.yaml', 'r'))
        accessKeyId = yaml_dict['accessKeyId']
        secretAccessKey = yaml_dict['secretAccessKey']
        region = yaml_dict['region']
        TABLE_NAME = yaml_dict['TABLE_NAME']
        csv_file_name = yaml_dict['csv_file_name']
        return accessKeyId, secretAccessKey, region, TABLE_NAME, csv_file_name

