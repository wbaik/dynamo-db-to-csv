import unittest

from dynamodb_data_type_unwrapper import DataTypeUnwrapper


class TestConvertUtil(unittest.TestCase):

    def test_convert_singleton_dict(self):

        response_from_db = {
            "Name": {"S": "Alex DeBrie"}
        }
        expected_output = 'Name: Alex DeBrie'

        self.assertEqual(expected_output, DataTypeUnwrapper.convert(response_from_db))

    def test_convert_non_singleton_dict(self):

        response_from_db = {
            "Name": {"S": "Alex DeBrie"},
            "Age": {"N": "29"}
        }
        expected_output = '{Name: Alex DeBrie, Age: 29}'

        self.assertEqual(expected_output, DataTypeUnwrapper.convert(response_from_db))

    def test_convert_non_singleton_dict_with_list(self):

        response_from_db = {
            "Name": {"S": "Alex DeBrie"},
            "Age": {"N": "29"},
            "Roles": {"L": ["Admin", "User"]}
        }

        expected_output = '{Name: Alex DeBrie, ' \
                           'Age: 29, ' \
                           'Roles: [Admin, User]}'

        self.assertEqual(expected_output, DataTypeUnwrapper.convert(response_from_db))

    def test_convert_nested_dict(self):
        response_from_db = {
            "Name": {
                "M": {
                    "Bill Murray": {
                        "Relationship": "Spouse",
                        "Age": 65
                    },
                    "Tina Turner": {
                        "Relationship": "Daughter",
                        "Age": 78,
                        "Occupation": "Singer"
                    }
                }
            },
            "Age": {"N": "29"},
            "Roles": {"L": ["Admin", "User"]}
        }

        expected_output = '{Name: {' \
                                'Bill Murray: {Relationship: Spouse, ' \
                                              'Age: 65}, ' \
                                'Tina Turner: {Relationship: Daughter, ' \
                                              'Age: 78, ' \
                                              'Occupation: Singer}' \
                          '}, ' \
                          'Age: 29, ' \
                          'Roles: [Admin, User]}'

        self.assertEqual(expected_output, DataTypeUnwrapper.convert(response_from_db))

    def test_convert_nested_second(self):
        response_from_db = {
            "Age": {"N": "8"},
            "Colors": {
                "L": [
                    {"S": "White"},
                    {"S": "Brown"},
                    {"S": "Black"}
                ]
            },
            "Name": {"S": "Fido"},
            "Vaccinations": {
                "M": {
                    "Rabies": {
                        "L": [
                            {"S": "2009-03-17"},
                            {"S": "2011-09-21"},
                            {"S": "2014-07-08"}
                        ]
                    },
                    "Distemper": {"S": "2015-10-13"}
                }
            },
            "Breed": {"S": "Beagle"},
            "AnimalType": {"S": "Dog"}
        }
        expected_output = '{Age: 8, ' \
                          'Colors: [White, Brown, Black], ' \
                          'Name: Fido, ' \
                          'Vaccinations: {Rabies: [2009-03-17, 2011-09-21, 2014-07-08], ' \
                                         'Distemper: 2015-10-13}, ' \
                          'Breed: Beagle, ' \
                          'AnimalType: Dog}'
        self.assertEqual(expected_output, DataTypeUnwrapper.convert(response_from_db))


if __name__ == '__main__':
    unittest.main()
