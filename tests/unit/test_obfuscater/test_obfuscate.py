from aws_lambda_powertools.utilities.obfuscater.obfuscator import Obfuscator
from aws_lambda_powertools.utilities.obfuscater.filtering_mode import FilterModes
import hashlib

INPUT_DICT = {
    'id': 111,
    'name': 'random_name',
    'email': 'random_name@gmail.com',
    'location': {
        'country': 'israel',
        'city': 'Tel Aviv'
    },
}
INPUT_DICT2 = {
    'pets': [
        {
            'type': 'cat',
            'legs': 4,
            'age': 15
        },
        {
            'type': 'dog',
            'legs': 4,
            'age': 2
        },
    ]
}

def test_find_all_keys_black_list():
    obfuscater = Obfuscator(FilterModes.BlackList)
    flatten_obj = obfuscater._flatten_json(INPUT_DICT2)
    res = obfuscater._get_keys_to_obfuscate(flatten_obj, ['type', 'age'])
    assert ('pets', 0, 'type') in res
    assert ('pets', 1, 'type') in res
    assert ('pets', 0, 'age') in res
    assert ('pets', 1, 'age') in res
    assert len(res) == 4

def test_find_all_keys_white_list():
    obfuscater = Obfuscator(FilterModes.WhiteList)
    flatten_obj = obfuscater._flatten_json(INPUT_DICT2)
    res = obfuscater._get_keys_to_obfuscate(flatten_obj, ['type', 'age'])
    assert ('pets', 0, 'legs') in res
    assert ('pets', 1, 'legs') in res
    assert len(res) == 2

def test_obfuscate():
    obfuscater = Obfuscator(FilterModes.BlackList)
    obfuscated_object = obfuscater.obfuscate(INPUT_DICT, ['name', 'email'])
    assert obfuscated_object.get('name') == len(INPUT_DICT.get('name')) * '*'
    assert obfuscated_object.get('email') == len(INPUT_DICT.get('email')) * '*'

def test_obfuscate_with_callable():
    def md5(string):
        return hashlib.md5(string.encode('utf-8')).hexdigest()
    obfuscater = Obfuscator(FilterModes.BlackList, md5)
    obfuscated_object = obfuscater.obfuscate(INPUT_DICT, ['name', 'email'])
    assert obfuscated_object.get('name') == md5(INPUT_DICT.get('name'))
    assert obfuscated_object.get('email') == md5(INPUT_DICT.get('email'))