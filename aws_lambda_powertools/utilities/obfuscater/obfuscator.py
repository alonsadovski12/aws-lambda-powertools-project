from typing import Any, Callable, Dict, List, Optional

from flatten_dict import flatten, unflatten  # type: ignore[import]

from .filtering_mode import FilterModes


class Obfuscator:
    """Creates and setups an obfuscator to mask chosen keys in objects.
    The client can choose the keys he wants to mask and the masking algorithm
    (the client can use reversible method like encrypt the object with KMS)

    Parameters
    ----------
    filtering_mode : FilterModes, optional
       filter mode which determine to mask white list or black list the keys. Default: FilterModes.BlackList
    obfuscation_method : Callable, optional
        an injection point, lets the client choose his masking algorithm. Default: Replace values with ***

    Examples
    ----------
    Example 1:
    obfuscater = Obfuscator(FilterModes.BlackList)
    obfuscated_object = obfuscater.obfuscate(INPUT_DICT, ['name', 'email'])
    print(obfuscated_object)
    {'id': 111, 'name': '***********', 'email': '*********************',
     'location': {'country': 'israel', 'city': 'Tel Aviv'}}

    Example 2:
    obfuscater = Obfuscator(FilterModes.BlackList, md5)
    obfuscated_object = obfuscater.obfuscate(INPUT_DICT, ['name', 'email'])
    print(obfuscated_object)
    {'id': 111, 'name': '3b2d3431cdc2dc59422eaba64c88393c', 'email': 'adbdfce40472f27324522a8e1bf40b24',
     'location': {'country': 'israel', 'city': 'Tel Aviv'}}


    """

    def __init__(
        self, filtering_mode: Optional[FilterModes] = None, obfuscation_method: Optional[Callable] = None
    ) -> None:
        self.filtering_mode = filtering_mode or FilterModes.BlackList
        self.obfuscation_method: Callable = obfuscation_method or Obfuscator.mask

    @staticmethod
    def mask(input_string: str) -> str:
        return "*" * len(input_string)

    def _flatten_json(self, input_obj: Dict) -> Dict[str, Any]:
        return flatten(input_obj, enumerate_types=(list,))

    def _unflatten(self, dictionary: Dict):
        return unflatten(dictionary)

    def _get_keys_to_obfuscate(self, flattened_obj: Dict, keys_to_filter: List) -> List:
        matched_keys = []
        flattened_keys = list(flattened_obj.keys())
        for filter_key in keys_to_filter:
            for object_key in flattened_keys:
                if filter_key in object_key:
                    matched_keys.append(object_key)

        if self.filtering_mode == FilterModes.BlackList:
            return matched_keys
        return (
            matched_keys
            if self.filtering_mode == FilterModes.BlackList
            else [key for key in flattened_keys if key not in matched_keys]
        )

    def _apply_obfuscation(self, flattened_obj: Dict, keys_to_obfuscate: List) -> None:
        for key in keys_to_obfuscate:
            obfuscated_value = self.obfuscation_method(flattened_obj[key])
            flattened_obj[key] = obfuscated_value

    def obfuscate(self, obj: Dict, keys_to_filter: List) -> Dict:
        if not keys_to_filter:
            return obj
        flattened_obj = self._flatten_json(obj)
        keys_to_obfuscate = self._get_keys_to_obfuscate(flattened_obj, keys_to_filter)
        self._apply_obfuscation(flattened_obj, keys_to_obfuscate)
        return self._unflatten(flattened_obj)
