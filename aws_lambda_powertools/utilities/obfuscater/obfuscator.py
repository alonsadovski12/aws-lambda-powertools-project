from typing import Callable, Dict, Any, List
from .filtering_mode import FilterModes
from flatten_dict import flatten, unflatten


class Obfuscator:
    def __init__(self, filtering_mode: FilterModes = None, obfuscation_method: Callable = None) -> None:
        self.filtering_mode = filtering_mode if filtering_mode else FilterModes.BlackList
        self.obfuscation_method = obfuscation_method if obfuscation_method else Obfuscator.mask

    @staticmethod
    def mask(input_string: str) -> str:
        return '*' * len(input_string)

    def _flatten_json(self, input_obj: Dict) -> Dict[str, Any]:
        return flatten(input_obj, enumerate_types=(list,))

    def _unflatten(self, dictionary: Dict):
        return unflatten(dictionary)


    def _get_keys_to_obfuscate(self, flattened_obj: Dict, keys_to_filter: List) -> List:
        matched_keys = list()
        flattened_keys = list(flattened_obj.keys())
        for filter_key in keys_to_filter:
            for object_key in flattened_keys:
                if filter_key in object_key:
                    matched_keys.append(object_key)
        
        if self.filtering_mode == FilterModes.BlackList:
            return matched_keys
        return matched_keys if self.filtering_mode == FilterModes.BlackList else  [key for key in flattened_keys if key not in matched_keys]

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
