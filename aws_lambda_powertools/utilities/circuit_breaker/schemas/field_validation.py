import dataclasses
import typing
from dataclasses import dataclass

from typing import Set, Type, Union

@dataclass(init=False)
class FieldValidationClass:
    """This dataclass is used for validating an arbitrary input dictionary.
       It will fail is a field is missing, but is forgiving to extra fields.
       Usage:
       Inherit and instantiate your derived class:

       Derived(**some_dict)

       some_dict must have all of Derived's fields.

       init=False - to avoid having to pass all fields explicitly
    """

    def __init__(self, **kwargs):
        fields = set(f.name for f in dataclasses.fields(self))
        mandatory_fields = set(f.name for f in dataclasses.fields(self) if not self._is_optional(f.type))
        # This tests that there are no missing fields in kwargs. extra, unparsed fields are ok
        self._check_mandatory_fields(mandatory_fields, **kwargs)
        for key, value in kwargs.items():
            if key in fields:
                setattr(self, key, value)
        # set default values to missing optional fields
        missing_fields = fields - set(kwargs.keys())
        for field in missing_fields:
            setattr(self, field, None)
        self.__post_init__()

    def __post_init__(self):
        pass

    @staticmethod
    def _check_mandatory_fields(fields: Set[str], **kwargs):
        if fields - set(kwargs.keys()):
            raise ValueError(f'Missing mandatory fields: {fields - set(kwargs.keys())}')
    
    @staticmethod
    def _is_optional(field: Type) -> bool:
        return typing.get_origin(field) is Union and type(None) in typing.get_args(field)