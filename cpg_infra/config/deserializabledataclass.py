# pylint: disable=too-many-return-statements,missing-function-docstring
"""
DeserializableDataclass is a dataclass that can be deserialized from a dictionary,
with some extra functionality for parsing types.
"""
import dataclasses
from types import UnionType
from typing import get_args, get_origin


class DeserializableDataclass:
    """
    DeserializableDataclass is a dataclass that can be deserialized from a dictionary,
    with some extra functionality for parsing types.

    We have a __post_init__ here to ensure that subdictionaries are
    parsed into the structure we want, and because the python.dataclasses
    won't do that automatically for us :(
    """

    @classmethod
    def instantiate(cls, **kwargs):
        return cls(**kwargs)

    def __post_init__(self):
        """
        Do correct initialization of subclasses where appropriate
        """
        fields = {field.name: field.type for field in dataclasses.fields(type(self))}

        for fieldname, ftype in fields.items():
            value = self.__dict__.get(fieldname)

            if not value:
                continue
            dtypes = []
            # determine which type we should try to parse the value as
            # handle unions (eg: None | DType)
            if isinstance(ftype, UnionType):
                is_already_correct_type = False
                for dtype in get_args(ftype):
                    if dtype and issubclass(dtype, DeserializableDataclass):
                        # It's a DeserializableDataclass :)
                        dtypes.append(dtype)
                    elif dtype and isinstance(value, dtype):
                        is_already_correct_type = True
                if is_already_correct_type:
                    continue

            elif issubclass(ftype, DeserializableDataclass):
                if isinstance(value, ftype):
                    continue
                dtypes.append(ftype)

            e = None
            # try to see if the value will parse as one of the detected DTypes
            for dtype in dtypes:
                if not isinstance(value, dict):
                    raise ValueError(
                        f'Expected {value} to be a dictionary to parse, got {type(value)}.'
                    )
                try:
                    self.__dict__[fieldname] = dtype(**value)
                    e = None
                    break
                except TypeError as exc:
                    e = exc

            if e:
                raise e

    def __repr__(self):
        args = ', '.join(f'{k}={v!r}' for k, v in vars(self).items())
        return f'{self.__class__.__name__}( {args} )'


def parse_value_from_type(config, fieldname, ftype):
    if ftype is None:
        return None

    if ftype in (list, dict) or get_origin(ftype) in (list, dict):
        ftype_type = ftype if ftype in (list, dict) else get_origin(ftype)
        value = config.get_object(fieldname)

        if value and isinstance(value, ftype_type):
            return value
        if value:
            print(
                f'{fieldname} :: {value} ({type(value)}) was parsed, but was not of type {ftype}'
            )

        return None

    if isinstance(ftype, UnionType) == UnionType:
        for inner_type in get_args(ftype):
            value = parse_value_from_type(config, fieldname, inner_type)
            if value:
                return value

        return None

    if ftype == bool:
        return config.get_bool(fieldname)

    value = config.get(fieldname)
    if value is None:
        return value

    inner_types = get_args(ftype)
    if inner_types:
        for inner_type in inner_types:
            value = parse_value_from_type(config, fieldname, inner_type)
            if value:
                return value
    else:
        try:
            value = ftype(value)
            if value:
                return value
        except (ValueError, TypeError):
            pass

    return None
