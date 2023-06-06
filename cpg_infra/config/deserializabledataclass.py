# pylint: disable=too-many-return-statements,missing-function-docstring
"""
DeserializableDataclass is a dataclass that can be deserialized from a dictionary,
with some extra functionality for parsing types.
"""
import dataclasses
from inspect import isclass
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
            try:
                self.__dict__[fieldname] = try_parse_value_as_type(value, ftype)
            except ValueError as e:
                raise ValueError(
                    f'Error parsing {self.__class__.__name__}.{fieldname} :: {e!r}'
                ) from e

    def __repr__(self):
        args = ', '.join(f'{k}={v!r}' for k, v in vars(self).items())
        return f'{self.__class__.__name__}( {args} )'


def get_display_type_from_value(value):
    """Get display string for type of value"""
    if value is None:
        return 'None'
    return get_display_type(type(value))


def get_display_type(t):
    """Get display string for type, t"""
    if isinstance(t, type(None)):
        return 'NoneType'

    if isclass(t):
        return t.__name__
    if isinstance(t, tuple):
        return ' | '.join(get_display_type(t) for t in t) + ']'
    if isinstance(t, UnionType):
        return ' | '.join(get_display_type(t) for t in get_args(t))

    return repr(t)


def try_parse_value_as_type(value, dtype):
    """
    Try to parse a value as a specific type.
    Raise a ValueError if it can't be parsed.
    """
    if dtype is None or isinstance(dtype, type(None)):
        if value is None:
            return None
        raise ValueError(f'Expected None, got {type(value)} for {value!r}')

    if isinstance(dtype, UnionType):
        dtype = get_args(dtype)

    if isinstance(dtype, tuple):
        # union type
        if len(dtype) == 0:
            # any
            return value

        # union type
        if value is None:
            if any(
                isinstance(t, type(None)) or t is type(None)  # noqa: E721
                for t in dtype
            ):
                return None
            raise ValueError(
                f'Expected (non-optional) {get_display_type(dtype)}, got None'
            )

        union_parse_errors: list[ValueError] = []
        for t in dtype:
            try:
                return try_parse_value_as_type(value, t)
            except ValueError as e:
                # debugging
                if not (t is None or isinstance(t, type(None))):
                    # skip optional errors
                    union_parse_errors.append(e)
                continue
        if len(union_parse_errors) == 1:
            message = (
                f'Could not coerce value of type ({get_display_type_from_value(value)!r}) '
                f'as any of the types in the union: {get_display_type(dtype)}, '
                f'value :: {value!r}'
            )
            raise ValueError(message) from union_parse_errors[0]
        error_message = ''.join(
            [f'\n\t{", ".join(e.args)}' for e in union_parse_errors]
        )
        message = (
            f'Could not coerce value of type ({get_display_type_from_value(value)!r}) '
            f'as any of the types in the union: {get_display_type(dtype)}, value :: {value!r}, '
            f'from errors: {error_message}'
        )
        raise ValueError(message)

    if isinstance(dtype, tuple) and len(dtype) == 1:
        return try_parse_value_as_type(value, dtype[0])

    if dtype is list or get_origin(dtype) is list:
        if not isinstance(value, list):
            raise ValueError(
                f'Expected list, got {get_display_type_from_value(value)} for {value!r}'
            )
        list_types = get_args(dtype)
        if len(list_types) != 1:
            return value
        return [try_parse_value_as_type(v, list_types[0]) for v in value]
    if dtype is dict or get_origin(dtype) is dict:
        if not isinstance(value, dict):
            raise ValueError(
                f'Expected dict, got {get_display_type_from_value(value)} for {value!r}'
            )
        dict_types = get_args(dtype)
        if len(dict_types) != 2:
            # no need for casting
            return value
        return {k: try_parse_value_as_type(v, dict_types[1]) for k, v in value.items()}
    if dtype is set or get_origin(dtype) is set:
        if not isinstance(value, (set, list)):
            raise ValueError(
                f'Expected set, got {get_display_type_from_value(value)} for {value!r}'
            )
        set_types = get_args(dtype)
        if len(set_types) != 1:
            # set[any]
            return set(value)
        return set(try_parse_value_as_type(v, set_types[0]) for v in value)
    if dtype is tuple or get_origin(dtype) is tuple:
        if not isinstance(value, (tuple, list)):
            raise ValueError(
                f'Expected tuple, got {get_display_type_from_value(value)} for {value!r}'
            )
        tuple_types = get_args(dtype)
        if len(tuple_types) == 0:
            return tuple(value)
        if len(tuple_types) != len(value):
            raise ValueError(
                f'Expected tuple of length {len(tuple_types)}, got {len(value)} for {value!r}'
            )
        return tuple(try_parse_value_as_type(v, t) for v, t in zip(value, tuple_types))

    if isinstance(value, dtype):
        return value

    if dtype is not None and issubclass(dtype, DeserializableDataclass):
        if not isinstance(value, dict):
            raise ValueError(f'Expected dict, got {type(value)} for {value!r}')
        return dtype.instantiate(**value)

    if any(dtype is prim for prim in (str, int, bool, float, bytes)):
        return dtype(value)

    raise ValueError(f'Unknown type {get_display_type(dtype)}')
