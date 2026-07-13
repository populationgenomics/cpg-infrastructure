# flake8: noqa:C901,ANN401,PLR2004,ERA001,ANN206,ANN102,ANN204
"""
DeserializableDataclass is a dataclass that can be deserialized from a dictionary,
with some extra functionality for parsing types.
"""

import dataclasses
import types
import typing
from collections.abc import Callable
from inspect import isclass
from typing import Any, Final, get_args, get_origin


@dataclasses.dataclass(frozen=True)
class DeserializableDataclass:
    """
    DeserializableDataclass is a dataclass that can be deserialized from a dictionary,
    with some extra functionality for parsing types.

    We have a __post_init__ here to ensure that subdictionaries are
    parsed into the structure we want, and because the python.dataclasses
    won't do that automatically for us :(
    """

    @classmethod
    def instantiate(cls, **kwargs: dict[str, Any]):
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
                    f'Error parsing {self.__class__.__name__}.{fieldname} :: {e!r}',
                ) from e

    def __repr__(self) -> str:
        args = ', '.join(f'{k}={v!r}' for k, v in vars(self).items())
        return f'{self.__class__.__name__}( {args} )'


def get_display_type_from_value(value: Any):
    """Get display string for type of value"""
    if value is None:
        return 'None'
    return get_display_type(type(value))


def get_display_type(t: Any):
    """Get display string for type, t"""
    if isinstance(t, type(None)):
        return 'NoneType'

    if isclass(t):
        return t.__name__
    if isinstance(t, tuple):
        return ' | '.join(get_display_type(x) for x in t)
    if isinstance(t, types.UnionType):
        return ' | '.join(get_display_type(t) for t in get_args(t))

    return repr(t)


PRIMITIVES: Final[tuple[type, ...]] = (str, int, bool, float, bytes)


def _parse_list(value: Any, args: tuple) -> Any:
    if not isinstance(value, list):
        raise ValueError(
            f'Expected list, got {get_display_type_from_value(value)} for {value!r}',
        )
    if len(args) != 1:
        # list[any]
        return value
    return [try_parse_value_as_type(v, args[0]) for v in value]


def _parse_set(value: Any, args: tuple) -> Any:
    if not isinstance(value, (set, list)):
        raise ValueError(
            f'Expected set, got {get_display_type_from_value(value)} for {value!r}',
        )
    if len(args) != 1:
        # set[any]
        return set(value)
    return {try_parse_value_as_type(v, args[0]) for v in value}


def _parse_dict(value: Any, args: tuple) -> Any:
    if not isinstance(value, dict):
        raise ValueError(
            f'Expected dict, got {get_display_type_from_value(value)} for {value!r}',
        )
    if len(args) != 2:
        # dict[any, any] -- no casting of keys or values
        return value
    return {k: try_parse_value_as_type(v, args[1]) for k, v in value.items()}


def _parse_tuple(value: Any, args: tuple) -> Any:
    if not isinstance(value, (tuple, list)):
        raise ValueError(
            f'Expected tuple, got {get_display_type_from_value(value)} for {value!r}',
        )
    if len(args) == 0:
        # bare tuple -- no element types
        return tuple(value)
    if len(args) == 2 and args[1] is Ellipsis:
        # tuple[X, ...] -- variable-length, homogeneous
        return tuple(try_parse_value_as_type(v, args[0]) for v in value)
    if len(args) != len(value):
        raise ValueError(
            f'Expected tuple of length {len(args)}, got {len(value)} for {value!r}',
        )
    return tuple(try_parse_value_as_type(v, t) for v, t in zip(value, args))


# Dispatch table keyed by a type's origin (get_origin(list[str]) is list), which
# also matches the bare builtin (get_origin(list) is None, so we fall back to
# dtype itself).
CONTAINER_PARSERS: Final[dict[type, Callable[[Any, tuple], Any]]] = {
    list: _parse_list,
    set: _parse_set,
    dict: _parse_dict,
    tuple: _parse_tuple,
}


def _is_none_type(t: Any) -> bool:
    """True if t denotes NoneType as a union member (bare None or NoneType)."""
    return t is None or t is type(None)


def _parse_union(value: Any, args: tuple) -> Any:
    if value is None:
        if any(_is_none_type(t) for t in args):
            return None
        raise ValueError(
            f'Expected (non-optional) {get_display_type(args)}, got None',
        )

    # value is non-None here, so the NoneType branch of an optional can't match
    # it -- drop it so the loop only tries branches that could succeed.
    candidates = [t for t in args if not _is_none_type(t)]
    errors: list[ValueError] = []
    for t in candidates:
        try:
            return try_parse_value_as_type(value, t)
        except ValueError as e:
            errors.append(e)
    detail = ''.join(f'\n\t{", ".join(e.args)}' for e in errors)
    raise ValueError(
        f'Could not coerce value of type ({get_display_type_from_value(value)!r}) '
        f'as any of the types in the union: {get_display_type(args)}, '
        f'value :: {value!r}, from errors: {detail}',
    ) from (errors[0] if errors else None)


def try_parse_value_as_type(value: Any, dtype: Any) -> Any:
    """
    Try to parse a value as a specific type.
    Raise a ValueError if it can't be parsed.
    """
    # Any (or an unparameterised, unrecognised construct) is accepted as-is.
    if dtype is Any:
        return value

    # None / NoneType.
    if dtype is None or dtype is type(None):
        if value is None:
            return None
        raise ValueError(
            f'Expected None, got {get_display_type_from_value(value)} for {value!r}',
        )

    origin = get_origin(dtype)
    args = get_args(dtype)

    # Type *forms* that aren't plain classes: unions and literals.
    match origin:
        case types.UnionType | typing.Union:  # `X | Y`, `Optional[X]`, `Union[...]`
            return _parse_union(value, args)
        case typing.Literal:
            if value not in args:
                raise ValueError(f'Expected literal {args}, got {value!r}')
            return value

    # Otherwise dtype resolves to a concrete class: the generic's origin
    # (`list[str]` -> list) or the bare type itself (dict, a TypedDict, a
    # DeserializableDataclass, int, ...).
    cls = origin or dtype

    # Parameterised/bare containers, with element coercion.
    if parser := CONTAINER_PARSERS.get(cls):
        return parser(value, args)

    if not isclass(cls):
        # Not an introspectable class (TypeVar, ForwardRef, unrecognised special
        # form). Can't coerce -- surface as ValueError per this function's
        # contract, so a union branch falls through instead of raising TypeError.
        raise ValueError(f'Unknown type {get_display_type(dtype)}')

    # Nested dataclasses, built recursively from their mapping form.
    if issubclass(cls, DeserializableDataclass):
        match value:
            case dict():
                return cls.instantiate(**value)
            case cls():  # already deserialised -- pass through
                return value
            case _:
                raise ValueError(f'Expected dict, got {type(value)} for {value!r}')

    # Anything whose runtime type is a dict subclass: a plain dict OR a TypedDict
    # (a TypedDict's MRO includes dict, so it lands here for free -- no
    # is_typeddict needed). Accept any mapping as-is; TypedDicts carry no runtime
    # key/value validation.
    if issubclass(cls, dict):
        match value:
            case dict():
                return value
            case _:
                raise ValueError(
                    f'Expected dict, got {get_display_type_from_value(value)} for {value!r}',
                )

    # Scalars: pass a correct instance through, else coerce a primitive.
    match value:
        case _ if isinstance(value, cls):
            return value
        case _ if cls in PRIMITIVES:
            return cls(value)
        case _:
            raise ValueError(f'Unknown type {get_display_type(dtype)}')
