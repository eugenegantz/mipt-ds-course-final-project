import typing
import re
import numbers
import decimal
import math
import datetime


def nget(
        obj: object,
        path: str,
        defval=None,
) -> typing.Any:
    """Вернуть значение свойства сложного объекта по указанному пути"""

    for k in path:
        if hasattr(obj, 'get'):
            obj = obj.get(k, None)
        else:
            obj = defval

    return obj
# /def



def is_telephone_number(s: str) -> bool:
    """Является ли строка телефонным номером"""
    if not isinstance(s, str):
        return False

    # +7 333 333 22 22
    # +38 333 333 22 22

    s = ''.join(re.findall(r'\+\d+', s, flags=re.IGNORECASE))
    m = bool(len(re.findall(r'^\+\d{11,12}$', s, flags=re.IGNORECASE)))

    return m
# /def


def is_email(s: str) -> bool:
    _regex = r'[^@]+@[^@]+\.[^@]+'

    return bool(len(re.findall(_regex, s)))


def is_macro_path(s: str) -> bool:
    return bool(len(re.findall(r'^\[[\w.]+\]', s)))


def is_numeric(
    s: typing.Union[str, numbers.Number]
) -> bool:
    """Является ли значение числом"""

    if isinstance(s, numbers.Number) and math.isfinite(s):
        return True

    if isinstance(s, str):
        return bool(re.match(r"^(-)?(\d+\.)?\d+$", s))

    return False
#/def


def is_int(s: typing.Union[str, numbers.Number]) -> bool:
    """Является ли значение целым числом"""

    if isinstance(s, str):
        if is_numeric(s):
            return float(s).is_integer()
        else:
            return False

    if isinstance(s, int):
        return True

    if isinstance(s, float):
        return s.is_integer()

    return False
# /def


def strptime(s: str) -> typing.Union[datetime.datetime, None]:
    s = s.strip()
    is_date = re.fullmatch(r'\d{1,2}\.\d{1,2}\.\d{1,2}', s)
    is_datetime = re.fullmatch(r'\d{1,2}\.\d{1,2}\.\d{1,2}\s\d{1,2}:\d{1,2}:\d{1,2}', s)

    if is_datetime:
        return datetime.datetime.strptime(s, "%d.%m.%y %H:%M:%S")

    elif is_date:
        return datetime.datetime.strptime(s, "%d.%m.%y")

    else:
        return None
#/def


def is_contains_time(d: datetime.datetime) -> bool:
    return d.time() != datetime.time(0, 0, 0)
#/def


def keyby(arr, key, key2=None):
    res = dict()

    for row in arr:
        res[row[key]] = row[key2] if key2 else row

    return res
#/def


def groupby(arr, key):
    res = dict()

    for row in arr:
        v = row[key]
        res[v] = res.get(v, [])
        res[v].append(row)

    return res
#/def


def omit(_dict, keys, inplace=False):
    if not inplace:
        _dict = _dict.copy()

    for k in keys:
        _dict.pop(k)

    return _dict


def noop(*args, **kwargs):
    return None


def stubTrue(*args, **kwargs):
    return True


def stubFalse(*args, **kwargs):
    return False