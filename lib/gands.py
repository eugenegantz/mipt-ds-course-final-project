import json
import re

class GSRef:
    """Таблица кодов товарной номенклатуры"""

    def __init__(self, path):
        self.__gands_by_gsid = dict()

        f = open(path, encoding='utf8')
        gands = json.loads(''.join(f.readlines()))

        for row in gands:
            gsid = row[1]
            _id = row[0]
            self.__gands_by_gsid[gsid] = _id
        #/for
    #/def

    def __getitem__(self, key):
        return self.__gands_by_gsid.get(key, None)

    def __setitem__(self, key, value):
        raise BaseException('denied')

    def __contains__(self, key):
        return key in self.__gands_by_gsid



def remove_gs_tag(s: str) -> str:
    return re.sub(r'\[/?gs\]', '', s, flags=re.IGNORECASE)
#/def



def parse_gs_codes(s: str, remove_tags=True) -> list:
    """Распарсить коды товарной номенклатуры"""
    if not isinstance(s, str):
        return []

    codes = re.findall(r'\[gs\]\w+\[/gs\]', s, re.IGNORECASE)

    if not remove_tags:
        return codes

    return [remove_gs_tag(code) for code in codes]
# /def



def is_contain_gs_tags(s: str) -> bool:
    """Содержит ли строка коды товарной номенклатуры"""
    return bool(
        len(
            parse_gs_codes(s, remove_tags=False)
        )
    )
#/def


def remove_fio_tags(s: str) -> str:
    """Удалить теги [fio]text[/fio] из строки"""
    return re.sub(r'\[/?fio\]', '', s, flags=re.IGNORECASE)