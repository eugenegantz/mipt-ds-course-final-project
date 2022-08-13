"""Обработка данных"""

from sklearn.preprocessing import OrdinalEncoder
from sklearn.preprocessing import OneHotEncoder

import sys
import numpy as np
import pandas as pd
import datetime

import lib.gands as gands
import lib.utils as utils

def df_cat_to_int(df: pd.DataFrame, inplace=False) -> pd.DataFrame:
    """Закодировать в числа категориальные признаки"""

    if inplace:
        _df = df
    else:
        _df = df.copy()

    for c in _df.columns:
        if 'category' == _df[c].dtype.name:
            _df[c] = _df[c].cat.codes

    return _df
#/def



def df_cat_to_ord_enc(df: pd.DataFrame, inplace=False) -> pd.DataFrame:
    """Упорядоченное-кодирование категориальных признаков"""

    if inplace:
        _df = df
    else:
        _df = df.copy()

    enc = OrdinalEncoder()

    cols = []

    for c in _df.columns:
        dtname = _df[c].dtype.name

        if 'category' == dtname:
            cols.append(c)

    enc.fit(_df[cols])

    _df[cols] = enc.transform(_df[cols]).astype('int8')

    return _df
#/def



def df_col_to_onehot_enc(
    df: pd.DataFrame,
    cols=None,
    inplace=False,
) -> pd.DataFrame:
    """Onehot-кодирование указанных признаков"""

    if inplace:
        _df = df
    else:
        _df = df.copy()

    if cols is None:
        cols = _df.columns

    if len(cols) == 0:
        return _df

    enc = OneHotEncoder()

    enc.fit(_df[cols])

    labels = np.array([], dtype='object')
    values = np.array([], dtype='object')

    for i, arr in enumerate(enc.categories_):
        values = np.append(values, arr)

    for i, arr in enumerate(enc.categories_):
        arr = arr.astype('object')
        lab = enc.feature_names_in_[i]

        arr.fill(lab)

        labels = np.append(labels, arr)

    enc_arr = enc.transform(_df[cols]).toarray().T.astype('int8')

    for i, series in enumerate(enc_arr):
        k = f'_enc_{labels[i]}_{values[i]}'
        _df[k] = series

    _df.drop(columns=cols, inplace=True)

    return _df
#/def


def df_add_weekday(_df, ts_col_name, weekday_col_name):
    def _applyfunc(ts):
        return datetime.datetime.fromtimestamp(ts)

    # return _df.assign(weekday=_df['gsdate_ts'].apply(_applyfunc).dt.strftime('%u'))

    # Если не указывать rename weekday, то столбец получит тип DataFrame
    # Не знаю. Возможно, это задокументированное поведение или такой тупой баг
    s = _df[ts_col_name].copy().apply(_applyfunc).dt.strftime('%u').rename(weekday_col_name).astype('int8')

    return pd.concat([_df, s], axis=1)




def df_calc_prod_load(_df, inplace=False):
    """
    Расчитать текущую нагрузку
    Будем считать, что текущая нагрузка это суммарное время уже запущеной работы (остаток) на момент запуска новой

    то есть
    row[k][gsdate_ts] < row[i][gsdate_ts] < row[k][gsdate2_ts]
    или
    row[k][время_запуска] < row[i][время_запуска] < row[k][время_завершения]

    где
    row[k] -- Другие задачи (запущенные работы)
    row[i] -- Текущая задача (новая работа)
    """

    if not inplace:
        _df = _df.copy()

    # _df['prod_load'] = 0
    #     _df.loc[:, 'prod_load'] = 0

    # PerformanceWarning: DataFrame is highly fragmented.
    s = pd.Series(data=np.zeros(_df.shape[0]), index=_df.index)
    s.name = 'prod_load'
    _df = pd.concat([_df, s], axis=1)

    for i, row in _df.iterrows():
        mask = (_df['gsdate_ts'] < row['gsdate_ts']) & (row['gsdate_ts'] < _df['gsdate2_ts'])

        d = _df[mask]

        # Вычесть текущую дату из даты до завершения
        # Получить разницу -- время оставшееся до завершения уже запущенных задач
        prod_t_diff = (d['gsdate2_ts'] - row['gsdate_ts']) / 60

        # Суммировать остаток времени недоделаных задач
        _df.loc[i, 'prod_load'] = prod_t_diff.sum()

        # _df.loc[i]['prod_load'] = _df[mask]['prod_t'].sum()

    return _df
#/def



def f_remove_outlier_iqr(s: pd.Series) -> pd.Series:
    """Исключить выбросы методом межквартильного расстояния"""

    q25 = s.quantile(0.25)
    q75 = s.quantile(0.75)

    iqr = q75 - q25
    cut_off = iqr * 1.5

    lower = q25 - cut_off
    upper = q75 + cut_off

    mask = (lower < s) & (s < upper)

    return s.loc[mask]
#/def



def f_remove_outlier_zscore(s: pd.Series) -> pd.Series:
    """Исключить выбросы методом z-score"""

    _df = pd.DataFrame(s)

    m = _df['y'].mean()
    s = _df['y'].std()

    _df['z'] = (_df['y'] - m) / s

    _df_drop = _df[abs(_df['z']) > 3]

    _df.drop(index=_df_drop.index, inplace=True)

    return _df['y']
# /def



class DFTypeCaster:

    def __init__(self, gsref: gands.GSRef):
        self.__gsref = gsref


    def auto_type_cast_df(self, df):
        for c in df.columns:
            # df[c] = self.type_cast_series(
                # df[c],
                # self.get_type_hint(df[c])
            # )

            kwargs = {
                c: self.type_cast_series(df[c], self.get_type_hint(df[c]))
            }

            df = df.assign(**kwargs)
        # /for

        return df
    #/def


    def type_cast_series(self, series, type_hint):
        """Привести поле (series) к указанному типу"""

        series = series.apply(lambda value: self.type_cast_value(value, type_hint))

        if ('gsid' == type_hint) or ('str' == type_hint):
            series = series.astype('category')

        return series
    # /def


    def type_cast_value(self, v, hint):
        """Привести значение к указанному типу"""

        if 'str' == hint:
            return str(v)

        if 'numeric' == hint:
            if not utils.is_numeric(v):
                return 0
            else:
                n = round(float(v), 4)

                if n > sys.maxsize:
                    return 0

                elif n.is_integer():
                    return int(n)

                else:
                    return n
            #/else
        # /if

        if 'gsid' == hint:
            gscodes = gands.parse_gs_codes(v)

            if len(gscodes) > 0:
                return gscodes[0]

            if v in self.__gsref:
                return v

            return ''
        # /if

        return v

    # /def


    def get_type_hint(self, series):
        """Спрогнозировать тип"""

        uniq = series.unique()

        uniq_len = len(uniq)

        types_stats = dict()

        # Подсчитать частоту входа каждого типа
        for v in uniq:
            _type = 'str'  # по умолчанию

            if v == '':
                continue

            # Если является кодом товарной номенклатруы
            if gands.is_contain_gs_tags(v) or (v in self.__gsref):
                _type = 'gsid'

            # Если является числом
            if utils.is_numeric(v):
                _type = 'numeric'

            type_counter = types_stats.get(_type, 0)
            type_counter += 1
            types_stats[_type] = type_counter
        # /for

        for _type in types_stats:
            if (types_stats[_type] / uniq_len) > 0.9:
                return _type
        # /for

        return 'str'
    #/def


def remove_empty_columns(df, inplace=False, print_dropped_columns=False):
    """Очистить все поля, в которых нет значений"""

    cols_to_drop = []

    if not inplace:
        df = df.copy()

    for c in df.columns:
        uniq = df[c].unique()

        if uniq.shape[0] < 2:
            cols_to_drop.append(c)
        # /
    # /for

    df.drop(columns=cols_to_drop, inplace=True)

    if print_dropped_columns:
        print('dropped_columns = ', cols_to_drop)

    return df
# /def