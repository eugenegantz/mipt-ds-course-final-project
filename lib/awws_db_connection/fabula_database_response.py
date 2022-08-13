""" class response """

import typing

class FabulaDatabaseResponse:

	__db_res = None


	def __init__(self, db_res):
		self.__db_res = db_res


	def get_error(self) -> typing.Optional[typing.AnyStr]:
		return self.__db_res['err'] or None


	def __format_key_value(self, db_res: typing.Dict) -> typing.List:
		fields = db_res.get('fld')
		res = db_res.get('res')

		result = []

		for db_row in res:
			row_index = 0
			result_row = {}

			for column_value in db_row:
				column_key = fields[row_index].get('Name')

				result_row[column_key] = column_value

				row_index += 1

			result.append(result_row)

		return result


	def format(self, format: typing.AnyStr = 'key_value') -> typing.Union[typing.List, typing.Dict]:
		if 'key_value' == format:
			return self.__format_key_value(self.__db_res)

		return self.__db_res.res


	def get_raw_result(self):
		return self.__db_res