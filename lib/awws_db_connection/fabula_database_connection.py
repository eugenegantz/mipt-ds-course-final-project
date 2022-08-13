""" Fabula (AwwS) database connection module """

import re
import json
import typing
import datetime
import http.client
import urllib.parse
from .base_64 import encode as base_64_encode
from .fabula_database_response import FabulaDatabaseResponse


class FabulaDatabaseConnection:

	token: typing.Dict = None


	db_server_url: typing.AnyStr = ''


	db_login_hash: typing.AnyStr = ''


	db_login: typing.AnyStr = ''


	db_login_2: typing.AnyStr = ''


	db_name: typing.AnyStr = ''


	db_src: typing.AnyStr = ''


	debug_print_query = False


	debug_print_raw_response_string = False


	def __init__(
		self,
		db_server_url,
		db_name,
		db_src,
		db_login_hash,
		db_login,
		db_login_2,
	):
		self.db_server_url  = db_server_url
		self.db_src         = db_src
		self.db_name        = db_name

		self.db_login       = db_login
		self.db_login_2     = db_login_2
		self.db_login_hash  = db_login_hash


	def __encode_fields(
		self,
		fields          : typing.Dict[str, str],
		request_method  : typing.AnyStr,
	):
		""" create request string """

		sep = ','
		encoded_string = []
		request_method = request_method.upper()

		for k in fields:
			encoded_string.append(k + ':"' + fields[k] + '"')

		encoded_string = "{" + sep.join(encoded_string) + "}"

		if 'GET' == request_method:
			encoded_string = base_64_encode(encoded_string)

		return encoded_string


	def __encode_login(
		self,
		tm              : typing.AnyStr,
		db_name         : typing.AnyStr,
		login           : typing.AnyStr,
		login2          : typing.AnyStr,
		login_hash      : typing.AnyStr,
		login_origin    : typing.AnyStr,
		request_method  : typing.AnyStr,
	):
		""" create login query string """

		fields = {
			'Src'       : 'main',
			'Sql'       : 'Auth',
			'Alias'     : 'Auth',
			'Conf'      : db_name,
			'Login'     : login,
			'Login2'    : login2,
			'Sha1'      : login_hash,
			'Tm'        : tm,
			'Origin'    : login_origin,
		}

		return self.__encode_fields(fields, request_method)


	def __encode_query(
		self,
		token_object    : typing.Dict,
		request_method  : typing.AnyStr,
		db_name         : typing.AnyStr,
		db_src          : typing.AnyStr,
		query           : typing.AnyStr,
		props           : typing.Dict[str, str] = None,
		db_cache        = ''
	):
		""" create query string """

		fields = {
			'id'    : '0',
			'Conf'  : db_name,
			'Src'   : db_src,
			'Login' : '',
			'Pwd'   : '',
			'Cache' : base_64_encode(db_cache),
			'Sql'   : base_64_encode(query),
			'IDS'   : token_object.get('IDS', ''),
			'User'  : token_object.get('User', ''),
		}

		fields.update(props or {})

		return self.__encode_fields(fields, request_method)


	def __validate_token(self, token: typing.Optional[typing.Dict] = None) -> bool:
		if token is None:
			return False

		if token.get('Err'):
			return False

		if token.get('IDS') is None:
			return False

		# TODO validate timestamp: should be today's (token is invalidated every night at 00:00)

		return True


	def login(self, token = None):
		""" auth in database and get secure token """

		if self.__validate_token(token) is True:
			self.token = token

		db_url              = self.db_server_url.strip('\\/')
		db_url_parsed       = urllib.parse.urlparse(db_url)

		db_origin_url       = self.db_server_url.strip('\\/')

		db_host, _, db_port = db_url_parsed.netloc.partition(':')
		db_port             = db_port or None

		request_method      = 'GET'

		now                 = datetime.datetime.now()
		tm                  = now.strftime('%H:%M')

		body = self.__encode_login(
			tm              = tm,
			db_name         = self.db_name,
			login           = self.db_login,
			login2          = self.db_login_2,
			login_hash      = self.db_login_hash,
			login_origin    = db_origin_url,
			request_method  = request_method
		)

		db_url_path = '/login?' + body

		req = http.client.HTTPConnection(db_host, db_port)
		req.request(method=request_method, url=db_url_path)

		res = req.getresponse()

		res_str = res.read().decode('cp1251')
		res_str = re.sub(r"[']", '"', res_str)

		self.token = json.loads(res_str)
		self.token['timestamp'] = datetime.datetime.now().timestamp()

		return self.token


	def __query(
		self,
		query       : typing.AnyStr,
		db_cache    : typing.AnyStr = '*___'
	):
		if self.debug_print_query:
			print(query)

		request_method      = 'POST'

		db_url              = self.db_server_url.strip('\\/')
		db_url_parsed       = urllib.parse.urlparse(db_url)
		db_url_path         = '/db?'

		db_host, _, db_port = db_url_parsed.netloc.partition(':')
		db_port             = db_port or None

		body = self.__encode_query(
			token_object    = self.token,
			db_src          = self.db_src,
			db_name         = self.db_name,
			db_cache        = db_cache,
			request_method  = request_method,
			query           = query,
		)

		req = http.client.HTTPConnection(db_host, db_port)
		req.request(method=request_method, url=db_url_path, body=body)

		res                 = req.getresponse()
		res_str             = res.read().decode('cp1251')

		if self.debug_print_raw_response_string:
			print(res_str)

		res_str = res_str\
			.replace(chr(0), r'\n')\
			.replace(chr(1), r'\n')\
			.replace(chr(2), r'')\
			.replace(r'\\', r'\\\\')\
			.replace('\t', r'\t')

		db_res              = json.loads(res_str)

		return db_res


	def query(
		self,
		query       : typing.AnyStr,
		db_cache    : typing.AnyStr = '*___',
		chunked=False,
		identity_field=None,
	):
		""" request query to awws database. return data from database """

		if chunked:
			dbres = self.__query_chunked(query, identity_field)

		else:
			dbres = self.__query(query, db_cache)

		return FabulaDatabaseResponse(dbres)


	def __get_field_index(self, fld_list: list, fld_key: str) -> int:
		for i, fld in enumerate(fld_list):
			if fld_key.lower() == fld['Name'].lower():
				return i

		return -1


	def __create_empty_dbres(self):
		return {
			'res': [],
			'fld': [],
			'recs': 0,
			't': 0,
			'tt': '',
			'err': '',
		}
	#/def


	def __query_chunked(
		self,
		query,
		identity_field: str,
		offset_rows=20_000
	) -> dict:
		# SELECT __TOP__
		#     __ID__, field_2, field_3
		# FROM Table
		# WHERE
		#     __WHERE_ID__
		#     AND mm IN (...)
		# __ORDER_BY__

		if not re.findall(r'__ID__', query):
			raise BaseException('Not found "__ID__" placeholder')

		if not re.findall(r'__TOP__', query):
			raise BaseException('Not found "__TOP__" placeholder')

		if not re.findall(r'__WHERE_ID__', query):
			raise BaseException('Not found "__WHERE_ID__" placeholder')

		if not re.findall(r'__ORDER_BY__', query):
			raise BaseException('Not found "__ORDER_BY__" placeholder')

		id_column_index = -1
		last_id_value = -1

		dbres = None

		while True:
			_query = query

			query_id        = f"[{identity_field}]"
			query_top       = f"TOP {offset_rows}"
			query_where_id  = f"([{identity_field}] > {last_id_value})"
			query_order_by  = f"ORDER BY [{identity_field}] ASC"

			_query = _query.replace(r'__ID__', query_id)
			_query = _query.replace(r'__TOP__', query_top)
			_query = _query.replace(r'__WHERE_ID__', query_where_id)
			_query = _query.replace(r'__ORDER_BY__', query_order_by)

			_loop_dbres = self.__query(_query)

			if dbres is None:
				# если пустой ответ
				if not _loop_dbres['recs']:
					return self.__create_empty_dbres()

				dbres           = _loop_dbres
				id_column_index = self.__get_field_index(dbres['fld'], identity_field)

			else:
				dbres['res'].extend(_loop_dbres['res'])
				dbres['recs'] = len(dbres['res'])
				dbres['t'] += _loop_dbres['t']
				dbres['tt'] += _loop_dbres['tt']
				dbres['err'] += _loop_dbres['err']
			#/else

			if len(_loop_dbres['res']) < offset_rows:
				break

			last_id_value = _loop_dbres['res'][-1][id_column_index]

		#/while

		return dbres
	#/def
