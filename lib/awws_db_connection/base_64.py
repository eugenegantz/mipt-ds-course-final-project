import base64
import typing
import re


def encode(_input_message: typing.AnyStr = '') -> typing.AnyStr:
	""" encode into base64 """

	if _input_message == '':
		return ''

	_input_message = re.sub(r'\r\n', '\n', _input_message)

	message_bytes = _input_message.encode('utf-8')
	base64_bytes = base64.b64encode(message_bytes)
	base64_string = base64_bytes.decode('utf-8')

	return base64_string[4:5] + base64_string


def decode(_input_base64: typing.AnyStr = '') -> typing.AnyStr:
	""" decode from base64 """

	if _input_base64 == '':
		return ''

	_input_base64 = _input_base64[1:]

	base64_bytes = _input_base64.encode('utf-8')
	message_bytes = base64.b64decode(base64_bytes)
	message_string = message_bytes.decode('utf-8')

	return message_string
