#!/usr/bin/env python
'''A module which implements the MySQL server protocol, giving you the ability to create MySQL-like services

It's a ported version of the library: https://github.com/CloudQuote/faux-mysql-server/blob/master/src/index.js

run in the background :
nohup python36 ./as_mysql.py  >/dev/null 2>&1 &

Windows Debugging:
/cygdrive/c/Program\ Files\ \(x86\)/Python36-32/python.exe -u as_mysql.py

'''

from threading import Thread
import traceback
import os
from binascii import hexlify
import socket
import random
import platform

print('Start server...')

HOST, PORT = '', 3306 # default MySQL Server Port

__author__ = 'Zac Sadan'
__license__ = "GPL"
__version__ = "1.0.0"


MYSERVER_PACKET_COUNT = 0
MYSERVER_SOCKET = 1
MYSERVER_DATABASE = 4
MYSERVER_THREAD_ID = 5
MYSERVER_SCRAMBLE = 6
MYSERVER_DBH = 7
MYSERVER_PARSER = 8
MYSERVER_BANNER = 9
MYSERVER_SERVER_CHARSET = 10
MYSERVER_CLIENT_CHARSET = 11
MYSERVER_SALT = 12

FIELD_CATALOG = 0
FIELD_DB = 1
FIELD_TABLE = 2
FIELD_ORG_TABLE = 3
FIELD_NAME = 4
FIELD_ORG_NAME = 5
FIELD_LENGTH = 6
FIELD_TYPE = 7
FIELD_FLAGS = 8
FIELD_DECIMALS = 9
FIELD_DEFAULT = 10

# This comes from include/mysql_com.h of the MySQL source

CLIENT_LONG_PASSWORD = 1
CLIENT_FOUND_ROWS = 2
CLIENT_LONG_FLAG = 4
CLIENT_CONNECT_WITH_DB = 8
CLIENT_NO_SCHEMA = 16
CLIENT_COMPRESS= 32 # Must implement that one
CLIENT_ODBC = 64
CLIENT_LOCAL_FILES = 128
CLIENT_IGNORE_SPACE = 256
CLIENT_PROTOCOL_41 = 512
CLIENT_INTERACTIVE = 1024
CLIENT_SSL = 2048 # Must implement that one
CLIENT_IGNORE_SIGPIPE = 4096
CLIENT_TRANSACTIONS = 8192
CLIENT_RESERVED = 16384
CLIENT_SECURE_CONNECTION = 32768
CLIENT_MULTI_STATEMENTS = 1 << 16
CLIENT_MULTI_RESULTS = 1 << 17
CLIENT_SSL_VERIFY_SERVER_CERT = 1 << 30
CLIENT_REMEMBER_OPTIONS = 1 << 31

SERVER_STATUS_IN_TRANS = 1
SERVER_STATUS_AUTOCOMMIT = 2
SERVER_MORE_RESULTS_EXISTS = 8
SERVER_QUERY_NO_GOOD_INDEX_USED = 16
SERVER_QUERY_NO_INDEX_USED = 32
SERVER_STATUS_CURSOR_EXISTS = 64
SERVER_STATUS_LAST_ROW_SENT = 128
SERVER_STATUS_DB_DROPPED = 256
SERVER_STATUS_NO_BACKSLASH_ESCAPES = 512

COM_SLEEP = 0
COM_QUIT = 1
COM_INIT_DB= 2
COM_QUERY = 3
COM_FIELD_LIST = 4
COM_CREATE_DB = 5
COM_DROP_DB = 6
COM_REFRESH = 7
COM_SHUTDOWN = 8
COM_STATISTICS = 9
COM_PROCESS_INFO = 10
COM_CONNECT = 11
COM_PROCESS_KILL = 12
COM_DEBUG = 13
COM_PING = 14
COM_TIME = 15
COM_DELAYED_INSERT = 16
COM_CHANGE_USER = 17
COM_BINLOG_DUMP = 18
COM_TABLE_DUMP = 19
COM_CONNECT_OUT = 20
COM_REGISTER_SLAVE = 21
COM_STMT_PREPARE = 22
COM_STMT_EXECUTE = 23
COM_STMT_SEND_LONG_DATA = 24
COM_STMT_CLOSE = 25
COM_STMT_RESET = 26
COM_SET_OPTION = 27
COM_STMT_FETCH = 28
COM_END = 29

# This is taken from include/mysql_com.h

MYSQL_TYPE_DECIMAL = 0
MYSQL_TYPE_TINY = 1
MYSQL_TYPE_SHORT = 2
MYSQL_TYPE_LONG = 3
MYSQL_TYPE_FLOAT = 4
MYSQL_TYPE_DOUBLE = 5
MYSQL_TYPE_NULL = 6
MYSQL_TYPE_TIMESTAMP = 7
MYSQL_TYPE_LONGLONG = 8
MYSQL_TYPE_INT24 = 9
MYSQL_TYPE_DATE = 10
MYSQL_TYPE_TIME = 11
MYSQL_TYPE_DATETIME = 12
MYSQL_TYPE_YEAR = 13
MYSQL_TYPE_NEWDATE = 14
MYSQL_TYPE_VARCHAR = 15
MYSQL_TYPE_BIT = 16
MYSQL_TYPE_NEWDECIMAL = 246
MYSQL_TYPE_ENUM = 247
MYSQL_TYPE_SET = 248
MYSQL_TYPE_TINY_BLOB = 249
MYSQL_TYPE_MEDIUM_BLOB = 250
MYSQL_TYPE_LONG_BLOB = 251
MYSQL_TYPE_BLOB = 252
MYSQL_TYPE_VAR_STRING = 253
MYSQL_TYPE_STRING = 254
MYSQL_TYPE_GEOMETRY = 255

NOT_NULL_FLAG = 1
PRI_KEY_FLAG = 2
UNIQUE_KEY_FLAG = 4
MULTIPLE_KEY_FLAG = 8
BLOB_FLAG = 16
UNSIGNED_FLAG = 32
ZEROFILL_FLAG = 64
BINARY_FLAG = 128
ENUM_FLAG = 256
AUTO_INCREMENT_FLAG = 512
TIMESTAMP_FLAG = 1024
SET_FLAG = 2048
NO_DEFAULT_VALUE_FLAG = 4096
NUM_FLAG = 32768

def hex_subset(data):
	return ' '.join('%02x' % b for b in data)

class Server(object):

	def __init__(self, opts):
		print('Initializing Server...')
		if type(opts) == dict:
			for opt_name, opt_value in opts.items():
				setattr(self, opt_name, opt_value)
		if not hasattr(self, 'banner'):
			self.banner = 'MyServer/1.0'
		if not hasattr(self, 'salt'):
		   self.salt = os.urandom(20)
		self.sequence = 0
		self.onPacket = self.helloPacketHandler
		self.incoming = []
		self.packetCount = 0
		self.shutdown_flag = socket.SHUT_RDWR

		self.sendServerHello()

	# external handler	
	def handleDisconnect(self):		
		print('disconnect')

	def writeHeader(self, data, length):
		data.writeUIntLE(length - 4, 0, 3)
		data.writeUInt8(self.sequence % 256, 3)
		self.sequence += 1		

	def sendPacket(self, payload):
		return self.socket.send(payload.data)

	def newDefinition(self, params):
		return {
			'catalog': params.get('catalog', 'def'),
			'schema': params.get('db'),
			'table': params.get('table'),
			'orgTable': params.get('orgTable'),
			'name': params.get('name'),
			'orgName': params.get('orgName'),
			'length': params.get('length', 0),
			'type': params.get('type', MYSQL_TYPE_STRING),
			'flags': params.get('flags', 0),
			'decimals': params.get('decimals'),
			'default': params.get('default')
		}

	def sendDefinitions(self, definitions):
		# Write Definition Header
		payload = Buffer(1024)
		length = 4
		length = self.writeLengthCodedBinary(payload, length, len(definitions))
		self.writeHeader(payload, length)
		self.sendPacket(payload.slice(0, length))
		
		# Write each definition
		length = 4
		val = None
		for definition in definitions:
			for field in ['catalog', 'schema', 'table', 'orgTable', 'name', 'orgName']:
				val = definition.get(field, '')				
				length = self.writeLengthCodedString(payload, length, val)				
		
			length = payload.writeUInt8(0x0C, length)
			length = payload.writeUInt16LE(11, length) # ASCII
			length = payload.writeUInt32LE(definition.get('columnLength'), length)
			length = payload.writeUInt8(definition.get('columnType'), length)
			length = payload.writeUInt16LE(definition.get('flags', 0), length)
			length = payload.writeUInt8(definition.get('decimals', 0), length)
			length = payload.writeUInt16LE(0, length) # \0\0 FILLER
			length = self.writeLengthCodedString(payload, length, definition.get('default'))
			self.writeHeader(payload, length)

			self.sendPacket(payload.slice(0, length))
  
		self.sendEOF()

	def sendRow(self, row):
		payload = Buffer(1024)
		length = 4
		for cell in row:
			if cell is None:
				length = payload.writeUInt8(0xFB, length)
			else:
				length = self.writeLengthCodedString(payload, length, cell)
		
		self.writeHeader(payload, length)
		self.sendPacket(payload.slice(0, length))

	def sendRows(self, rows = []):
		for row in rows:
			self.sendRow(row)
		self.sendEOF()

	def sendEOF(self, args=None):
		# Write EOF
		def_args = { 'warningCount': 0, 'serverStatus': SERVER_STATUS_AUTOCOMMIT }
		args = self.get_args_with_defaults(args, def_args)
		payload = Buffer(16)
		length = 4
		length = payload.writeUInt8(0xFE, length)
		length = payload.writeUInt16LE(args.get('warningCount'), length)
		length = payload.writeUInt16LE(args.get('serverStatus'), length)
		self.writeHeader(payload, length)
		self.sendPacket(payload.slice(0, length))

	def sendServerHello(self):
		## Sending Server Hello...
		payload = Buffer(128)
		pos = 4
		pos = payload.writeUInt8(10, pos) # Protocol version
		pos += payload.write(getattr(self, 'banner', 'MyServer/1.0'), pos)
		pos = payload.writeUInt8(0, pos)

		pos = payload.writeUInt32LE(os.getpid(), pos)

		pos += Buffer.copyToBuffer(self.salt, payload, pos, 0, 8)

		pos = payload.writeUInt8(0, pos)

		pos = payload.writeUInt16LE(
			CLIENT_LONG_PASSWORD | 
			CLIENT_CONNECT_WITH_DB | 
			CLIENT_PROTOCOL_41 | 
			CLIENT_SECURE_CONNECTION
			, pos)

		# default is latin1
		pos = payload.writeUInt8(getattr(self,'serverCharset', 0x21), pos)

		pos = payload.writeUInt16LE(SERVER_STATUS_AUTOCOMMIT, pos)

		payload.fill(0, pos, pos + 13)
		pos += 13

		pos += Buffer.copyToBuffer(self.salt, payload, pos, 8)
		pos = payload.writeUInt8(0, pos)
		self.writeHeader(payload, pos)
		return self.sendPacket(payload.slice(0, pos))

	# external handler
	def handleData(self, data):
		if data is not None and len(data) > 0:
		   self.incoming.append(data)
		self.gatherIncoming('from handleData')
		if data is None:
			print('Connection closed')
			# https://docs.python.org/3.5/library/socket.html#socket.socket.close
			self.socket.shutdown(self.shutdown_flag)
			self.socket.close()
	
	def gatherIncoming(self, wherewas):
		incoming_len = len(self.incoming)		
		if incoming_len > 0:
			incoming = None
			if incoming_len == 1:
				incoming = Buffer(self.incoming[0])
			else:
				length = 0
				for buf in self.incoming:
					length += len(buf)
				incoming = Buffer(length)
				length = 0
				for buf in self.incoming:
					length += Buffer.copyToBuffer(buf, incoming, length)
			remaining = self.readPackets(incoming)
			self.incoming = [remaining.data]


	def readPackets(self, buf):
		offset = 0
		data = None
		packetLength = None
		while True:
			data = buf.slice(offset)
			if data.length() < 4:
				return data
  
			packetLength = data.readUIntLE(0, 3)

			if data.length() < packetLength + 4:
				return data

			self.sequence = data.readUIntLE(3) + 1
			offset += packetLength + 4
			packet = data.slice(4, packetLength + 4)
			self.onPacket(packet)
			self.packetCount += 1

	# handler for self.onPacket		
	def helloPacketHandler(self, packet):
		## Reading Client Hello...
		# http://dev.mysql.com/doc/internals/en/the-packet-header.html

		if packet.length() == 0:
			return self.sendError({ 'message': 'Zero length hello packet' })

		ptr = 0

		clientFlags = packet.slice(ptr, ptr + 4)
		ptr += 4

		maxPacketSize = packet.slice(ptr, ptr + 4)
		ptr += 4

		self.clientCharset = packet.readUInt8(ptr)
		ptr += 1
 
		some_filler = packet.slice(ptr, ptr + 23)
		ptr += 23

		usernameEnd = packet.indexOf(0, ptr)
		username = packet.toString('ascii', ptr, usernameEnd)
		ptr = usernameEnd + 1

		scrambleLength = packet.readUInt8(ptr)
		ptr += 1

		if scrambleLength > 0:
			self.scramble = packet.slice(ptr, ptr + scrambleLength).data
			ptr += scrambleLength
 
		database = None
		databaseEnd = packet.indexOf(0, ptr)
		if databaseEnd >= 0:
			database = packet.toString('ascii', ptr, databaseEnd)

		self.onPacket = None
	
		try: 
			authorized = self.onAuthorize({ 'client_flags': clientFlags.data, 
										  'max_packet_size': maxPacketSize.data, 
										  'username': username, 
										  'database': database })
			if not authorized: 
				raise ServerError('Not Authorized')

			self.onPacket = self.normalPacketHandler
			self.sendOK({ 'message': 'OK' })

		except:
			self.sendError({ 'message': 'Authorization Failure' })
			self.socket.shutdown(self.shutdown_flag)
			self.socket.close()

	# handler for self.onPacket		
	def normalPacketHandler(self, packet):
		if packet is None:
			raise ServerError('Empty packet')
		return self.onCommand(self, {
			'command': packet.readUInt8(0),
			'extra': packet.slice(1).data if packet.length() > 1 else None
		})
		
	def get_args_with_defaults(self, args, def_args):
		if args is None:
			args = {}
		for key, value in def_args.items():
			if args.get(key) is None and value is not None:
				args[key] = value
		return args
	
	def sendOK(self, args=None):
		def_args = { 'message': None, 'affectedRows': 0, 'insertId': None, 'warningCount': 0 }
		args = self.get_args_with_defaults(args, def_args)

		data = Buffer(len(args.get('message')) + 64)
		length = 4
		length = data.writeUInt8(0, length)
		length = self.writeLengthCodedBinary(data, length, args.get('affectedRows'))
		length = self.writeLengthCodedBinary(data, length, args.get('insertId'))
		length = data.writeUInt16LE(SERVER_STATUS_AUTOCOMMIT, length)
		length = data.writeUInt16LE(args.get('warningCount', 0), length)
		length = self.writeLengthCodedString(data, length, args.get('message'))
		self.writeHeader(data, length)

		self.sendPacket(data.slice(0, length))

	def sendError(self, args=None):
		## Sending Error ...
		def_args = { 'message': 'Unknown MySQL error', 'errno': 2000, 'sqlState': 'HY000'}		
		args = self.get_args_with_defaults(args, def_args)
		data = Buffer(len(args.get('message')) + 64)
		length = 4
		length = data.writeUInt8(0xFF, length)
		length = data.writeUInt16LE(args.get('errno'), length)
		length += data.write('#', length)
		length += data.write(args.get('sqlState'), length, 5)
		length += data.write(args.get('message'), length)
		length = data.writeUInt8(0, length)

		self.writeHeader(data, length)
		self.sendPacket(data.slice(0, length))

	def writeLengthCodedString(self, buf, pos, strval):
		if strval is None:
			return buf.writeUInt8(0, pos) 
		if type(strval) != str:
			#Mangle it
			strval = str(strval)
		buf.writeUInt8(253, pos)
		buf.writeUIntLE(len(strval), pos + 1, 3)
		buf.write(strval, pos + 4)
		return pos + len(strval) + 4

	def writeLengthCodedBinary(self, buf, pos, int_value):
		if int_value is None: 
		   return buf.writeUInt8(251, pos)
		elif int_value < 251:
		   return buf.writeUInt8(int_value, pos)		
		elif int_value < 0x10000:
			buf.writeUInt8(252, pos)
			buf.writeUInt16LE(int_value, pos + 1)
			return pos + 3
		elif int_value < 0x1000000:
			buf.writeUInt8(253, pos)
			buf.writeUIntLE(int_value, pos + 1, 3)
			return pos + 4
		else:
			buf.writeUInt8(254, pos)
			buf.writeUIntLE(int_value, pos + 1, 8)
			return pos + 9

class ServerError(Exception):
	"""Common class for exceptions in this module."""

	def __init__(self, message):
		self.message = message

class Buffer(object):

	def __init__(self, init_value):
		if type(init_value) == int:
			self.data = bytearray(init_value)
			size = init_value
		else:
			self.data = bytearray(init_value)
			size = len(init_value)
		self.size = size
		self.encoding = 'utf-8'

	# https://nodejs.org/api/buffer.html#buffer_buf_copy_target_targetstart_sourcestart_sourceend	
	def copy(self, target_buf, target_start_pos = 0, 
			 source_start_pos = 0, source_end_pos = None):
		return self.copyToBuffer(self.data, target_buf, target_start_pos, 
								 source_start_pos, source_end_pos)

	@staticmethod
	def copyToBuffer(source_data, target_buf, target_start_pos = 0, 
			 source_start_pos = 0, source_end_pos = None):
		if source_end_pos is None:
			source_end_pos = len(source_data)	
		target_available_bytes = target_buf.length() - target_start_pos - 1
		source_bytes_num = source_end_pos - source_start_pos
		copied_bytes_num = 0
		if target_available_bytes < source_bytes_num:
			real_source_end_pos = source_start_pos + target_available_bytes + 1
			copied_bytes_num = target_available_bytes
		else:
			real_source_end_pos = source_end_pos	
			copied_bytes_num = source_bytes_num
		target_end_pos = target_start_pos + source_bytes_num
		target_buf.data[target_start_pos:target_end_pos] = (
			source_data[source_start_pos:real_source_end_pos])
		return copied_bytes_num

	# https://nodejs.org/api/buffer.html#buffer_buf_fill_value_offset_end_encoding	
	def fill(self, value, start_pos = 0, end_pos = None):
		if end_pos is None:
			end_pos = self.size
		bytes_number = end_pos - start_pos
		self.data[start_pos:end_pos] = bytes([value for i in range(end_pos - start_pos)])

	# https://nodejs.org/api/buffer.html#buffer_buf_indexof_value_byteoffset_encoding	
	def indexOf(self, value, pos):
		return self.data.find(value, pos)

	# https://nodejs.org/api/buffer.html#buffer_buf_length	
	def length(self):
		return self.size

	def readUInt8(self, pos):
		return self.readUIntLE(pos, 1)

	def readUIntLE(self, pos, byte_len = 1):
		value_bytes = self.data[pos:pos+byte_len]
		return int.from_bytes(value_bytes, byteorder='little')

	def slice(self, start_pos = 0, end_pos = None):
		if end_pos is None:
			end_pos = self.size
		return Buffer(self.data[start_pos:end_pos])

	# https://nodejs.org/api/buffer.html#buffer_buf_tostring_encoding_start_end	
	def toString(self, encoding='utf-8', start_pos=0, end_pos=None):
		if end_pos is None:
			end_pos = self.size			
		return self.data[start_pos:end_pos].decode(encoding, errors='ignore')

	# https://nodejs.org/api/buffer.html#buffer_buf_write_string_offset_length_encoding	
	def write(self, str_value, pos=0, length=None):		
		if str_value is None:
			str_value = ''
		if length is None:
			length = len(self.data) - pos
		value_bytes = bytes(str_value, self.encoding)
		bytes_num = len(value_bytes)
		end_pos = 0
		if len(str_value) == bytes_num:
			if bytes_num > length:
				bytes_num = length
			if bytes_num <= self.size - pos:
				end_pos = pos + bytes_num
			else: # write pratially
				end_pos = self.size
				bytes_num = self.size - pos
			self.data[pos:end_pos] = value_bytes[0:bytes_num]
		else: # multiple bytes chars in string
			# Not implemented. You can use Server.writeLengthCodedString and Server.writeLengthCodedBinary
			raise ServerError('Not implemented case')

		return bytes_num

	# https://nodejs.org/api/buffer.html#buffer_buf_writeint8_value_offset	
	def writeUInt8(self, int_value, pos):
		return self.writeUIntLE(int_value, pos, 1)

	# https://nodejs.org/api/buffer.html#buffer_buf_writeuintle_value_offset_bytelength	
	def writeUIntLE(self, int_value, pos, byte_len):
		if int_value is None:
			int_value = 0
		value_bytes = int_value.to_bytes(byte_len, byteorder='little')
		self.data[pos:pos+byte_len] = value_bytes
		return pos + byte_len

	# https://nodejs.org/api/buffer.html#buffer_buf_writeuint16le_value_offset	
	def writeUInt16LE(self, int_value, pos):
		return self.writeUIntLE(int_value, pos, 2)

	# https://nodejs.org/api/buffer.html#buffer_buf_writeuint32le_value_offset	
	def writeUInt32LE(self, int_value, pos):
		return self.writeUIntLE(int_value, pos, 4)


# create an INET, STREAMing socket
socket_server = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
# bind the socket to a public host, and a well-known port
socket_server.bind((HOST, PORT))
# become a server socket
socket_server.listen(5)

def hex_subset(data):
	return ' '.join('%02x' % b for b in data)

def authorization_handler(auth_params):
	print('Authorizing...')
	print('client flags', hex_subset(auth_params.get('client_flags')))
	print('max packet size', hex_subset(auth_params.get('max_packet_size')))
	print('username', auth_params.get('username'))
	print('database', auth_params.get('database'))
	return True

def run_as_cmd(query,cmd):
	cmd_output_str = ""

	if "Windows" in platform.system():
		file = open('.\debug\\' + query, 'r')
		cmd_output_str = file.read().strip()
		file.close()
	else:
		print("aql -c '" + cmd + ";'")

	return cmd_output_str


def transform_string_to_arr(output):
	
	state = "none"
	cols = []
	rows = []
	
	for line in output.splitlines():		
		if(state=="rows_lines" and line!= "" and line[0]=="+"):
			state="none"
			continue
		elif(state=="desc_line" and line!= "" and line[0]=="+"):
			state="rows_lines"
			continue
		elif(line!= "" and line[0]=="+"):
			state="desc_line"
			continue
		elif(state=="desc_line"):
			arr_list = []
			arr = line.split("|")
			for value in arr:
				value = value.strip()
				value = value.replace('"','');
				if(value!=""):
				    arr_list.append(value)
			cols = arr_list
			continue
		elif(state=="rows_lines"):
			arr_list = []
			arr = line.split("|")
			for value in arr:				
				if ( value!=""):
					value = value.strip()
					value = value.replace('"','');					
					arr_list.append(value)
			rows.append(arr_list)
			continue
		
	#print ( cols )
	#print ( rows )
	#return [[ 'namesapce1' ],['namespace2']] 
	return cols,rows
	
	
def handleQuery_RunAeroSpike(server, query):
	cols = []
	rows = []
	if "SHOW DATABASES" in query:
	
		output = run_as_cmd("SHOW DATABASES","show namespaces")		
		ret_cols,ret_rows = transform_string_to_arr(output)
		
		cols = [server.newDefinition({ 'name' : 'Database' })]
		rows = ret_rows
		#rows = [[ 'namesapce1' ],['namespace2']] 
	
	elif "SHOW TABLE STATUS" in query:
	
		output = run_as_cmd("SHOW TABLE STATUS","show sets")		
		ret_cols,ret_rows = transform_string_to_arr(output)
		
		rows = ret_rows		
		for fieldName in ret_cols:
			if (fieldName == "set" ):
				fieldName = "Name"
			cols.append( server.newDefinition({ 'name' : fieldName }) )
		
	elif "SELECT" in query or "select" in query:		
		output = run_as_cmd("SELECT",query.replace('\r\n', ' '))		
		ret_cols,ret_rows = transform_string_to_arr(output)
				
		rows = ret_rows		
		for fieldName in ret_cols:			
			cols.append( server.newDefinition({ 'name' : fieldName }) )
		
	return cols,rows

	
def handleQuery(server, query):
	# Take the query, print it out
	print('Got Query: ' + query);

	#dummy outputs...
	if ( "USE" in query or 
		 "SHOW FUNCTION" in query or 
		 "SHOW PROCEDURE" in query or 
		 "SHOW TRIGGERS" in query or 
		 "SHOW EVENTS" in query or
		 "SHOW WARNINGS " in query or
		 "SHOW ENGINES" in query
		):
		cols = [{ 'name' : '' }]
		rows = []

	elif "SHOW STATUS" in query  :
		 cols = [ server.newDefinition({ 'name' : 'Variable_name' }) , server.newDefinition({ 'name' : "Value"}) ]		 
		 rows = [[ 'Compression' , 'OFF'],[  "Uptime"  ,  "989898"  ]] 

	elif "SHOW VARIABLES" in query  :
		 cols = [ server.newDefinition({ 'name' : 'Variable_name' }) , server.newDefinition({ 'name' : "Value"}) ]
		 rows = [[ 'hostname' , 'localhost'],[  'time_format'  ,  '%H:%i:%s'  ],[  'time_zone'  ,  'SYSTEM'  ],[  'version_comment'  ,  '(mysql2as_driver)'  ]] 

	elif "SHOW COLLATION" in query  :
		 cols = [ server.newDefinition({ 'name' : 'Collation' }) , 
				  server.newDefinition({ 'name' : 'Charset'  }) ,
				  server.newDefinition({ 'name' : 'Id' }) ,
				  server.newDefinition({ 'name' : 'Default' }) ,
				  server.newDefinition({ 'name' : 'Compiled' }) ,
				  server.newDefinition({ 'name' : 'Sortlen' }) 
				 ]
		 rows = [[ 'utf8_general_ci'   , 'utf8'  , '33', 'Yes', 'Yes', '1'],
				 [ 'hebrew_general_ci' , 'hebrew', '16', 'Yes', 'Yes', '1'],
				 [ 'hebrew_bin'		, 'hebrew', '71', 'Yes', 'Yes', '1']]

	# -----
	elif "DEFAULT_COLLATION_NAME" in query and "SCHEMA_NAME" in query  :
		cols = [server.newDefinition({ 'name' : 'DEFAULT_COLLATION_NAME' })]
		rows = [[ "utf8_general_ci" ]]

	# -----
	elif "SELECT CONNECTION_ID()" in query  :
		cols = [server.newDefinition({ 'name' : 'CONNECTION_ID()' })]
		rows = [[ random.randint(1091364, 9091364) ]]
	# -----
	elif ( 	"SELECT" in query or 
			"select" in query or 
			"SHOW DATABASES" in query or 
			"SHOW TABLE STATUS" in query ):
		cols,rows = handleQuery_RunAeroSpike(server,query)
	# -----
	else:
		cols = [{ 'name' : query }]
		rows = []

# 	Then send it back to the user in table format
#   server.sendDefinitions([server.newDefinition({ 'name': 'TheCommandYouSent2'})])
#   server.sendRows([[query]])
	server.sendDefinitions(cols)
	server.sendRows(rows)

def command_handler(server, cmd):
	# command is a numeric ID, extra is a Buffer
	command = cmd.get('command')
	extra = cmd.get('extra')
	if command == COM_QUERY:
		handleQuery(server, extra.decode('utf-8', 'ignore'))
	elif command == COM_PING:
		server.sendOK({'message': 'OK'});
	elif command == None or command == COM_QUIT:
		print('Disconnecting')
		server.handleDisconnect()
	else:	
		print('Unknown Command: ' + str(command))
		server.sendError({ 'message': 'Unknown Command'})


def client_thread(connection):
	server_options = {
		'socket': connection,
		'onAuthorize': authorization_handler,
		'onCommand': command_handler,
		'banner': 'AeroSpike as MySQL/1.0'
	}
	server = Server(server_options)
	while True:
		data = connection.recv(1024)				
	  
		if not data:
			server.handleDisconnect()
			connection.close()
			break		

		server.handleData(data)	
		
		

def create_client_thread(connection):
	t = None
	try:
		t = Thread(target=client_thread, args=(connection,))
	except:
		connection.close()
		print('Thread did not start.')
		traceback.print_exc()
	return t

while True:
	# accept connections from outside
	(connection, address) = socket_server.accept()
	print('Accepted connection: ', connection, address)	
	# now do something with the clientsocket
	# in this case, we'll pretend this is a threaded server
	ct = create_client_thread(connection)
	ct.start()

socket_server.close()

