#!/usr/bin/env python
'''A module which implements the MySQL server protocol, giving you the ability to create MySQL-like services

run in the background :
nohup python36 ./as_mysql.py  >/dev/null 2>&1 &


'''
import re
import os
import socket
import random
import string
import platform
import datetime
import traceback
import subprocess
from threading import Thread

print('Start server...')

AS_HOST, HOST, PORT = os.environ.get('AS_HOST', '127.0.0.1'), '', 3306  # default MySQL Server Port

__author__ = 'Zac Sadan, Dmitri Krasnenko'
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
COM_INIT_DB = 2
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

AEROSPIKE_QUERY_TIMEOUT = 10000


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
        self.onPacket = self.hello_packet_handler
        self.incoming = []
        self.packetCount = 0
        self.shutdown_flag = socket.SHUT_RDWR

        self.send_server_hello()

    # external handler
    def handle_disconnect(self):
        print('disconnect')

    def write_header(self, data, length):
        data.writeUIntLE(length - 4, 0, 3)
        data.writeUInt8(self.sequence % 256, 3)
        self.sequence += 1

    def send_packet(self, payload):
        return self.socket.send(payload.data)

    def send_results(self, col, rows):
        if not col:
            self.send_ok()
        else:
            self.send_definitions(col)
            self.send_rows(rows)

    @staticmethod
    def new_definition(params):
        return {
            'catalog': params.get('catalog', 'def'),
            'schema': params.get('db'),
            'table': params.get('table'),
            'orgTable': params.get('orgTable'),
            'name': params.get('name'),
            'orgName': params.get('orgName'),
            'length': params.get('length', 0),
            'type': params.get('type', MYSQL_TYPE_VAR_STRING),
            'flags': params.get('flags', 0),
            'decimals': params.get('decimals'),
            'default': params.get('default')
        }

    def send_definition(self, definition):
        pos = 4
        payload = Buffer(1024)

        for field in ['catalog', 'schema', 'table', 'orgTable', 'name', 'orgName']:
            pos = self.write_length_coded_string(
                payload, pos, definition.get(field, '')
            )

        pos = payload.writeUInt8(0x0C, pos)
        pos = payload.writeUInt16LE(11, pos)  # ASCII
        pos = payload.writeUInt32LE(definition.get('length'), pos)
        pos = payload.writeUInt8(definition.get('type'), pos)
        pos = payload.writeUInt16LE(definition.get('flags', 0), pos)
        pos = payload.writeUInt8(definition.get('decimals', 0), pos)
        pos = payload.writeUInt16LE(0, pos)  # \0\0 FILLER

        # Deprecated by MySQL
        # pos = self.write_length_coded_string(payload, pos, definition.get('default'))

        self.write_header(payload, pos)
        self.send_packet(payload.slice(0, pos))

    def send_definitions(self, definitions):
        pos = 4
        payload = Buffer(1024)

        # Write definition header
        pos = self.write_length_coded_binary(
            payload, pos, len(definitions)
        )

        self.write_header(
            payload, pos
        )

        self.send_packet(
            payload.slice(0, pos)
        )

        # Write each definition
        for definition in definitions:
            self.send_definition(
                definition
            )

        # Send EOF
        self.send_eof()

    def send_row(self, row):
        payload = Buffer(1024)
        pos = 4

        for cell in row:
            if cell is None:
                pos = payload.writeUInt8(0xFB, pos)
            else:
                pos = self.write_length_coded_string(payload, pos, cell)

        self.write_header(payload, pos)
        self.send_packet(payload.slice(0, pos))

    def send_rows(self, rows=None):
        if rows:
            for row in rows:
                self.send_row(row)

        self.send_eof()

    def send_eof(self, args=None):
        # Write EOF
        pos = 4
        payload = Buffer(16)

        def_args = {
            'warningCount': 0,
            'serverStatus': SERVER_STATUS_AUTOCOMMIT
        }

        args = self.get_args_with_defaults(args, def_args)

        pos = payload.writeUInt8(0xFE, pos)
        pos = payload.writeUInt16LE(args.get('warningCount'), pos)
        pos = payload.writeUInt16LE(args.get('serverStatus'), pos)

        self.write_header(payload, pos)
        self.send_packet(payload.slice(0, pos))

    def send_server_hello(self):
        pos = 4
        payload = Buffer(128)

        # Protocol version
        pos = payload.writeUInt8(10, pos)
        pos += payload.write(getattr(self, 'banner', 'MyServer/1.0'), pos)
        pos = payload.writeUInt8(0, pos)

        pos = payload.writeUInt32LE(os.getpid(), pos)

        pos += Buffer.copy_to_buffer(self.salt, payload, pos, 0, 8)

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

        pos += Buffer.copy_to_buffer(self.salt, payload, pos, 8)
        pos = payload.writeUInt8(0, pos)
        self.write_header(payload, pos)
        return self.send_packet(payload.slice(0, pos))

    # external handler
    def handle_data(self, data):
        if data is not None and len(data) > 0:
            self.incoming.append(data)
        self.gather_incoming('from handleData')
        if data is None:
            print('Connection closed')
            # https://docs.python.org/3.5/library/socket.html#socket.socket.close
            self.socket.shutdown(self.shutdown_flag)
            self.socket.close()

    def gather_incoming(self, wherewas):
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
                    length += Buffer.copy_to_buffer(buf, incoming, length)
            remaining = self.read_packets(incoming)
            self.incoming = [remaining.data]

    def read_packets(self, buf):
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
    def hello_packet_handler(self, packet):
        ## Reading Client Hello...
        # http://dev.mysql.com/doc/internals/en/the-packet-header.html

        if packet.length() == 0:
            return self.send_error({'message': 'Zero length hello packet'})

        ptr = 0

        clientFlags = packet.slice(ptr, ptr + 4)
        ptr += 4

        maxPacketSize = packet.slice(ptr, ptr + 4)
        ptr += 4

        self.clientCharset = packet.readUInt8(ptr)
        ptr += 1

        some_filler = packet.slice(ptr, ptr + 23)
        ptr += 23

        usernameEnd = packet.index_of(0, ptr)
        username = packet.toString('ascii', ptr, usernameEnd)
        ptr = usernameEnd + 1

        scrambleLength = packet.readUInt8(ptr)
        ptr += 1

        if scrambleLength > 0:
            self.scramble = packet.slice(ptr, ptr + scrambleLength).data
            ptr += scrambleLength

        database = None
        databaseEnd = packet.index_of(0, ptr)
        if databaseEnd >= 0:
            database = packet.toString('ascii', ptr, databaseEnd)

        self.onPacket = None

        try:
            authorized = self.onAuthorize(
                {
                    'username': username,
                    'database': database,
                    'client_flags': clientFlags.data,
                    'max_packet_size': maxPacketSize.data,
                }
            )

            if not authorized:
                raise ServerError('Not Authorized')

            self.onPacket = self.normal_packet_handler
            self.send_ok()
        except:
            self.send_error({'message': 'Authorization Failure'})
            self.socket.shutdown(self.shutdown_flag)
            self.socket.close()

    # handler for self.onPacket
    def normal_packet_handler(self, packet):
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

    def send_ok(self, args=None):
        def_args = {'message': '', 'insertId': None, 'affectedRows': 0, 'warningCount': 0}
        args = self.get_args_with_defaults(
            args,
            def_args
        )

        data = Buffer(len(args.get('message')) + 64)
        pos = 4
        pos = data.writeUInt8(0, pos)
        pos = self.write_length_coded_binary(data, pos, args.get('affectedRows'))
        pos = self.write_length_coded_binary(data, pos, args.get('insertId'))
        pos = data.writeUInt16LE(SERVER_STATUS_AUTOCOMMIT, pos)
        pos = data.writeUInt16LE(args.get('warningCount', 0), pos)
        pos = self.write_length_coded_string(data, pos, args.get('message'))

        self.write_header(data, pos)
        self.send_packet(data.slice(0, pos))

    def send_error(self, args=None):
        def_args = {'message': 'Unknown error', 'errno': 2000, 'sqlState': 'HY000'}
        args = self.get_args_with_defaults(
            args, def_args,
        )

        pos = 4
        data = Buffer(len(args.get('message')) + 64)

        pos = data.writeUInt8(0xFF, pos)
        pos = data.writeUInt16LE(args.get('errno'), pos)
        pos += data.write('#', pos)
        pos += data.write(args.get('sqlState'), pos, 5)
        pos += data.write(args.get('message'), pos)
        pos = data.writeUInt8(0, pos)

        self.write_header(data, pos)
        self.send_packet(data.slice(0, pos))

    @staticmethod
    def write_length_coded_string(buf, pos, strval):
        if strval is None:
            return buf.writeUInt8(0, pos)

        if type(strval) != str:
            strval = str(strval)

        buf.writeUInt8(253, pos)
        buf.writeUIntLE(len(strval), pos + 1, 3)
        buf.write(strval, pos + 4)
        return pos + len(strval) + 4

    @staticmethod
    def write_length_coded_binary(buf, pos, int_value):
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
    def copy(self, target_buf, target_start_pos=0, source_start_pos=0, source_end_pos=None):
        return self.copy_to_buffer(
            self.data,
            target_buf,
            target_start_pos,
            source_start_pos, source_end_pos
        )

    @staticmethod
    def copy_to_buffer(source_data, target_buf, target_start_pos=0, source_start_pos=0, source_end_pos=None):
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
    def fill(self, value, start_pos=0, end_pos=None):
        if end_pos is None:
            end_pos = self.size
        bytes_number = end_pos - start_pos
        self.data[start_pos:end_pos] = bytes([value for i in range(end_pos - start_pos)])

    # https://nodejs.org/api/buffer.html#buffer_buf_indexof_value_byteoffset_encoding
    def index_of(self, value, pos):
        return self.data.find(value, pos)

    # https://nodejs.org/api/buffer.html#buffer_buf_length
    def length(self):
        return self.size

    def readUInt8(self, pos):
        return self.readUIntLE(pos, 1)

    def readUIntLE(self, pos, byte_len = 1):
        value_bytes = self.data[pos:pos+byte_len]
        return int.from_bytes(value_bytes, byteorder='little')

    def slice(self, start_pos=0, end_pos=None):
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


def authorization_handler(auth_params):
    print('Authorizing...')
    print('client flags', hex_subset(auth_params.get('client_flags')))
    print('max packet size', hex_subset(auth_params.get('max_packet_size')))
    print('username', auth_params.get('username'))
    print('database', auth_params.get('database'))
    return True


def run_as_cmd(query, cmd):
    if "Windows" in platform.system():
        with open('.\\debug\\' + query, 'r') as file:
            cmd_output_str = file.read()

        return transform_string_to_arr(
            cmd_output_str.strip()
        )
    else:
        return transform_aql_to_arr(
            run_aql(AS_HOST, cmd)
        )


def run_aql(host, cmd):
    print("Execs: aql -h {} -c '{}'".format(host, cmd))

    proc = subprocess.Popen(['aql', '-h', host, '-c', 'set timeout {}; {}'.format(AEROSPIKE_QUERY_TIMEOUT, cmd)], stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    stds = proc.communicate()
    resc = proc.wait()

    stderr = stds[1]
    stdout = stds[0]

    if not resc and not stderr:
        return stdout.splitlines()
    else:
        raise Exception(stderr.decode("utf-8"))


def transform_aql_to_arr(output):

    def is_all_columns(cl, rw):
        for ind in range(0, len(cl)):
            if cl[ind] != rw[ind]:
                return False
        return True

    if output and len(output) >= 5:
        rows = []
        cols = [col.strip().replace('"', '') for col in output[4].decode("utf-8").split('|') if col.strip()]

        for row_str in output[6:-6]:
            dec_row_str = row_str.decode(
                "utf-8"
            )

            # Ignore AQL header line
            if dec_row_str[0] == '+':
                continue

            i = 0
            j = len(dec_row_str)
            if dec_row_str[0] == '|':
                i = 1

            if dec_row_str[-1] == '|':
                j = -1

            row = [cell.strip().replace('"', '') for cell in dec_row_str[i:j].split('|')]

            if not is_all_columns(cols, row):
                rows.append(
                    row
                )

        return cols, rows
    else:
        return [], []


def transform_string_to_arr(output):

    state = "none"
    cols = []
    rows = []

    for line in output.splitlines():
        if state == "rows_lines" and line != "" and line[0] == "+":
            state="none"
            continue
        elif state == "desc_line" and line != "" and line[0] == "+":
            state = "rows_lines"
            continue
        elif line != "" and line[0] == "+":
            state = "desc_line"
            continue
        elif state == "desc_line":
            arr_list = []
            arr = line.split("|")
            for value in arr:
                value = value.strip()
                value = value.replace('"','');
                if value != "":
                    arr_list.append(value)
            cols = arr_list
            continue
        elif state == "rows_lines":
            arr_list = []
            arr = line.split("|")
            for value in arr:
                if value != "":
                    value = value.strip()
                    value = value.replace('"', '')
                    arr_list.append(value)
            rows.append(arr_list)
            continue

    return cols, rows


def handle_query_run_aerospike(server, query):
    # cols = None
    # rows = None
    if re.match("show databases", query, re.IGNORECASE):

        _, rows = run_as_cmd(
            "show databases", "show namespaces"
        )

        cols = [server.new_definition({'name': 'Database'})]

    elif re.match("show tables", query, re.IGNORECASE):

        ret_cols, ret_rows = run_as_cmd(
            "show tables", "show sets"
        )

        cols = [
            server.new_definition({'name': "Name"})
        ]

        set_name_field_indx = ret_cols.index('set')
        for row in ret_rows:
            del row[0:set_name_field_indx]
            del row[1:]

        rows = ret_rows
    elif re.match('show table status', query, re.IGNORECASE):

        ret_cols, ret_rows = run_as_cmd(
            "show table status", "show sets"
        )

        cols = [
            server.new_definition({'name': "Name"}),
            server.new_definition({'name': "Engine"}),
            server.new_definition({'name': "Version"}),
            server.new_definition({'name': "Row_format"}),
            server.new_definition({'name': "Rows", "type": MYSQL_TYPE_DECIMAL}),
            server.new_definition({'name': "Avg_row_length", "type": MYSQL_TYPE_DECIMAL}),
            server.new_definition({'name': "Data_length", "type": MYSQL_TYPE_DECIMAL}),
            server.new_definition({'name': "Max_data_length", "type": MYSQL_TYPE_DECIMAL}),
            server.new_definition({'name': "Index_length", "type": MYSQL_TYPE_DECIMAL}),
            server.new_definition({'name': "Data_free", "type": MYSQL_TYPE_DECIMAL}),
            server.new_definition({'name': "Auto_increment", "type": MYSQL_TYPE_DECIMAL}),
            server.new_definition({'name': "Create_time", "type": MYSQL_TYPE_DECIMAL}),
            server.new_definition({'name': "Update_time", "type": MYSQL_TYPE_DECIMAL}),
            server.new_definition({'name': "Check_time", "type": MYSQL_TYPE_DECIMAL}),
            server.new_definition({'name': "Collation", "type": MYSQL_TYPE_STRING}),
            server.new_definition({'name': "Checksum", "type": MYSQL_TYPE_DECIMAL}),
            server.new_definition({'name': "Create_options", "type": MYSQL_TYPE_DECIMAL}),
            server.new_definition({'name': "Comment", "type": MYSQL_TYPE_STRING})
        ]

        set_name_field_indx = ret_cols.index('set')
        for row in ret_rows:
            del row[0:set_name_field_indx]
            del row[1:]

            row.extend(['Aerospike', '10', 'Fixed', None, None, None, None, None, None, None, None, None, None, 'utf8_general_ci', None, None, '(mysql2as_driver)'])

        rows = ret_rows
    else:
        ret_cols, ret_rows = run_as_cmd(
            ''.join(random.choices(string.ascii_uppercase + string.digits, k=7)), query.replace('\r\n', ' ')
        )

        cols = []
        rows = ret_rows
        for field_name in ret_cols:
            cols.append(server.new_definition(
                {'name': field_name}
            ))

    return cols, rows


def handle_query(server, query):
    print('Query: ' + query)

    cols = None
    rows = None

    try:

        # dummy outputs...
        if re.match(
                "use|show events|show engines|show function|show triggers|show warnings|show procedure",
                query, re.IGNORECASE):
            cols = None
            rows = None

        elif re.match('select database', query, re.IGNORECASE):
            cols = [server.new_definition({'name': 'database()'})]
            rows = [['NULL']]

        elif re.match('select now', query, re.IGNORECASE):
            cols = [server.new_definition({'name': 'now()'})]
            rows = [[datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')]]

        elif re.match("select @@version_comment", query, re.IGNORECASE):
            cols = [server.new_definition({'name': '@@version_comment'})]
            rows = [['(mysql2as_driver)']]

        elif re.match("select @@global.max_allowed_packet", query, re.IGNORECASE):
            cols = [server.new_definition({'name': '@@global.max_allowed_packet'})]
            rows = [['33554432']]

        elif re.match("show status", query, re.IGNORECASE):
            cols = [
                server.new_definition({'name': 'Variable_name'}),
                server.new_definition({'name': 'Value'})
            ]

            rows = [
                ['Compression', 'OFF'],
                ['Uptime', '989898']
            ]

        elif re.match("show variables|show session variables", query, re.IGNORECASE):
            cols = [server.new_definition({'name': 'Variable_name'}), server.new_definition({'name': "Value"})]

            if 'LIKE' not in query and 'like' not in query:
                rows = [
                    ['time_zone', 'SYSTEM'],
                    ['hostname', 'localhost'],
                    ['time_format', '%H:%i:%s'],
                    ['lower_case_table_names', '1'],
                    ['version_comment', '(mysql2as_driver)']
                ]

        elif "SHOW COLLATION" in query:
            cols = [
                server.new_definition({'name': 'Collation'}),
                server.new_definition({'name': 'Charset'}),
                server.new_definition({'name': 'Id'}),
                server.new_definition({'name': 'Default'}),
                server.new_definition({'name': 'Compiled'}),
                server.new_definition({'name': 'Sortlen'})
            ]

            rows = [
                ['utf8_general_ci',   'utf8',   '33', 'Yes', 'Yes', '1'],
                ['hebrew_general_ci', 'hebrew', '16', 'Yes', 'Yes', '1'],
                ['hebrew_bin',        'hebrew', '71', 'Yes', 'Yes', '1']
            ]

        # -----
        elif "DEFAULT_COLLATION_NAME" in query and "SCHEMA_NAME" in query:
            cols = [server.new_definition({'name': 'DEFAULT_COLLATION_NAME'})]
            rows = [["utf8_general_ci"]]

        # -----
        elif "SELECT CONNECTION_ID()" in query:
            cols = [server.new_definition({'name': 'CONNECTION_ID()'})]
            rows = [[random.randint(1091364, 9091364)]]
        # -----
        elif re.search("select|show tables|show databases|show table status|show sets", query, re.IGNORECASE):
            cols, rows = handle_query_run_aerospike(server, query)
        # -----
        else:
            cols = None
            rows = None

        # Then send it back to the user in table format
        server.send_results(cols, rows)
    except Exception as e:
        server.send_error({'message': str(e)})


def command_handler(server, cmd):
    # command is a numeric ID, extra is a Buffer
    command = cmd.get('command')
    extra = cmd.get('extra')
    if command == COM_QUERY:
        handle_query(server, extra.decode('utf-8', 'ignore'))
    elif command == COM_PING:
        server.send_ok()
    elif command is None or command == COM_QUIT:
        print('Disconnecting')
        server.handle_disconnect()
    else:
        print('Unknown Command: ' + str(command))
        server.send_error({'message': 'Unknown Command'})


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
            server.handle_disconnect()
            connection.close()
            break

        server.handle_data(data)


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

