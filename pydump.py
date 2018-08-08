import os
import math
import datetime
import struct
import zlib
import pymysql
import warnings
import argparse
import sys
import glob
import io

def str2bool(v):
    if v.lower() in ('yes', 'true', 't', 'y', '1'):
        return True
    elif v.lower() in ('no', 'false', 'f', 'n', '0'):
        return False
    else:
        raise argparse.ArgumentTypeError('Boolean value expected.')

def str_escape(str):
	temp = io.StringIO()
	for i in range(len(str)):
		c = str[i]
		if c == '\\':
			temp.write('\\\\')
		elif c == '\t':
			temp.write('\\t')
		elif c == '\n':
			temp.write('\\n')
		elif c == '\r':
			temp.write('\\r')
		else:
			temp.write(c)

	return temp.getvalue()

def str_unescape(str):
	if str == '\\0':
		return None
	elif str.startswith('\\x'):
		return bytearray.fromhex(str[2:])

	temp = io.StringIO()
	i = 0
	while i < len(str):
		c = str[i]
		if c == '\\':
			if str[i+1] == 't':
				temp.write('\t')
			elif str[i+1] == 'r':
				temp.write('\r')
			elif str[i+1] == 'n':
				temp.write('\n')
			elif str[i+1] == '\\':
				temp.write('\\')
			else:
				prnit("escape error")
				sys.exit(1)
			i = i + 2
		else:
			temp.write(c)
			i = i + 1

	return temp.getvalue()

class ZipEntry:
	def __init__(self, name, dt, offset):
		self.name = name
		self.dt = dt
		self.offset = offset
		self.compress = zlib.compressobj(zlib.Z_DEFAULT_COMPRESSION, zlib.DEFLATED, -15)
#		self.crc = 0xffffffff
		self.crc = 0
		self.compressed_size = 0
		self.original_size = 0

class ZipStream:
	def __init__(self, output):
		self.totalsize = 0
		self.output = output
		self.entries = []

	def create_entry(self, name, dt):
		entry = ZipEntry(name, dt, self.totalsize)
		self.write_entry(entry)
		self.entries.append(entry)
		return entry

	def write(self, entry, data):
		compressed = entry.compress.compress(data)
		self.output.write(compressed)
		entry.compressed_size += len(compressed)
		entry.original_size += len(data)
		entry.crc = zlib.crc32(data, entry.crc)

	def flush(self, entry):
		compressed = entry.compress.flush()
		self.output.write(compressed)
		entry.compressed_size += len(compressed)
		self.totalsize += entry.compressed_size
		self.write_ddec(entry)

	def write_struct(self, fmt, data):
		packed = struct.pack(fmt, data)
		self.output.write(packed)
		self.totalsize += len(packed)

	def write_ddec(self, entry):
		self.write_struct('<I', 0x08074b50)
		self.write_struct('<I', entry.crc)
		self.write_struct('<Q', entry.compressed_size)
		self.write_struct('<Q', entry.original_size)

	def write_entry(self, entry):
		self.write_struct('<I', 0x04034b50)
		self.write_struct('<H', 45)
		self.write_struct('<H', 8)
		self.write_struct('<H', 8)
		self.write_struct('<H', math.floor(entry.dt.second / 2 + entry.dt.minute * 32 + entry.dt.hour * 2048))
		self.write_struct('<H', entry.dt.day + entry.dt.month * 32 + (entry.dt.year - 1980) * 512)
		self.write_struct('<I', 0) # CRC. It's not determined yet. Use data descriptor.
		self.write_struct('<I', 0xFFFFFFFF) # compressed size. Use ZIP64
		self.write_struct('<I', 0xFFFFFFFF) # uncompressed size. Use ZIP64
		self.write_struct('<H', len(entry.name))
		self.write_struct('<H', 16 + 4)
		self.write_struct(str(len(entry.name)) + 's', entry.name.encode('utf-8')) # uncompressed size
		self.write_struct('<H', 1)
		self.write_struct('<H', 16)
		self.write_struct('<Q', 0) # uncompressed size. It's not determined yet. Use data descriptor.
		self.write_struct('<Q', 0) # compressed size. It's not determined yet. Use data descriptor.

	def write_central_header(self, entry):
		self.write_struct('<I', 0x02014b50)
		self.write_struct('<B', 45)
		self.write_struct('<B', 3)
		self.write_struct('<H', 45)
		self.write_struct('<H', 8)
		self.write_struct('<H', 8)
		self.write_struct('<H', math.floor(entry.dt.second / 2 + entry.dt.minute * 32 + entry.dt.hour * 2048))
		self.write_struct('<H', entry.dt.day + entry.dt.month * 32 + (entry.dt.year - 1980) * 512)
		self.write_struct('<I', entry.crc) # CRC
		self.write_struct('<I', 0xFFFFFFFF) # compressed size 
		self.write_struct('<I', 0xFFFFFFFF) # uncompressed size
		self.write_struct('<H', len(entry.name))
		self.write_struct('<H', 8 * 3 + 4) # extra field size
		self.write_struct('<H', 0) # comment size
		self.write_struct('<H', 0) # disk number start
		self.write_struct('<H', 0) # internal file attributes
		self.write_struct('<I', 0) # external file attributes
		self.write_struct('<I', 0xFFFFFFFF) # offset
		self.write_struct(str(len(entry.name)) + 's', entry.name.encode('utf-8')) # uncompressed size
		self.write_struct('<H', 1)
		self.write_struct('<H', 8 * 3)
		self.write_struct('<Q', entry.original_size)
		self.write_struct('<Q', entry.compressed_size)
		self.write_struct('<Q', entry.offset)

	def write_end64(self):
		self.end64_offset = self.totalsize
		self.write_struct('<I', 0x06064b50)
		self.write_struct('<Q', 0x38 - 12)
		self.write_struct('<H', 45)
		self.write_struct('<H', 45)
		self.write_struct('<I', 0) # number of disks
		self.write_struct('<I', 0) # number of disks
		self.write_struct('<Q', len(self.entries))
		self.write_struct('<Q', len(self.entries))
		self.write_struct('<Q', self.size_central_header)
		self.write_struct('<Q', self.central_header_offset)

	def write_end64_locator(self):
		self.write_struct('<I', 0x07064b50)
		self.write_struct('<I', 0) # number of disks
		self.write_struct('<Q', self.end64_offset)
		self.write_struct('<I', 0) # number of disks

	def write_end(self):
		self.write_struct('<I', 0x06054b50)
		self.write_struct('<H', 0) # number of disks
		self.write_struct('<H', 0) # number of disks
		self.write_struct('<H', len(self.entries))
		self.write_struct('<H', len(self.entries))
		self.write_struct('<I', self.size_central_header)
		self.write_struct('<I', 0xFFFFFFFF) # central header offset. Use Zip64 locator.
		self.write_struct('<H', 0) # comment length

	def close(self):
		self.central_header_offset = self.totalsize
		for entry in self.entries:
			self.write_central_header(entry)

		self.size_central_header = self.totalsize - self.central_header_offset

		self.write_end64()
		self.write_end64_locator()
		self.write_end()
		self.output.close()

class LocalFileOutput():
	def __init__(self):
		pass

	def mkdir(self, dir):
		os.mkdir(dir)

	def create_file(self, name):
		return open(name, mode='wb')

	def write(self, file):
		file.write(data)

	def close_file(self, file):
		file.close()

	def close(self):
		pass

class LocalFileInput():
	def __init__(self):
		pass

	def listdir(self, path):
		files = glob.glob(path)
		return [os.path.splitext(os.path.basename(file))[0] for file in files]

	def open_file(self, name, mode, encoding):
		return open(name, mode=mode, encoding=encoding)

	def readline(self, file):
		return file.readline()

	def readall(self, file):
		return file.read()

	def close_file(self,file):
		file.close()

class ZipOutput():
	def __init__(self, path, output):
		self.output = output
		self.zipStream = ZipStream(self.output.create_file(path))

	def create_database(self, database):
		pass

	def create_file(self, database, file_name):
		return self.zipStream.create_entry(database + '/' + file_name, datetime.datetime.now())

	def write(self, file, data):
		self.zipStream.write(file, data.encode('utf-8'))

	def close_file(self, file):
		self.zipStream.flush(file)

	def close(self):
		self.zipStream.close()

class FlatOutput():
	def __init__(self, path, output):
		self.output = output
		self.output.mkdir(path)
		self.root = path

	def create_database(self, database):
		self.output.mkdir(self.root + '/' + database)

	def create_file(self, database, file_name):
		return self.output.create_file(self.root + '/' + database + '/' + file_name)

	def write(self, file, data):
		file.write(data.encode('utf-8'))

	def close_file(self, file):
		file.close()

	def close(self):
		pass

class FlatInput():
	def __init__(self, path, input):
		self.input = input
		self.root = path

	def show_databases(self):
		return self.input.listdir(self.root + '/*')

	def show_tables(self, database):
		return self.input.listdir(self.root + '/' + database + '/*.def')

	def open_file(self, database, file_name):
		return self.input.open_file(self.root + '/' + database + '/' + file_name, mode='r', encoding='utf-8')

	def readall(self, file):
		return self.input.readall(file)

	def readline(self, file):
		return self.input.readline(file)

	def close_file(self, file):
		self.input.close_file(file)

	def close(self):
		pass

class PyDump():
	def write_column(self, entry, column, description, field):
		if column is None:
			self.output.write(entry, '\\0')
		elif description[1] == pymysql.constants.FIELD_TYPE.VAR_STRING:
			if field.charsetnr == 63:
				self.output.write(entry, '\\x' + column.hex())
			else:
				self.output.write(entry, str_escape(column))
		elif description[1] == pymysql.constants.FIELD_TYPE.STRING:
			self.output.write(entry, str_escape(column))
		elif description[1] == pymysql.constants.FIELD_TYPE.TIMESTAMP:
			if type(column) is datetime.datetime:
				self.output.write(entry, column.strftime('%Y-%m-%d %H:%M:%S.%f'))
			else:
				# for 0000-00-00 00:00:00 case
				self.output.write(entry, column)
		elif description[1] == pymysql.constants.FIELD_TYPE.DATETIME:
			if type(column) is datetime.datetime:
				self.output.write(entry, column.strftime('%Y-%m-%d %H:%M:%S.%f'))
			else:
				# for 0000-00-00 00:00:00 case
				self.output.write(entry, column)
		elif description[1] == pymysql.constants.FIELD_TYPE.DATE:
			self.output.write(entry, column.strftime('%Y-%m-%d'))
		elif description[1] == pymysql.constants.FIELD_TYPE.TIME:
			self.output.write(entry, str(column))
		elif description[1] == pymysql.constants.FIELD_TYPE.FLOAT:
			self.output.write(entry, str(column))
		elif description[1] == pymysql.constants.FIELD_TYPE.DOUBLE:
			self.output.write(entry, str(column))
		elif description[1] == pymysql.constants.FIELD_TYPE.BLOB:
			if field.charsetnr == 63:
				self.output.write(entry, '\\x' + column.hex())
			else:
				self.output.write(entry, str_escape(column))
		elif description[1] == pymysql.constants.FIELD_TYPE.GEOMETRY:
			self.output.write(entry, '\\x' + column.hex())
		elif type(column) is int:
			self.output.write(entry, str(column))
		else:
			print("unsupported column type error")
			print(column)
			print(description)
			sys.exit(1)

	def dump_table(self, database_name ,table_name):
		print('dumping table: ' + table_name, end='')
		sys.stdout.flush()
		entry = self.output.create_file(database_name, table_name + '.def')
		cur = self.con.cursor()
		cur.execute('use ' + database_name)
		cur.execute('show create table ' + table_name)
		rows = cur.fetchall()
		cur.close
		for row in rows:
			self.output.write(entry, row[1])
			self.output.close_file(entry)

		entry = self.output.create_file(database_name, table_name + '.dat')
		cur = self.con.cursor()
		cur.execute('use ' + database_name)
		cur.execute('select * from ' +  table_name)
		row_count = 0
		row = cur.fetchone()
		while row is not None:
			for i in range(len(row)):
				if i != 0:
					self.output.write(entry, '\t')

				self.write_column(entry, row[i], cur.description[i], cur._result.fields[i])
			self.output.write(entry, '\n')
			row_count += 1
			if row_count % 10000 == 0:
				print(' ', row_count, end='')
				sys.stdout.flush()
			row = cur.fetchone()

		print('')
		sys.stdout.flush()
		self.output.close_file(entry)

	def dump_database(self, database_name):

		self.output.create_database(database_name)

		cur = self.con.cursor()
		cur.execute('SELECT TABLE_NAME FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_SCHEMA=%s AND TABLE_TYPE=%s',(database_name,'BASE TABLE'))

		rows = cur.fetchall()
		cur.close
		for row in rows:
			self.dump_table(database_name, row[0])

	def dump_all_databases(self):
		self.create_output(self.args.dump)
		cur = self.con.cursor()

		cur.execute('SHOW DATABASES')
		rows = cur.fetchall()
		cur.close()

		for row in rows:
			if row[0] == 'information_schema':
				continue

			if row[0] == 'performance_schema':
				continue

			if row[0] == 'mysql' and self.args.user_databases:
				continue

			if row[0] == 'sys' and self.args.user_databases:
				continue

			self.dump_database(row[0])

		self.output.close()

	def dump_databases(self, databases):
		self.create_output(self.args.dump)
		for database in databases:
			self.dump_database(database)

		self.output.close()

	def restore_table(self, database, table):
		print('restoreing table: ' + table)
		sys.stdout.flush()
		def_file = self.input.open_file(database, table + '.def')
		sql = self.input.readall(def_file)
		sql = "CREATE TABLE IF NOT EXISTS" + sql[12:]
		cur = self.con.cursor()
		cur.execute('USE ' + database)
		if self.args.drop_table:
			cur.execute('DROP TABLE IF EXISTS ' + table)
		try:
			cur.execute(sql)
		except pymysql.err.ProgrammingError as e:
			pass

		self.input.close_file(def_file)
		data_file = self.input.open_file(database, table + '.dat')
		field_count = 0

		line = data_file.readline().rstrip('\n')
		columns = line.split('\t')
		temp = '%s,' * len(columns)
		sql = 'INSERT INTO ' + table + ' values(' + temp[:-1] + ')'
		count = 0

		while line:
			data = []
			for column in columns:
				data.append(str_unescape(column))

			try:
				cur.execute(sql, tuple(data))
			except Exception as e:
				print('ERROR ' + e.args[1])
				sys.exit(1)

			count = count + 1
			if count >= self.args.commit:
				self.con.commit()
			line = data_file.readline().rstrip('\n')
			columns = line.split('\t')

		self.con.commit()
		self.input.close_file(data_file)

	def restore_database(self, database):
		cur = self.con.cursor()
		if self.args.drop_database:
			cur.execute('DROP DATABASE IF EXISTS ' + database)

		with warnings.catch_warnings():
			warnings.simplefilter('ignore')
			try:
				cur.execute('CREATE DATABASE IF NOT EXISTS ' + database)
			except Exception:
				raise

		tables = self.input.show_tables(database)

		for table in tables:
			self.restore_table(database, table)

	def restore_all_databases(self):
		self.open_input(self.args.restore)
		databases = self.input.show_databases()
		for database in databases:
			self.restore_database(database)

		self.input.close()

	def restore_databases(self, databases):
		self.open_input(self.args.restore)
		for database in databases:
			self.restore_database(database)

		self.input.close()

	def dump(self):
		error = False

		if self.args.filter is None:
			print('filter parameter is required.')
			error = True

		if self.args.transport is None:
			print('transport parameter is required.')
			error = True

		if error:
			return

		if self.args.single_transaction:
			cur = self.con.cursor()
			cur.execute('SET SESSION TRANSACTION ISOLATION LEVEL REPEATABLE READ')
			cur.execute('START TRANSACTION WITH CONSISTENT SNAPSHOT')

		if self.args.all_databases or self.args.user_databases:
			self.dump_all_databases()
		else:
			self.dump_databases(self.args.databases)

	def restore(self):
		error = False

		if self.args.filter is None:
			print('filter parameter is required.')
			error = True

		if self.args.transport is None:
			print('transport parameter is required.')
			error = True

		if error:
			return

		cur = self.con.cursor()
		cur.execute('SET FOREIGN_KEY_CHECKS=0')

		if self.args.all_databases:
			self.restore_all_databases()
		else:
			self.restore_databases(self.args.databases)

	def create_output(self, root):
		output = None
		try:
			if self.args.transport == 'local':
				output = LocalFileOutput()

			if self.args.filter == 'flat':
				self.output = FlatOutput(root, output)
			elif self.args.filter == 'zip':
				self.output = ZipOutput(root + '.zip', output)
		except FileExistsError as e:
			print('ERROR ' + e.args[1])
			sys.exit(1)

	def open_input(self, root):
		input = None
		if self.args.transport == 'local':
			input = LocalFileInput()

		if self.args.filter == 'flat':
			self.input = FlatInput(root, input)

	def start(self):
		parser = argparse.ArgumentParser(
			prog='pydump.py',
			usage='',
			description='',
			epilog='',
			add_help=False
			)

		parser.add_argument('-u', '--user')
		parser.add_argument('-p', '--password')
		parser.add_argument('-P', '--port', type=int, default=3306)
		parser.add_argument('-h', '--host')
		parser.add_argument('-t', '--transport')
		parser.add_argument('-f', '--filter')
		parser.add_argument('-c', '--commit', type=int, default=10000)
		parser.add_argument('--databases', nargs='*')
		parser.add_argument('--all-databases', action='store_true', default=False)
		parser.add_argument('--user-databases', action='store_true', default=False)
		parser.add_argument('--single-transaction', action='store_true', default=False)
		parser.add_argument('--drop-database', type=str2bool)
		parser.add_argument('--drop-table', type=str2bool)
		parser.add_argument('-d', '--dump')
		parser.add_argument('-r', '--restore')
		parser.add_argument('-?', '--help', action='help')

		if len(sys.argv) == 1:
			print(parser.format_help())
			sys.exit(0)

		self.args = parser.parse_args()

		self.con = pymysql.connect(user=self.args.user, password=self.args.password, host=self.args.host, port=self.args.port, cursorclass=pymysql.cursors.SSCursor, binary_prefix=True)

		if self.args.dump:
			self.dump()
		elif self.args.restore:
			self.restore()
		else:
			print(parser.format_help())
			sys.exit(0)

pydump = PyDump()
pydump.start()
