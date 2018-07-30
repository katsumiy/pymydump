import pymysql
import warnings
import argparse
import sys
import os
import glob
import io
from datetime import datetime
from os.path import basename

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

class LocalOutput():
	def __init__(self, path):
		os.mkdir(path)
		self.root = path

	def create_database(self, database):
		os.mkdir(self.root + '/' + database)

	def create_file(self, database, file_name):
		return open(self.root + '/' + database + '/' + file_name, 'w', encoding='utf-8')

	def write(self, file, data):
		file.write(data)

	def close_file(self, file):
		file.close()

	def close(self):
		pass

class LocalInput():
	def __init__(self, path):
		self.root = path

	def read_databases(self):
		files = glob.glob(self.root + '/*')
		databases = [basename(file) for file in files]
		return databases

	def read_tables(self, database):
		files = glob.glob(self.root + '/' + database + '/*.def')
		tables = [basename(file)[:-4] for file in files]
		return tables

	def open_file(self, database, file_name):
		return open(self.root + '/' + database + '/' + file_name, 'r', encoding='utf-8')

	def readline(self, file):
		file.readline()

	def close_file(self, file):
		file.close()

	def close(self):
		pass

class PyDump():
	def write_column(self, entry, column, description, field):
		if column is None:
			self.output.write(entry, '\\0')
		elif description[1] == pymysql.constants.FIELD_TYPE.VAR_STRING and field.charsetnr == 63:
			self.output.write(entry, '\\x' + column.hex())
		elif description[1] == pymysql.constants.FIELD_TYPE.VAR_STRING:
			self.output.write(entry, str_escape(column))
		elif description[1] == pymysql.constants.FIELD_TYPE.STRING:
			self.output.write(entry, str_escape(column))
		elif description[1] == pymysql.constants.FIELD_TYPE.TIMESTAMP:
			if type(column) is datetime:
				self.output.write(entry, column.strftime('%Y-%m-%d %H:%M:%S.%f'))
			else:
				# for 0000-00-00 00:00:00 case
				self.output.write(entry, column)
		elif description[1] == pymysql.constants.FIELD_TYPE.DATETIME:
			if type(column) is datetime:
				self.output.write(entry, column.strftime('%Y-%m-%d %H:%M:%S.%f'))
			else:
				# for 0000-00-00 00:00:00 case
				self.output.write(entry, column)
		elif description[1] == pymysql.constants.FIELD_TYPE.DATE:
			self.output.write(entry, column.strftime('%Y-%m-%d'))
		elif description[1] == pymysql.constants.FIELD_TYPE.TIME:
			self.output.write(entry, str(column))
		elif description[1] == pymysql.constants.FIELD_TYPE.BLOB and field.charsetnr == 63:
			self.output.write(entry, '\\x' + column.hex())
		elif description[1] == pymysql.constants.FIELD_TYPE.BLOB:
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
		print(table_name)
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
		row = cur.fetchone()
		while row is not None:
			for i in range(len(row)):
				if i != 0:
					self.output.write(entry, '\t')

				self.write_column(entry, row[i], cur.description[i], cur._result.fields[i])
			self.output.write(entry, '\n')
			row = cur.fetchone()

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
		self.create_output()
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
		self.create_output()
		for database in databases:
			self.dump_database(database)

		self.output.close()

	def restore_table(self, database, table):
		print(table)
		def_file = self.input.open_file(database, table + '.def')
		sql = def_file.read()
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
		sql = None
		count = 0

		while line:
			columns = line.split('\t')
			print(len(columns))
			if sql is None:
				temp = '%s,' * len(columns)
				sql = 'INSERT INTO ' + table + ' values(' + temp[:-1] + ')'

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

		tables = self.input.read_tables(database)

		for table in tables:
			self.restore_table(database, table)

	def restore_all_databases(self):
		self.open_input()
		databases = self.input.read_databases()
		for database in databases:
			self.restore_database(database)

		self.input.close()

	def restore_databases(self, databases):
		self.open_input()
		for database in databases:
			self.restore_database(database)

		self.input.close()

	def dump(self):
		if self.args.single_transaction:
			cur = self.con.cursor()
			cur.execute('SET SESSION TRANSACTION ISOLATION LEVEL REPEATABLE READ')
			cur.execute('START TRANSACTION WITH CONSISTENT SNAPSHOT')

		if self.args.all_databases or self.args.user_databases:
			self.dump_all_databases()
		else:
			self.dump_databases(self.args.databases)

	def restore(self):
		cur = self.con.cursor()
		cur.execute('SET FOREIGN_KEY_CHECKS=0')

		if self.args.all_databases:
			self.restore_all_databases()
		else:
			self.restore_databases(self.args.databases)

	def create_output(self):
		try:
			if self.args.local != None:
				self.output = LocalOutput(self.args.local)
		except FileExistsError as e:
			print('ERROR ' + e.args[1])
			sys.exit(1)

	def open_input(self):
		if self.args.local != None:
			self.input = LocalInput(self.args.local)

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
		parser.add_argument('-l', '--local')
		parser.add_argument('-z', '--zip')
		parser.add_argument('-s', '--scp')
		parser.add_argument('-3', '--s3')
		parser.add_argument('-c', '--commit', type=int, default=10000)
		parser.add_argument('--databases', nargs='*')
		parser.add_argument('--all-databases', action='store_true', default=False)
		parser.add_argument('--user-databases', action='store_true', default=False)
		parser.add_argument('--single-transaction', action='store_true', default=False)
		parser.add_argument('--drop-database', type=str2bool)
		parser.add_argument('--drop-table', type=str2bool)
		parser.add_argument('-d', '--dump', action='store_true', default=False)
		parser.add_argument('-r', '--restore', action='store_true', default=False)
		parser.add_argument('-?', '--help', action='help')

		if len(sys.argv) == 1:
			print(parser.format_help())
			sys.exit(0)

		self.args = parser.parse_args()

		self.con = pymysql.connect(user=self.args.user, password=self.args.password, host=self.args.host, port=self.args.port, cursorclass=pymysql.cursors.SSCursor)

		if self.args.dump:
			self.dump()
		elif self.args.restore:
			self.restore()
		else:
			print(parser.format_help())
			sys.exit(0)

pydump = PyDump()
pydump.start()
