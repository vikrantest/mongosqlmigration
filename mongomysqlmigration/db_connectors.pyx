import MySQLdb
import sys
from django.conf import settings as my_settings
from pymongo import MongoClient
import psycopg2
import redis


LOCALHOST = 'localhost'

class DbConnection(object):


	def mysql_db_connect(self,host=None,username=None,password=None,database=None):
		_host = host or LOCALHOST
		_username = username
		_password = password
		_database = database

		try:
			connection = MySQLdb.connect(_host,_username,_password,_database)
			return connection
		except MySQLdb.Error, e:
			print "Error %d: %s" % (e.args[0], e.args[1])
			return None


	def mongo_db_connect(self,host=None,username=None,password=None,database=None,port=None):#check for username and password
		_host = host or LOCALHOST
		_database = database
		print port#None check
		if not port:
			_port = 27017
		url = '%s:%s' % (_host,port)

		try:
			if any([username is None,password is None ]):
				connection = MongoClient(url,connect=False)
			else:
				connection = MongoClient(url,username=username,password=password,connect=False)
			return connection
		except:
			print "Error while connecting mongo db"
			import sys
			print sys.exc_info()
			return None



	# def redis_db_connect(self,host=None,username=None,password=None,database=None):
	# 	_host = host or LOCALHOST
	# 	# _username = username or my_settings.REDIS_DATABASE_USERNAME
	# 	_password = password
	# 	_database = database or 0
	# 	_port=6379

	# 	try:
	# 		if password:
	# 			connection = redis.Redis(host=_host, port=port, db=_database,password=password)
	# 		else:
	# 			connection = redis.Redis(host=_host, port=port, db=_database)
	# 		return connection
	# 	except:
	# 		print "Error while connecting redis server db"
	# 		return None


	# 	return True

	def redis_db_connect(self,host=None,username=None,password=None,database=None):
		_host = host
		_password = password
		_database = database or 0
		_port=6379
		print _host,_port,_password

		try:
			if password:
				connection = redis.Redis(host= _host, port= _port, db= _database,password= _password)
			else:
				connection = redis.Redis(host= _host, port= _port, db= _database)
			return connection
		except:
			print "Error while connecting redis server db"
			return None


		return True


	def postgres_db_connect(self,host=None,username=None,password=None,database=None):
		_host = host or LOCALHOST
		_username = username
		_password = password
		_database = database

		try:
			connection = psycopg2.connect("host=_host,username=_username,password=_password,database=_database")
			return connection
		except MySQLdb.Error, e:
			print "Error %d: %s" % (e.args[0], e.args[1])
			return None

		
	



