import MySQLdb
import sys
from django.conf import settings as my_settings
from pymongo import MongoClient




class DbConnection(object):


	def mysql_db_connect(self,host=None,username=None,password=None,database=None):
		_host = host or my_settings.DATABASE_HOST
		_username = username or my_settings.DATABASE_USERNAME
		_password = password or my_settings.DATABASE_PASSWORD
		_database = database or my_settings.DATABASE

		try:
			connection = MySQLdb.connect(_host,_username,_password,_database)
			return connection
		except MySQLdb.Error, e:
			print "Error %d: %s" % (e.args[0], e.args[1])
			return None


	def mongo_db_connect(self,host=None,username=None,password=None,database=None,port=None):#check for username and password
		_host = host or my_settings.MONGO_DATABASE_HOST
		_database = database or my_settings.MONGO_DATABASE
		print port#None check
		if not port:
			_port = 27017
		url = '%s:%s' % (_host,port)
		print url
		print 0000000000000000

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



	def redis_db_connect(self,host=None,username=None,password=None,database=None):
		_host = host or my_settings.REDIS_DATABASE_HOST
		_username = username or my_settings.REDIS_DATABASE_USERNAME
		_password = password or my_settings.REDIS_DATABASE_PASSWORD
		_database = database or my_settings.REDIS_DATABASE
		_port=6379

		try:
			connection = redis.StrictRedis(host=_host, port=port, db=_database)
			return connection
		except:
			print "Error while connecting redis server db"
			return None


		return True
		
	



