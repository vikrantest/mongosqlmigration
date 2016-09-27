import MySQLdb
import sys
from django.conf import settings as my_settings
from pymongo import MongoClient
from db_connectors import *



class MysqlToMongo():
	"""
	For migrating data from mysql to mongo
	"""

	def __init__(self,mysql_host,mysql_username,mysql_password,mysql_database,mongo_host,mongo_username=None,mongo_password=None,mongo_database = None,mongo_port=None):
		if not mongo_host:
			self.mongo_host = 'localhost'
		else:
			self.mongo_host = mongo_host
		if not mysql_host:
			self.mysql_host = 'localhost'
		else:
			self.mysql_host = mysql_host

		# 'localhost' if not mysql_host else _mysql_host
		# 'localhost' if not mongo_host else _mongo_host



		self.mysql_username = mysql_username
		self.mysql_password = mysql_password
		self.mysql_database = mysql_database
		self.mongo_username = mongo_username
		self.mongo_password = mongo_password
		self.mongo_database = mongo_database
		self.mongo_port = mongo_port




	def __get_connections(self):

		connection_obj = DbConnection()

		mysql_connection = connection_obj.mysql_db_connect(self.mysql_host,self.mysql_username,self.mysql_password,self.mysql_database)
		mongo_connection = connection_obj.mongo_db_connect(self.mongo_host,self.mongo_username,self.mongo_password,self.mongo_database,self.mongo_port)

		return {'mysql_connection':mysql_connection,'mongo_connection':mongo_connection}


	def __create_mongo_dataset(self,keys_maps,mysql_obj):

		self.key_values_list = {}

		for keys,values in keys_maps.items():
			query = 'select * from %s limit 3' % (keys)

			mysql_obj.execute(query)

			data = mysql_obj.fetchall()

			self.key_values_list[keys] = data

		return True


	def __insert_into_mongo(self,keys_maps,dataset,mongo_obj):
		print dataset
		mongo_obj = mongo_obj[self.mongo_database]

		for keys,value in keys_maps.items():
			print mongo_obj
			mongo_db_collection = mongo_obj[keys]
			data_list = dataset[keys]
			data_list = [dict(zip(value,list(x))) for x in data_list]
			print mongo_db_collection
			mongo_db_collection.insert_many(data_list)
			print data_list
			# for m in data_list:
			# 	mongo_db_collection.insert(m)



	def mysql_to_mongo(self,tables=[],exclude_tables=[]):
		if len(tables)<1:
			all_tables = True
		else:
			all_tables = False

		if isinstance(tables,str):
			tables = [tables]


		keys_maps = {}
		connections = self.__get_connections()

		mysql_obj = connections['mysql_connection'].cursor()

		if all_tables:

			query = 'show tables'

			mysql_obj.execute(query)

			tables =  mysql_obj.fetchall()
			tables = [x[0] for x in tables]

		for table in tables:
			query = 'desc %s' % (table)
			mysql_obj.execute(query)
			fields =  mysql_obj.fetchall()
			fields = [x[0] for x in fields]
			keys_maps[table] = fields

		if len(exclude_tables)>0:
			for tabs in exclude_tables:
				del keys_maps[tabs]

		self.__create_mongo_dataset(keys_maps,mysql_obj)

		self.__insert_into_mongo(keys_maps,self.key_values_list,connections['mongo_connection'])



		connections['mysql_connection'].close()
		connections['mongo_connection'].close()




class MongoToMysql():
	"""
	For migrating data from mongo to mysql
	"""

	def __init__(self,mysql_host,mysql_username,mysql_password,mysql_database,mongo_host,mongo_username=None,mongo_password=None,mongo_database = None,mongo_port=None):
		if not mongo_host:
			self.mongo_host = 'localhost'
		else:
			self.mongo_host = mongo_host
		if not mysql_host:
			self.mysql_host = 'localhost'
		else:
			self.mysql_host = mysql_host

		# 'localhost' if not mysql_host else _mysql_host
		# 'localhost' if not mongo_host else _mongo_host



		self.mysql_username = mysql_username
		self.mysql_password = mysql_password
		self.mysql_database = mysql_database
		self.mongo_username = mongo_username
		self.mongo_password = mongo_password
		self.mongo_database = mongo_database
		self.mongo_port = mongo_port




	def __get_connections(self):

		connection_obj = DbConnection()

		mysql_connection = connection_obj.mysql_db_connect(self.mysql_host,self.mysql_username,self.mysql_password,self.mysql_database)
		mongo_connection = connection_obj.mongo_db_connect(self.mongo_host,self.mongo_username,self.mongo_password,self.mongo_database,self.mongo_port)

		return {'mysql_connection':mysql_connection,'mongo_connection':mongo_connection}


	def mongo_to_mysql(self,collections=[],exclude_collections=[]):
		connections = self.__get_connections()
		mongo_obj = connections['mongo_connection']
		mongo_obj = mongo_obj[self.mongo_database]
		# print mongo_obj.get_collection('')
		datetime_input = raw_input('Are you using unix time?Y/N')
		try:
			if 'n' in str(datetime_input.lower()):
				datetime_format = raw_input('please enter datetime format , like mm/dd/YYYY h:m:s')
				if not validate_format(datetime_format):
					return 'Wrong input given for datetime format'

		except:
			return 'Wrong input given for datetime format'



		collections = mongo_obj.collection_names()
		doc_collect_mapping = {}
		for collection in collections:
			mongo_collect_obj = mongo_obj[collection]
			try:data = mongo_collect_obj.find()[1]
			except:
				data = {}
			doc_collect_mapping[collection] = data
		print doc_collect_mapping
		for k,v in doc_collect_mapping.items():
			print k,v.keys()










