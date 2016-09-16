# #!/usr/bin/python





from db_migration import *

# a = MysqlToMongo('173.194.246.127','glydel','glydel123','sinocastel','104.154.230.129',mongo_database='test',mongo_port='27017')
a = MongoToMysql('173.194.246.127','glydel','glydel123','sinocastel','104.154.230.129',mongo_database='test',mongo_port='27017')


# 
# a.mysql_to_mongo(tables = 'rpm_log')
a.mongo_to_mysql()


