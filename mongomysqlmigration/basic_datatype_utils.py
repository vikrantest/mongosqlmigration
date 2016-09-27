import datetime
import sys
import re



class DateFormatLib(objects):

	def __init__(self,value):
		self.value = valye



	def checkdatetime(self,value):
		if isinstance(value,str):
			print 444444444444

	@classmethod
	def validate_format(cls,date_format):
		date_format = date_format.lower()
		if re.match('[0-9]',date_format):
			return False
		if date_format in date_formats:
			return True
		return False
