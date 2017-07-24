from Component import * 

class Currency(Component):
	Description=''
	_attributes=[]
	_attributes_value={}
	_items=[]		
	_methods=[]
	

	def __init__(self):
		self.Description='This Component Shows Current Rates'
		self._attributes=[('from','string'),('to','string'),('size','int'),('X','int'),('Y','int')]		
		self._attributes_value={'from':'USD','to':'EUR','size':300,'X':0,'Y':0}		
		self._methods=[('getPage','Change current page')]


	def getPage(self):
		return "Page is changed"

