from Component import * 

class Weather(Component):
	Description=''
	_attributes=[]
	_attributes_value={}
	_items=[]		
	_methods=[]
	X=0
	Y=0
	

	def __init__(self):
		self.Description='This Component Shows Current Weather'
		self._attributes=[('language','string'),('url','string'),('type','string'),('dimension','tuple'),('X','int'),('Y','int')]		
		self._attributes_value={'language':'tr','url':'default','type':'c','dimension':(),'X':0,'Y':0}		
		self._methods=[('getPage','Change current page')]


	def getPage(self):
		return "Page is changed"

