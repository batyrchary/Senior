from Component import * 

class Map(Component):
	Description=''
	_attributes=[]
	_attributes_value={}
	_items=[]		
	_methods=[]
	X=0
	Y=0
	

	def __init__(self):
		self.Description='This Component Shows Map'
		self._attributes=[('zoom','integer'),('lat','float'),('lng','float'),('X','int'),('Y','int')]		
		self._attributes_value={'zoom':10,'lat':50,'lng':-0.2,'X':0,'Y':0}		
		self._methods=[('getPage','Change current page')]

	def getPage(self):
		return "Page is changed"


