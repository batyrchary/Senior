from Component import * 

class Image(Component):
	Description=''
	_attributes=[]
	_attributes_value={}
	_items=[]		
	_methods=[]
	X=0
	Y=0
	

	def __init__(self):
		self.Description='This Component Shows Image'
		self._attributes=[('X','int'),('Y','int')]		
		self._attributes_value={'X':0,'Y':0}		
		self._methods=[('getPage','Change current page')]


	def getPage(self):
		return "Page is changed"

