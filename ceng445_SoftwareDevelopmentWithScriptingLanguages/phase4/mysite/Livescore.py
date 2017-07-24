from Component import * 

class Livescore(Component):
	Description=''
	_attributes=[]
	_attributes_value={}
	_items=[]		
	_methods=[]
	X=0
	Y=0


	def __init__(self):
		self.Description='This Component Shows livescores'
		self._attributes=[('height','int'),('width','int'),('dimension','tuple'),('X','int'),('Y','int')]		
		self._attributes_value={'height':280,'width':360,'dimension':(),'X':0,'Y':0}		
		self._methods=[('info','Change current page')]


	def info(self):
		return "Page of favourite team is returned"





