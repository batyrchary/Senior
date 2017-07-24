from Component import * 

class Unitconverter(Component):
	Description=''
	_attributes=[]
	_attributes_value={}
	_items=[]		
	_methods=[]

	def __init__(self):
		self.Description='This Component Converts units'
		self._attributes=[('backgroundColor','string'),('dimension','tuple'),('unit','string'),('X','int'),('Y','int')]		
		self._attributes_value={'backgroundColor':'red','dimension':(),'unit':'length', 'X':0, 'Y':0}		
		self._methods=[('categorizer','category changer')]


	def categorizer(self):
		return "category changed"

