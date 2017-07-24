class Component():

	def __init__(self):
		pass
		
	def description(self):
		return self.Description

	def attributes(self): 
		return self._attributes

	def __getitem__(self,key):
		try:
			return self._attributes_value[key]
		except KeyError:
			return 'This key does not exist'

	def __setitem__(self,key,value):
		try:
			a=self._attributes_value[key]
			self._attributes_value[key]=value
		except KeyError:
			return 'This key does not exist'

	def methods(self):
		return self._methods

	def execute(self):
		if (self.Description=='This Component Shows Current Weather'):
			return "Weather component executed"
		elif (self.Description=='This Component Shows Map'):
			return "Map component executed"
		elif (self.Description=='This Component Shows Current Rates'):
			return "Currency component executed"
		elif (self.Description=='This Component Shows Current Time'):
			return "Clock component executed"
		elif (self.Description=='This Component Shows livescores'):
			return "Livescore component executed"
		elif (self.Description=='This Component Shows History'):
			return "History component executed"
		elif (self.Description=='This Component Shows Card Brand'):
			return "Card component executed"
		else:
			return "This Component Converts units"



