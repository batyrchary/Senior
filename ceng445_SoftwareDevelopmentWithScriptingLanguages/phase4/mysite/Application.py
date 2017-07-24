import sys
from sys import argv 
import os
import string
import pickle
import importlib


global ids


class Application:
	_loaded={}
	_components={}
	components={}
	instances_created={}
	ids=0
	_available=["Unitconverter","Weather","Map","Currency","Timezone","Capitals","Histroy","Card","Dictionary","Image"]
	
	def __init__(self):
		self._loaded={}
		self._components={}
		self.components={}
		self.instances_created={}
		ids=0
		self._available=["Unitconverter","Weather","Map","Currency","Timezone","Capitals","Histroy","Card","Dictionary","Image"]
		
	def available(self):
		return self._available

	def loaded(self): 
		return self._loaded
	
	def load(self,key):
		if(key=="Weather"):		 			
			w=importlib.import_module('Weather')
			self._loaded.update({'Weather':'This Component Shows Current Weather'})
			self.components.update({'Weather':w})
		elif(key=="Unitconverter"):			
			w=importlib.import_module('Unitconverter')
			self._loaded.update({'Unitconverter':'This Component Converts units'})
			self.components.update({'Unitconverter':w})			
		elif(key=="Map"):
			p=importlib.import_module('Map')			
			self._loaded.update({'Map':'This Component Shows Map'})
			self.components.update({'Map':p})			
		elif(key=="Currency"):
			p=importlib.import_module('Currency')			
			self._loaded.update({'Currency':'This Component Shows Current Rates'})
			self.components.update({'Currency':p})			
		elif(key=="Timezone"):
			p=importlib.import_module('Timezone')			
			self._loaded.update({'Timezone':'This Component Shows Timezone'})
			self.components.update({'Timezone':p})			
		elif(key=="Capitals"):
			p=importlib.import_module('Capitals')			
			self._loaded.update({'Capitals':'This Component Shows Capitals'})
			self.components.update({'Capitals':p})			
		elif(key=="Card"):
			p=importlib.import_module('Card')			
			self._loaded.update({'Card':'This Component Shows Card Brand'})
			self.components.update({'Card':p})			
		elif(key=="History"):
			p=importlib.import_module('History')			
			self._loaded.update({'History':'This Component Shows History'})
			self.components.update({'History':p})			
		elif(key=="Dictionary"):
			p=importlib.import_module('Dictionary')			
			self._loaded.update({'Dictionary':'This Component Shows Dictionary'})
			self.components.update({'Dictionary':p})			
		elif(key=="Image"):
			p=importlib.import_module('Image')			
			self._loaded.update({'Image':'This Component Shows Image'})
			self.components.update({'Image':p})			




	
	def saveDesign(self,path):
		filename3 = "saved3.p"
		fullpath3 = os.path.join(path, filename3)

		idd_attribute={}
		for key, value in self._components.iteritems():
			idd_attribute.update({key : self.instances_created[key]._attributes_value})

		pickle.dump(idd_attribute, open(fullpath3, "wb" ) )
		filename = "saved.p"
		fullpath = os.path.join(path, filename)

		pickle.dump(self._components, open(fullpath, "wb" ) )
		filename2 = "saved2.p"
		fullpath2 = os.path.join(path, filename2)
		pickle.dump(self._loaded, open(fullpath2, "wb" ) )
		


	def loadDesign(self,path):

		filename2 = "saved2.p"
		fullpath2 = os.path.join(path, filename2)		
		l = pickle.load( open( fullpath2, "rb" ) )

		for key, value in l.iteritems():
			self.load(key)

		filename = "saved.p"
		fullpath = os.path.join(path, filename)		
		self._components = pickle.load( open( fullpath, "rb" ) )

		idd_attribute={}
		filename3 = "saved3.p"
		fullpath3 = os.path.join(path, filename3)		
		idd_attribute = pickle.load( open( fullpath3, "rb" ) )

		
		for key, value in self._components.iteritems():
			if(value[0] == "Weather"):
				com=self.components["Weather"]
				w=com.Weather()
				w._attributes_value=idd_attribute[key]
				self.instances_created.update({key:w})
			elif(value[0] == "Unitconverter"):
				com=self.components["Unitconverter"]
				p=com.Unitconverter()
				p._attributes_value=idd_attribute[key]
				self.instances_created.update({key:p})
			elif(value[0] == "Map"):
				com=self.components["Map"]
				p=com.Map()
				p._attributes_value=idd_attribute[key]
				self.instances_created.update({key:p})
			elif(value[0] == "Currency"):
				com=self.components["Currency"]
				p=com.Currency()
				p._attributes_value=idd_attribute[key]
				self.instances_created.update({key:p})
			elif(value[0] == "Timezone"):
				com=self.components["Timezone"]
				p=com.Timezone()
				p._attributes_value=idd_attribute[key]
				self.instances_created.update({key:p})
			elif(value[0] == "Capitals"):
				com=self.components["Capitals"]
				p=com.Capitals()
				p._attributes_value=idd_attribute[key]
				self.instances_created.update({key:p})
			elif(value[0] == "Card"):
				com=self.components["Card"]
				p=com.Card()
				p._attributes_value=idd_attribute[key]
				self.instances_created.update({key:p})
			elif(value[0] == "History"):
				com=self.components["History"]
				p=com.History()
				p._attributes_value=idd_attribute[key]
				self.instances_created.update({key:p})
			elif(value[0] == "Dictionary"):
				com=self.components["Dictionary"]
				p=com.Dictionary()
				p._attributes_value=idd_attribute[key]
				self.instances_created.update({key:p})
			elif(value[0] == "Image"):
				com=self.components["Image"]
				p=com.Image()
				p._attributes_value=idd_attribute[key]
				self.instances_created.update({key:p})




	def addInstance(self,componentname, x,y):
		idss=self.ids
		string=componentname+`idss`
		com=self.components[componentname]
		while True:
			if string in self._components:
				self.ids=self.ids+1
				string=string + `self.ids`
			else:
				break

		self._components.update({string:(componentname,x,y)})		
		self.ids=self.ids+1
		if(componentname == "Weather"):
			self.instances_created.update({string:com.Weather()})
		elif(componentname == "Unitconverter"):
			self.instances_created.update({string:com.Unitconverter()})
		elif(componentname == "Map"):
			self.instances_created.update({string:com.Map()})
		elif(componentname == "Currency"):
			self.instances_created.update({string:com.Currency()})
		elif(componentname == "Timezone"):
			self.instances_created.update({string:com.Timezone()})
		elif(componentname == "Capitals"):
			self.instances_created.update({string:com.Capitals()})
		elif(componentname == "Card"):
			self.instances_created.update({string:com.Card()})
		elif(componentname == "History"):
			self.instances_created.update({string:com.History()})
		elif(componentname == "Dictionary"):
			self.instances_created.update({string:com.Dictionary()})
		elif(componentname == "Image"):
			self.instances_created.update({string:com.Image()})




		self.instances_created[string]["X"]=x
		self.instances_created[string]["Y"]=y

		return  string 

	def instances(self):
		return self._components

	def removeInstance(self, idd):
		del self.instances_created[idd]	
		del self._components[idd]	

	def callMethod(self, idd, method_name, params):
		ins=self.instances_created[idd]
		if((ins.description() == 'This Component Shows Current Weather') & (method_name == "getPage")):
			return ins.getPage()
		elif((ins.description() == 'This Component Shows Map') & (method_name == "getPage")):
			return ins.getPage()
		elif((ins.description() == 'This Component Shows Capitals') & (method_name == "getPage")):
			return ins.getPage()
		elif((ins.description() == 'This Component Shows Current Rates') & (method_name == "getPage")):
			return ins.getPage()
		elif((ins.description() == 'This Component Shows Card Brand') & (method_name == "getPage")):
			return ins.getPage()
		elif((ins.description() == 'This Component Shows History') & (method_name == "getPage")):
			return ins.getPage()
		elif((ins.description() == 'This Component Shows Timezone') & (method_name == "getPage")):
			return ins.getPage()
		elif((ins.description() == 'This Component Shows Dictionary') & (method_name == "getPage")):
			return ins.getPage()
		elif((ins.description() == 'This Component Shows Image') & (method_name == "getPage")):
			return ins.getPage()
		elif((ins.description() == 'This Component Converts units') & (method_name == "categorizer")):
			return ins.categorizer()

		elif(method_name == "description"):							
			return ins.description()
		elif(method_name == "attributes"):							
			return ins.attributes()
		elif(method_name == "methods"):							
			return ins.methods()
		elif(method_name == "execute"):							
			return ins.execute()
		elif(method_name == "__getitem__"):							
			return ins.__getitem__(params[0])
		elif(method_name == "__setitem__"):							
			return ins.__setitem__(params[0],params[1])
		else:
			return "this function does not exist"
		
	def execute(self):
		return self.instances_created

