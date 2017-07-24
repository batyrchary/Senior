from django.shortcuts import render
from django.template.defaulttags import register
from Application import * 
app = Application()

from django.http import HttpResponse
from django.shortcuts import redirect,render
from django.template import RequestContext, loader
from django.core.context_processors import csrf
from django.contrib.auth import authenticate,login,logout
from django.contrib.auth.decorators import login_required


app.load("Weather")
app.load("Map")
app.load("Capitals")
app.load("Unitconverter")
app.load("Currency")
app.load("Timezone")
app.load("History")
app.load("Card")
app.load("Dictionary")
app.load("Image")


dictionary={}


def index(request):

	theinput2=""
	Map={}
	Unitconverter={}
	Weather={}
	Currency={}
	Timezone={}
	Capitals={}
	Card={}
	History={}
	Dictionary={}
	Image={}

	
	



	instances_created=app.execute()
	instances=app.instances()	


	if(request.GET.get("loader2")):
		app.loadDesign("/home/batyr/Desktop/")
		return redirect("/")



	for num in range(0,5):  #to iterate between 10 to 20
		for i in range(0,10): #to iterate on the factors of the number
			x=str(num)
			y=str(i)
			mybtnl=x+y
			varmi=0
			if(request.GET.get(mybtnl)):
				for key, value in instances.iteritems():
					name,xx,yy=value
					if num==xx and i==yy:
						varmi=1;
						break
				if varmi==0:
					theinput2=(request.GET.get('mytextboxl'))
					app.addInstance(theinput2,num,i)
					break

			mybtnr=x+y+"r"
			if(request.GET.get(mybtnr)):

				for key, value in instances.iteritems():
					name,xx,yy=value
					if num==xx and i==yy:
						app.removeInstance(key)
						break


	for key, value in instances_created.iteritems():
		if((value.description() == 'This Component Shows Map')):
			Map.update({key:value})
		elif((value.description() == 'This Component Shows Current Weather')):
			Weather.update({key:value})
		elif((value.description() == 'This Component Converts units')):
			Unitconverter.update({key:value})
		elif((value.description() == 'This Component Shows Current Rates')):
			Currency.update({key:value})
		elif((value.description() == 'This Component Shows Timezone')):
			Timezone.update({key:value})
		elif((value.description() == 'This Component Shows Capitals')):
			Capitals.update({key:value})
		elif((value.description() == 'This Component Shows Card Brand')):
			Card.update({key:value})
		elif((value.description() == 'This Component Shows History')):
			History.update({key:value})
		elif((value.description() == 'This Component Shows Dictionary')):
			Dictionary.update({key:value})
		elif((value.description() == 'This Component Shows Image')):
			Image.update({key:value})



	return render(request, 'personal/home.html',{'map':Map,'weather':Weather,'unitconverter':Unitconverter,'currency':Currency,'timezone':Timezone,'capitals':Capitals,'card':Card,'history':History,'dictionary':Dictionary,'image':Image})
	

def test(request):
	for num in range(0,5):  #to iterate between 10 to 20
		varmi=0
		for i in range(0,10): #to iterate on the factors of the number
			x=str(num)
			y=str(i)
			mybtnl=x+y
			component_name=request.GET.get(mybtnl)
			print component_name
			if(component_name):
				print num
				print i
				app.addInstance(component_name,num,i)								
				varmi=1
				break
		if(varmi):
			break


	instances=app.instances()	
	for key, value in instances.iteritems():
		name,xx,yy=value
		print name,xx,yy



def save(request):
	app.saveDesign("/home/batyr/Desktop/")



def remove(request):
	instances=app.instances()	
	varmi=0
	for num in range(0,5):  #to iterate between 10 to 20
		for i in range(0,10): #to iterate on the factors of the number
			x=str(num)
			y=str(i)
			mybtnl=x+y
			component_name=request.GET.get(mybtnl)
			print component_name
			if(component_name):
				for key, value in instances.iteritems():
					name,xx,yy=value
					print name,xx,yy
					if xx==num and yy==i:
						app.removeInstance(key)
						varmi=1
						break
				if(varmi):
					break
		if(varmi):
			break


