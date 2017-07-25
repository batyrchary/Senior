from django.shortcuts import render
from django.template.defaulttags import register

from django.http import HttpResponse
from django.shortcuts import redirect,render
from django.template import RequestContext, loader
#from django.core.context_processors import csrf
from django.contrib.auth import authenticate,login,logout
from django.contrib.auth.decorators import login_required




def index(request):
	return render(request, 'personal/home.html')


