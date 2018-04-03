import sys, getopt
import math



wordbag=set()

filevectors={}

fileTFIDF={}




def readfiles(name):

   wordDict={}
   file = open(name, "r+")
   for line in file:
      line = line.strip('\n')
      words=line.split(" ")
      for w in words:
         wordbag.add(w)

         if w in wordDict:
            newCounter=wordDict.get(w)
            newCounter=newCounter+1
            wordDict.update({w: newCounter})
         else:
            wordDict[w]=1

         filevectors[name]=wordDict



def howmanyfilescontain(word):
   counter=0

   for key in filevectors:


      if word in filevectors.get(key):
         counter=counter+1

   return counter


def createTFIDF(name,howmanyfiles):
   
   values=filevectors.get(name)
#   print values

   Datahowmanyfilescontain=[]
   for w in wordbag:
      Datahowmanyfilescontain.append(howmanyfilescontain(w))

   TFIDFvec=[]
   index=0
   for w in wordbag:
      if w in values:
         coefficient=values.get(w)

         logDown=Datahowmanyfilescontain[index]

         logUP=(howmanyfiles*1.0)/logDown
         
         logOut=math.log(logUP,2.0)

         TFIDFvec.append(logOut*coefficient)

         
      else:
         TFIDFvec.append(0)
      index=index+1


   fileTFIDF[name]=TFIDFvec
 #  print name+"="+str(TFIDFvec)+"\n"




def similarityOfVecs(first,second,n):

   vec1=fileTFIDF.get(first)
   vec2=fileTFIDF.get(second)

   total=0
   for i in range (0, n):
      x=vec1[i]
      y=vec2[i]
      #total=total+math.pow(abs(x-y),n)
      total=total+abs(x-y)**2
      

   return total**(1/float(2))
   #return total**(1/float(n))





def printResult(argv,n):
   
   lenn=len(argv)
#   print "number of words in wordbag="+str(n)+"\n"

   for i in range (0,lenn-1):
      j=i+1
      for j in range (i+1, lenn):
         print argv[i]+","+argv[j]+"="+str(similarityOfVecs(argv[i],argv[j],n))



   



def main(argv):

   howmanyfiles=len(argv)

#   print howmanyfiles

   for arg in argv:
      readfiles(arg)

#   print "wordbag="+str(wordbag)+"\n"

   for arg in argv:
      createTFIDF(arg,howmanyfiles)

   printResult(argv,len(wordbag))




if __name__ == "__main__":

   main(sys.argv[1:])