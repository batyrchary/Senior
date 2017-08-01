#include <iostream>
#include <vector>
#include <stdlib.h>
#include <time.h> 
#include"constraint.h"

using namespace std;

typedef struct data
{
	bool visited;
	int indexY;
	int indexX;
	int value;
}data;

vector< vector<data*> > matrix;
vector<data*> tabulist;

int numVisited=0;
int dimension,v2,index,X,Y;
int num_of_arguments;
int num_of_test_cases;
vector<vector<int > > arguments;
int test_case;

int fun(data* sCandidate)
{
	vector<int> arg=arguments[test_case];

	if(sCandidate!=NULL)
		arg.push_back(sCandidate->value);	

	constraint object=constraint(arg);
	vector<int> manipulated = object.manipulator1();
	int ret=manipulated[0];
	for(int i=1; i<manipulated.size(); i++)
		ret=ret+manipulated[i];
	
	return ret;	
}
int diff(data* sCandidate)
{
	return fun(sCandidate)-fun(NULL);
}


void printer()
{
	for(int x=0; x<dimension; x++)
	{
		for(int y=0; y<dimension; y++)
		{
			if((matrix[x][y])->value == 0)
				cout<<(matrix[x][y])->value<<"     ";
			else
				cout<<(matrix[x][y])->value<<"   ";
		}	
		cout<<endl;
	}
	cout<<endl<<"------------------------------"<<endl<<endl;

/*	for(int y=0; y<dimension; y++)
	{
		for(int x=0; x<dimension; x++)
		{
			cout<<"("<<(matrix[x][y])->indexX<<","<<(matrix[x][y])->indexY<<")   ";
		}	
		cout<<endl;
	}*/
}

void filler(vector<int> domain)
{
	for(int y=0; y<dimension; y++)
	{
		vector<data*> row;
		for(int x=0; x<dimension; x++)
		{
			data *A = new data;
			A->visited=false;
			A->indexX=x;
			A->indexY=y;
			A->value=domain[y*dimension + x];
			row.push_back(A);
		}
		matrix.push_back(row);	
	}
}


data* search_neighborhood(int X, int Y)
{
	data* bestCandidate = matrix[Y][X];
	(matrix[Y][X])->visited=true;
	numVisited++;		

	data* sCandidate=bestCandidate;
cout<<"X = "<<X<<" Y = "<<Y<<endl;	

//cout<<"value = "<<sCandidate->value<<endl;
		
		if(X!=0 && Y!=0)
		{
			sCandidate=matrix[Y-1][X-1];
			if(!sCandidate->visited)
			{				
				sCandidate->visited=true;
				numVisited++;				
				if (diff(sCandidate) < diff(bestCandidate))
					bestCandidate=sCandidate;		
			}
		}
//cout<<"value = "<<sCandidate->value<<endl;

		if(Y!=0)
		{
			sCandidate=matrix[Y-1][X];
			if(!sCandidate->visited)
			{	
				numVisited++;			
				sCandidate->visited=true;
				if (diff(sCandidate) < diff(bestCandidate))
					bestCandidate=sCandidate;		
			}
		}
//cout<<"value = "<<sCandidate->value<<endl;

		if(X!=0)
		{
			sCandidate=matrix[Y][X-1];
			if(!sCandidate->visited)
			{				
				numVisited++;
				sCandidate->visited=true;
				if (diff(sCandidate) < diff(bestCandidate))
					bestCandidate=sCandidate;		
			}
		}
//cout<<"value = "<<sCandidate->value<<endl;

		if(X!=(dimension-1) && Y!=(dimension-1))
		{
			sCandidate=matrix[Y+1][X+1];
			if(!sCandidate->visited)
			{				
				numVisited++;
				sCandidate->visited=true;
				if (diff(sCandidate) < diff(bestCandidate))
					bestCandidate=sCandidate;		
			}
		}
//cout<<"value = "<<sCandidate->value<<endl;

		if(X!=(dimension-1))
		{
			sCandidate=matrix[Y][X+1];
			if(!sCandidate->visited)
			{				
				numVisited++;
				sCandidate->visited=true;
				if (diff(sCandidate) < diff(bestCandidate))
					bestCandidate=sCandidate;		
			}
		}
//cout<<"value = "<<sCandidate->value<<endl;

		if(Y!=(dimension-1))
		{
			sCandidate=matrix[Y+1][X];
			if(!sCandidate->visited)
			{				
				numVisited++;
				sCandidate->visited=true;
				if (diff(sCandidate) < diff(bestCandidate))
					bestCandidate=sCandidate;		
			}
		}
//cout<<"value = "<<sCandidate->value<<endl;

		if(X!=(dimension-1) && Y!=0)
		{
			sCandidate=matrix[Y-1][X+1];
			if(!sCandidate->visited)
			{				
				numVisited++;
				sCandidate->visited=true;
				if (diff(sCandidate) < diff(bestCandidate))
					bestCandidate=sCandidate;		
			}
		}
//cout<<"value = "<<sCandidate->value<<endl;

		if(Y!=(dimension-1) && X!=0)
		{
			sCandidate=matrix[Y+1][X-1];
			if(!sCandidate->visited)
			{				
				numVisited++;
				sCandidate->visited=true;
				if (diff(sCandidate) < diff(bestCandidate))
					bestCandidate=sCandidate;		
			}
		}
//cout<<"value = "<<sCandidate->value<<endl;
//cout<<"sCandidate = "<<sCandidate->value<<endl;
//cout<<"bestCandidate = "<<bestCandidate->value<<endl;
		
		return bestCandidate;	
}


data* local_finder(int X, int Y)
{
	data* initial = matrix[Y][X];

	while(1)
	{
		data* best=search_neighborhood(X,Y);	
		if(best == initial)
			break;	
		initial=best;
		X=best->indexX;
		Y=best->indexY;	
	}
	return initial;
}


data* function(vector<int> domain)
{

///////////////////////////////////////////////////////////////
	X = rand() % dimension;
	Y = rand() % dimension;

	data* sBest = local_finder(X,Y);
	cout<<"local = "<<sBest->value<<endl;	
	tabulist.push_back(sBest);
	while(1)
	{
		if(diff(sBest)==0)		
		{
			cout<<"tabulist = ";
			for(int i=0; i<tabulist.size(); i++)
			cout<<tabulist[i]->value<<" ";
			cout<<endl;

			cout<<"global = "<<sBest->value<<endl;
			break;
		}

		if(numVisited >= (dimension*dimension))
		{
//			cout<<"not found"<<endl;
			break;
		}		

		X = rand() % dimension;
		Y = rand() % dimension;
		
		if(!(matrix[Y][X])->visited)
		{		
			sBest = local_finder(X,Y);
			cout<<"local = "<<sBest->value<<endl;	
			tabulist.push_back(sBest);
			cout<<"<<<<<<<<<<<<<<<<>>>>>>>>>>>>>>"<<endl;
		}	
	}
	return sBest;


}





int main()
{
	srand (time(NULL));
	cout<<"number of arguments:";	
	cin>>num_of_arguments;

	cout<<"number of test cases:";	
	cin>>num_of_test_cases;

	for(int i=0; i<num_of_test_cases; i++)
	{
		vector<int> arg_vec;

		for(int i=0; i<num_of_arguments; i++)
		{
			int arg=rand() % num_of_arguments + 1;
			arg_vec.push_back(arg);	
		}
			arguments.push_back(arg_vec);	
	}

	vector<int> domain;
	
	dimension = rand() % 16 + 1;         // dimension in the range 1 to 16
	index = rand() % (dimension*dimension);

	for(int i=0; i<dimension*dimension; i++)
	{
		v2 = rand() % 800 + 100;     
		if(i==index)
			domain.push_back(0);
		else
			domain.push_back(v2);
	}		
	cout<<"index = "<<index<<endl;
	cout<<"dimension = "<<dimension<<endl;

	filler(domain);
	printer();

	test_case=0;		
	data* neutral=function(domain);

	int passed=0;

	for(int i=0; i<num_of_test_cases; i++)
	{
		test_case=i;
		cout<<"test_case "<<i+1<<endl;
		cout<<"arguments = ";
		for(int j=0; j<num_of_arguments; j++)
			cout<<arguments[i][j]<<" ";	
		
		cout<<endl;		

		if(diff(neutral)==0)		
		{
			cout<<"passed"<<endl;
			passed++;
		}
		else
		{
			passed=0;
			cout<<"failed, restarting everything"<<endl;
			i=0;
			test_case=i;
			neutral=function(domain);
			if(numVisited >= (dimension*dimension))
			{
				cout<<"not found"<<endl;
				break;
			}		
		}

	}
	cout<<"<<<<<<<<<<<<<<<<>>>>>>>>>>>>>>>>"<<endl;
	if(passed == num_of_test_cases)
	{
		cout<<"passed all tests"<<endl;
		cout<<"neutral value = "<<neutral->value<<endl;	
	}
	else
	{
		cout<<"not found"<<endl;
	}
	

	return 0;
}


