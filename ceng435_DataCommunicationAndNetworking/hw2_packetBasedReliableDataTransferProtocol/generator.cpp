#include <iostream>
#include <stdlib.h>
#include <fstream>

using namespace std;

int main()
{
	int i=1,t,j;
	char a='A';
	ofstream file;
	file.open("input.txt");
	for(t=0;t<5;t++)
	{
		for(i=0;i<1000;i++)
		{
			for(j=0;j<999;j++)
			{
				file<<(char)((rand()%57)+65);	
			}
			file<<endl;
		}
	}
	file.close();
	return 0;
}
