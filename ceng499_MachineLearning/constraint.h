#ifndef constraint_h
#define constraint_h

#include <iostream>
#include <vector>
using namespace std;

class constraint{
public:
	constraint(vector<int> arg);
	vector<int> manipulator1(); // sum_squares_ctr
	bool checker1(); 		  // sum_squares_ctr
	vector<int> argument;

};

constraint::constraint(vector<int> arg)
{
	argument=arg;
}

vector<int> constraint::manipulator1()
{
	vector<int> return_value;;
	
	for(int i=0; i<argument.size(); i++)
	{
		return_value.push_back(argument[i]*argument[i]);
	}
	return return_value;
}

bool constraint::checker1()
{
	vector<int> manipulated = manipulator1();
	int ret=manipulated[0];
	for(int i=1; i<manipulated.size(); i++)
		ret=ret+manipulated[i];

	return ret==18;//	return ret!=18; 	return ret<=18;
}

#endif
