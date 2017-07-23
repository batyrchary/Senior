#ifndef MATH_VECTOR3_H
#define MATH_VECTOR3_H

#include <iostream>

class Vector3
{
public:

	float _data[3];

	Vector3 ()
	{
		_data[0]=0;
		_data[1]=0;
		_data[2]=0;


	}

	Vector3 (float data[3])
	{
		_data[0]=data[0];
		_data[1]=data[1];
		_data[2]=data[2];


	}    

    friend std::istream &operator>>(std::istream &stream, Vector3 &vertex)
    {
        return stream >> vertex._data[0] >> vertex._data[1] >> vertex._data[2];
    }
};

#endif //MATH_VECTOR3_H
