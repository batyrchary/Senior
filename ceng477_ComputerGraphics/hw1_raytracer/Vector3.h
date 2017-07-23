#ifndef MATH_VECTOR3_H
#define MATH_VECTOR3_H

#include <iostream>

class Vector3
{
public:
    float _data[3];

    friend std::istream &operator>>(std::istream &stream, Vector3 &vertex)
    {
        return stream >> vertex._data[0] >> vertex._data[1] >> vertex._data[2];
    }
    void operator=(Vector3* XYZ)
    {
	this->_data[0]=XYZ->_data[0];
	this->_data[1]=XYZ->_data[1];
	this->_data[2]=XYZ->_data[2];
    }
};

#endif //MATH_VECTOR3_H
