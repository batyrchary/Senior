#ifndef RAYTRACER_MESH_H
#define RAYTRACER_MESH_H

#include <vector>
#include "Triangle.h"
#include<iostream>
using namespace std;


struct transformation
{
	string type;
	int id;
};

class Mesh 
{

	public:
	int Model_id;
	int Model_type;

	int number_of_transformation;
	vector<transformation> transformations;

	int number_of_triangles;

	vector<Triangle> triangles;



};


#endif //RAYTRACER_MESH_H
