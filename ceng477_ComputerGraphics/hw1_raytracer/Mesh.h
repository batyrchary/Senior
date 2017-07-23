#ifndef RAYTRACER_MESH_H
#define RAYTRACER_MESH_H

#include <vector>
#include "Triangle.h"
#include<iostream>
using namespace std;

class Mesh 
{
	public:
	int Model_id;
	vector<Triangle> triangles;
};


#endif //RAYTRACER_MESH_H
