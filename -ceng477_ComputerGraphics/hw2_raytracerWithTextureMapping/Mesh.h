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
	Vector3 center;
	vector<Triangle> triangles;
	Mesh(int MID, int tex_id)
	{
		
		
		center._data[0]=0;
		center._data[1]=0;
		center._data[2]=0;

		Vector3 a1;
		a1._data[0]=-0.5; a1._data[1]=0.5; a1._data[2]=0.5; 

		Vector3 a2;
		a2._data[0]=-0.5; a2._data[1]=-0.5; a2._data[2]=0.5; 
		Vector3 a3;
		a3._data[0]=0.5; a3._data[1]=-0.5; a3._data[2]=0.5; 
		Vector3 a4;
		a4._data[0]=0.5; a4._data[1]=0.5; a4._data[2]=0.5; 
		Vector3 a5;
		a5._data[0]=0.5; a5._data[1]=0.5; a5._data[2]=-0.5; 
		Vector3 a6;
		a6._data[0]=0.5; a6._data[1]=-0.5; a6._data[2]=-0.5; 
		Vector3 a7;
		a7._data[0]=-0.5; a7._data[1]=0.5; a7._data[2]=-0.5; 
		Vector3 a8;
		a8._data[0]=-0.5; a8._data[1]=-0.5; a8._data[2]=-0.5; 

		Triangle f1(MID,tex_id,a1,a2,a3,center);
		Triangle f11(MID,tex_id,a1,a3,a4,center);

		Triangle f2(MID,tex_id,a7,a8,a2,center);
		Triangle f22(MID,tex_id,a7,a2,a1,center);

		Triangle f3(MID,tex_id,a5,a6,a8,center);
		Triangle f33(MID,tex_id,a5,a8,a7,center);

		Triangle f4(MID,tex_id,a4,a3,a6,center);
		Triangle f44(MID,tex_id,a4,a6,a5,center);

		Triangle f5(MID,tex_id,a2,a8,a6,center);
		Triangle f55(MID,tex_id,a2,a6,a3,center);

		Triangle f6(MID,tex_id,a7,a1,a4,center);
		Triangle f66(MID,tex_id,a7,a4,a5,center);

		triangles.push_back(f1);
		triangles.push_back(f11);
		triangles.push_back(f2);
		triangles.push_back(f22);
		triangles.push_back(f3);
		triangles.push_back(f33);
		triangles.push_back(f4);
		triangles.push_back(f44);
		triangles.push_back(f5);
		triangles.push_back(f55);
		triangles.push_back(f6);
		triangles.push_back(f66);


	};

};


#endif //RAYTRACER_MESH_H
