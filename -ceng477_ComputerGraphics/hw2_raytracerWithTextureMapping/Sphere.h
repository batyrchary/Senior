#ifndef RAYTRACER_SPHERE_H
#define RAYTRACER_SPHERE_H

#include "Ray.h"


class Sphere {

public:
    // This function gets a ray, and if the sphere intersects with the ray, returns true and fills the hitInfo struct
    // Else return false and leave hitInfo unchanged
	
	//int SID;
	int Material_ID;
	float Radius=1;
	int Texture_id;
//	int vertex_ID_Center;
	//////////////////////////////////initially
	float center[3]={0,0,0};
	float x[3]={1,0,0};
	float y[3]={0,1,0};
	float z[3]={0,0,1};
	//////////////////////////////////////////////////
//	Sphere(int sid,int MID,float R,int VIC){ SID=sid; Material_ID=MID; Radius=R; vertex_ID_Center=VIC; }



	bool Intersect(const Ray& ray, RayHitInfo& hitInfo) const;
};


#endif //RAYTRACER_SPHERE_H
