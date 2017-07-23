#ifndef RAYTRACER_TRIANGLE_H
#define RAYTRACER_TRIANGLE_H

#include "Ray.h"
#include <math.h> 
#include <algorithm> 


class Triangle
{
public:
    // This function gets a ray, and if the triangle intersects with the ray, returns true and fills the hitInfo struct
    // Else return false and leave hitInfo unchanged
    bool Intersect(const Ray& ray, RayHitInfo& hitInfo) const;

	int Material_ID;
	int Texture_ID;
	Vector3 C;
	Vector3 VID[3];
	Triangle(int MID, int tex_id, Vector3 a, Vector3 b, Vector3 c, Vector3 center)
	{
		C=center;
	
		VID[0]=a;
		VID[1]=b;
		VID[2]=c;
		Material_ID=MID;
		Texture_ID=tex_id;	
	}
};


#endif //RAYTRACER_TRIANGLE_H

