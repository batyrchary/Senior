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
	int VID[3];
	Triangle(Vector3 _pos,int MID){VID[0]=_pos._data[0],VID[1]=_pos._data[1],VID[2]=_pos._data[2];Material_ID=MID;};

};


#endif //RAYTRACER_TRIANGLE_H

