#ifndef RAYTRACER_MATERIAL_H
#define RAYTRACER_MATERIAL_H

#include "Color.h"
#include "Vector3.h"
struct Specular
{
	Color RGB;
	float SpecExp;
};

class Scene;

class Material {
    //////////////////
    /// PUT YOUR VARIABLES, HELPER FUNCTIONS HERE !
    //////////////////
public:

    /*
     * Implement a Calculate function that does Diffuse, Specular and Ambient, Reflective shading
     */

	int MID;
	Color Ambient;
	Color Diffuse;
	Color Reflectance;
	Specular specular;


    friend std::istream& operator>>(std::istream& stream, Material& mat);
};

using MaterialId = int;

#endif //RAYTRACER_MATERIAL_H
