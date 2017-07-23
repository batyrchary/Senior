#ifndef RAYTRACER_SCENE_H
#define RAYTRACER_SCENE_H

#include <vector>
#include "Camera.h"
#include "Sphere.h"
#include "Mesh.h"
#include "Color.h"
#include "PointLight.h"
#include "Vertex.h"
#include <iostream>
using namespace std;
#include<cmath>
#include <math.h>       /* sin */

#include "Material.h"
#include <string>
#include <string.h>
#include <fstream>
#include <stdlib.h>
//#include "aoa_jpeg.c"
//#include "aoa_jpeg.h"


struct rotator
{
	Vector3 rotate;	
	float degree;
};

struct transformation
{
	string type;
	int id;
};



class Scene {

    std::vector<Vertex> _vertices;

    std::vector<Camera> _cameras;
    std::vector<Sphere> _spheres;
    std::vector<Mesh>   _meshes;

    std::vector<Material> _materials;

    std::vector<PointLight> _lights;

    int _rayReflect;
    Color _background;
    Color _ambient;

public:
//	vector<UCOLOR**> texturesU;
//	vector<int*> textureSizes;
		vector<float> degrees;
		vector<Vector3> axiss;

	vector< vector<transformation> > rot_tran;

	vector<bool> rot_varmi;

	vector<string> textures;
	Vertex getVertex(int id);
	int trans_count;
	vector<Vector3>translations;
	int Scale_count;
	vector<Vector3>Scales;
	int Rotate_count;
	vector<rotator>Rotates;

	void see_cameras();
	void add_cameras(Camera camera);
	void add_meshes(Mesh mesh);		

	
	void add_texture(string text);	
	void see_texture();


	void set_rayReflect(int rayReflect);
	int get_rayReflect();

	void add_spheres(Sphere s);
	vector<Sphere> getSpheres();
	vector<Mesh> getMeshes();

	vector<Material> get_materials();
	vector<PointLight> get_lights();
	
	void setBackground(Color* Background_Color);
	Color getBackground();
	
	void setAmbient(Color* Ambient_Color);
	Color getAmbient();

	void addPoint_Light(PointLight _light);
	void seePoint_Light();
	
	void add_Materials(Material M);
	void seeMaterials();













	const std::vector<Camera>& Cameras() const 
	{
        return _cameras;
	}

    //////////////////////////////
    // READ SCENE FUNCTIONS HERE !
    //////////////////////////////
};

extern Scene* CurrentScene;

void ReadScene(int argc, char** argv);


#endif //RAYTRACER_SCENE_H
