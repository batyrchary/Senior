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


#include "Material.h"
#include <string>
#include <string.h>
#include <fstream>
#include <stdlib.h>


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

    const std::vector<Camera>& Cameras() const {
        return _cameras;
    }

	vector<Sphere> getSpheres();

	vector<Mesh> getMeshes();

///////////////////////////////////////////////edit
	vector<Material> get_materials();
	vector<PointLight> get_lights();
////////////////////////////////////////////////end edit
	Vertex getVertex(int id);
	void set_rayReflect(int rayReflect);
	int get_rayReflect();
	
	void setBackground(Color* Background_Color);
	Color getBackground();
	
	void setAmbient(Color* Ambient_Color);
	Color getAmbient();

	void addPoint_Light(PointLight _light);
	void seePoint_Light();
	
	void add_Materials(Material M);
	void seeMaterials();
	void addVertices(Vertex _vertice);
	void seeVertices();

	void add_meshes(Mesh mesh);

	void see_meshes();
	void add_spheres(Sphere s);
	void see_spheres();
	void add_cameras(Camera camera);
	void see_cameras();

    //////////////////////////////
    // READ SCENE FUNCTIONS HERE !
    //////////////////////////////
};

extern Scene* CurrentScene;

void ReadScene(int argc, char** argv);

    /*
     * Reads the scene and cameras from arguments
     * Then sets the CurrentScene pointer
     */


#endif //RAYTRACER_SCENE_H
