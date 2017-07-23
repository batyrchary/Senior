#ifndef RAYTRACER_SCENE_H
#define RAYTRACER_SCENE_H

#include <vector>
#include "Camera.h"
#include "Mesh.h"
#include "Color.h"

#include <iostream>
using namespace std;
#include <string>
#include <string.h>
#include <fstream>
#include <stdlib.h>
#include<cmath>
#include <math.h>       /* sin */


struct rotator
{
	Vector3 rotate;	
	float degree;
};


class Scene {

public:
    std::vector<Vector3> _vertices;
    std::vector<Camera> _cameras;
    std::vector<Mesh>   _meshes;
    Color _background;

	int num_of_vertices;
	vector<Color> color_of_vertices;
	vector<Vector3> translations;
	vector<Vector3> scales;
	vector<rotator> rotations;



    const std::vector<Camera>& Cameras() const {
        return _cameras;
    }

};

extern Scene* CurrentScene;


void ReadScene(int argc, char** argv);

#endif //RAYTRACER_SCENE_H
