#ifndef RAYTRACER_CAMERA_H
#define RAYTRACER_CAMERA_H

#include "Vector3.h"
#include "Ray.h"
#include "Image.h"
#include <string>
#include <iostream>
using namespace std;
class Camera {
public:
    struct {
        Vector3 Forward, Up, Left;
    } _space;

    struct {
        float Left, Right, Bottom, Top;
        float Distance;

        int Width;
        int Height;

    } _imagePlane;

    Vector3 _position;
    Vector3 _gaze;

    std::string _outputFile;

public:

    /*
     * BEWARE! You should render each camera by using this function, we will grade it according to regulations!
     */

	int camera_id;

	Camera(Vector3 position,Vector3 gaze,string outputFile,Vector3 F,Vector3 U,Vector3 L, float Left1, float Right1, float Bottom1, float Top1, float Distance1, int Width1, int Height1,int c_id);
	Image Render() const;
	Color for_reflect(Ray ray, int count) const;

	const std::string& OutputFile() const
	{
		return _outputFile;
	}
};

#endif //RAYTRACER_CAMERA_H
