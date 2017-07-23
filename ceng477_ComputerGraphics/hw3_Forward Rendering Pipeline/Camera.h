#ifndef RAYTRACER_CAMERA_H
#define RAYTRACER_CAMERA_H

#include "Vector3.h"
#include "Image.h"
#include <string>
#include <iostream>
using namespace std;
class Camera {
public:
    struct {
       Vector3 Up, Xaxis, _position, _gaze;    
	} _space;

    struct {
		float Left, Right, Bottom, Top;
		float Far;
		float Near;

        int Width;
        int Height;

    } _imagePlane;

    std::string _outputFile;

public:

    /*
     * BEWARE! You should render each camera by using this function, we will grade it according to regulations!
     */

	int camera_id;

Camera(Vector3 position,Vector3 gaze,string outputFile,Vector3 Up,Vector3 Xaxis, float Left1, float Right1, float Bottom1, float Top1, float Near, float Far, int Width1, int Height1,int c_id);

	Image Render() const;
	const std::string& OutputFile() const
	{
		return _outputFile;
	}
};

#endif //RAYTRACER_CAMERA_H
