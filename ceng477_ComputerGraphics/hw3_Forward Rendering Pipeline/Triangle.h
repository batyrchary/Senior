#ifndef RAYTRACER_TRIANGLE_H
#define RAYTRACER_TRIANGLE_H


class Triangle
{
public:
	int Model_id;
	int Model_type;
	int VID[3];

	Triangle(int MID, int model_type, int a, int b, int c)
	{
		VID[0]=a;
		VID[1]=b;
		VID[2]=c;
		Model_id=MID;
		Model_type=model_type;	
	}

};

#endif //RAYTRACER_TRIANGLE_H

