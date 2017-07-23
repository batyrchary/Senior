#include "Sphere.h"
#include "Scene.h"

float dot_product(Vector3 v1, Vector3 v2)
{
	//a · b = ax × bx + ay × by + az × bz
	float ret=v1._data[0]*v2._data[0] + v1._data[1]*v2._data[1] + v1._data[2]*v2._data[2];
	return ret; 
}
////////////////////////////////////////////////////////////////////////////////////// asagina bakmadim
bool Sphere::Intersect(const Ray& ray, RayHitInfo& hitInfo) const
{

	Vector3 origin=ray.Origin();
	Vector3 direction=ray.Direction();
	float t1,t2;
	
	Vector3 center;
	center=CurrentScene->getVertex(vertex_ID_Center-1).Position();

	Vector3 diff;
	diff._data[0]=origin._data[0]-center._data[0];
	diff._data[1]=origin._data[1]-center._data[1];
	diff._data[2]=origin._data[2]-center._data[2];
	
	float a=dot_product(direction,direction);
	float b=2*dot_product(diff,direction);
	float c=dot_product(diff,diff)-Radius*Radius;
	
	if(0>(b*b - 4*a*c))
	{
		return false;
	}
	
	t1=(- b + ( sqrt(b*b - 4*a*c) ))/(2*a);
	t2=(- b - ( sqrt(b*b - 4*a*c) ))/(2*a);
	
//	n=(p-c)/R;
	
	hitInfo.Material=Material_ID;
	hitInfo.Position._data[0]=(origin._data[0]) + (direction._data[0]) * (min(t1,t2));
	hitInfo.Position._data[1]=(origin._data[1]) + (direction._data[1]) * (min(t1,t2));
	hitInfo.Position._data[2]=(origin._data[2]) + (direction._data[2]) * (min(t1,t2));
	
	hitInfo.Normal._data[0]=(hitInfo.Position._data[0]-center._data[0])/Radius;
	hitInfo.Normal._data[1]=(hitInfo.Position._data[1]-center._data[1])/Radius;
	hitInfo.Normal._data[2]=(hitInfo.Position._data[2]-center._data[2])/Radius;
	
	

	
	hitInfo.Parameter=min(t1,t2);
	
	return true;
}


