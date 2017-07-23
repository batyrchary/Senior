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
	
	Vector3 Center;

	Center._data[0]=center[0];
	Center._data[1]=center[1];
	Center._data[2]=center[2];

	Vector3 diff;
	diff._data[0]=origin._data[0]-Center._data[0];
	diff._data[1]=origin._data[1]-Center._data[1];
	diff._data[2]=origin._data[2]-Center._data[2];
	
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
	
	hitInfo.Normal._data[0]=(hitInfo.Position._data[0]-Center._data[0])/Radius;
	hitInfo.Normal._data[1]=(hitInfo.Position._data[1]-Center._data[1])/Radius;
	hitInfo.Normal._data[2]=(hitInfo.Position._data[2]-Center._data[2])/Radius;
	
//	cout<<"radius ="<< Radius<<"center = "<<Center._data[0]<<" "<<Center._data[1]<<" "<<Center._data[2]<<endl;
	
	
	
	
	hitInfo.tex_id=Texture_id;
	hitInfo.indicator="s";
	
	hitInfo.Parameter=min(t1,t2);
	hitInfo.center=Center;
	hitInfo.x_s._data[0]=x[0];
	hitInfo.x_s._data[1]=x[1];
	hitInfo.x_s._data[2]=x[2];
	
	hitInfo.y_s._data[0]=y[0];
	hitInfo.y_s._data[1]=y[1];
	hitInfo.y_s._data[2]=y[2];

	hitInfo.z_s._data[0]=z[0];
	hitInfo.z_s._data[1]=z[1];
	hitInfo.z_s._data[2]=z[2];

	
	
//	cout<<"center= "<<hitInfo.center._data[0]<<" "<<hitInfo.center._data[1]<<" "<<hitInfo.center._data[2]<<endl;
	hitInfo.R=Radius;
	
	
	return true;
}


