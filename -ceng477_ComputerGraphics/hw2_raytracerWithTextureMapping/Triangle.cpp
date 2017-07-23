#include "Triangle.h"
#include "Scene.h"
#include <iostream>
using namespace std;
#include <math.h> 
#include <algorithm> 


float dot_product2(Vector3 v1, Vector3 v2)
{
	//a · b = ax × bx + ay × by + az × bz
	float ret=v1._data[0]*v2._data[0] + v1._data[1]*v2._data[1] + v1._data[2]*v2._data[2];
	return ret; 
}






float determinant(float mat[3][3])
{
	float ret;
	float a,d,g,b,e,h,c,f,i;
	a=mat[0][0];
	d=mat[0][1];
	g=mat[0][2];
	b=mat[1][0];
	e=mat[1][1];
	h=mat[1][2];
	c=mat[2][0];
	f=mat[2][1];
	i=mat[2][2];

	ret=a*(e*i - h*f) + b*(g*f - d*i) + c*(d*h - e*g);

	return ret;
}


/////////////////////////////////////////////////////////////////////////////// asagina bakmadım
bool Triangle :: Intersect(const Ray& ray, RayHitInfo& hitInfo) const
{
	Vector3 origin=ray.Origin();
	Vector3 direction=ray.Direction();
	float t1,t2;
	
	
	Vector3 a=VID[0];
	Vector3 b=VID[1];
	Vector3 c=VID[2];
	
/////////////////////////////////////////////////////////////////////////////////////////////edit	
	float b_a[3];
	float c_a[3];

	b_a[0]=b._data[0]-a._data[0];	c_a[0]=c._data[0]-a._data[0];
	b_a[1]=b._data[1]-a._data[1];	c_a[1]=c._data[1]-a._data[1];
	b_a[2]=b._data[2]-a._data[2];	c_a[2]=c._data[2]-a._data[2];	

///////////////////////////cross product
	//cx = aybz − azby
	//cy = azbx − axbz
	//cz = axby − aybx

	float b_axc_a[3]; // cross product
	
	b_axc_a[0]=b_a[1]*c_a[2] - b_a[2]*c_a[1];
	b_axc_a[1]=b_a[2]*c_a[0] - b_a[0]*c_a[2];
	b_axc_a[2]=b_a[0]*c_a[1] - b_a[1]*c_a[0];

	float nn=sqrt( (b_axc_a[0]) * (b_axc_a[0]) + (b_axc_a[1]) * (b_axc_a[1]) + (b_axc_a[2]) * (b_axc_a[2]) );
	
	Vector3 norm;
	norm._data[0]=(b_axc_a[0])/nn;
	norm._data[1]=(b_axc_a[1])/nn;
	norm._data[2]=(b_axc_a[2])/nn;



/////////////////////////////////////////////////////////////////////////////////////////////end_edit
	
	float A[3][3];
	float for_B[3][3];
	float for_Y[3][3];
	float for_T[3][3];

	A[0][0]=a._data[0]-b._data[0];	A[0][1]=a._data[0]-c._data[0];	A[0][2]=direction._data[0];
	A[1][0]=a._data[1]-b._data[1];	A[1][1]=a._data[1]-c._data[1];	A[1][2]=direction._data[1];
	A[2][0]=a._data[2]-b._data[2];	A[2][1]=a._data[2]-c._data[2];	A[2][2]=direction._data[2];


	for_B[0][0]=a._data[0]-origin._data[0];	for_B[0][1]=a._data[0]-c._data[0];	for_B[0][2]=direction._data[0];
	for_B[1][0]=a._data[1]-origin._data[1];	for_B[1][1]=a._data[1]-c._data[1];	for_B[1][2]=direction._data[1];
	for_B[2][0]=a._data[2]-origin._data[2];	for_B[2][1]=a._data[2]-c._data[2];	for_B[2][2]=direction._data[2];

	for_Y[0][0]=a._data[0]-b._data[0];	for_Y[0][1]=a._data[0]-origin._data[0];	for_Y[0][2]=direction._data[0];
	for_Y[1][0]=a._data[1]-b._data[1];	for_Y[1][1]=a._data[1]-origin._data[1];	for_Y[1][2]=direction._data[1];
	for_Y[2][0]=a._data[2]-b._data[2];	for_Y[2][1]=a._data[2]-origin._data[2];	for_Y[2][2]=direction._data[2];

	for_T[0][0]=a._data[0]-b._data[0];	for_T[0][1]=a._data[0]-c._data[0];	for_T[0][2]=a._data[0]-origin._data[0];
	for_T[1][0]=a._data[1]-b._data[1];	for_T[1][1]=a._data[1]-c._data[1];	for_T[1][2]=a._data[1]-origin._data[1];
	for_T[2][0]=a._data[2]-b._data[2];	for_T[2][1]=a._data[2]-c._data[2];	for_T[2][2]=a._data[2]-origin._data[2];
	
	float B,Y,T;

	B=(determinant(for_B))/(determinant(A));	
	Y=(determinant(for_Y))/(determinant(A));	
	T=(determinant(for_T))/(determinant(A));	

	//t min ≤ t ≤ t max
	//β + γ ≤ 1
	//0 ≤ β
	//0 ≤ γ

	if(((B+Y) <=1) && (B>=0) && (Y>=0))//intersects
	{
		hitInfo.Material=Material_ID;
		
		
		///////////////////////////////////////////////////////////////////////////////////edit
		hitInfo.Position._data[0]=(origin._data[0]) + ((direction._data[0]) * T);
		hitInfo.Position._data[1]=(origin._data[1]) + ((direction._data[1]) * T);
		hitInfo.Position._data[2]=(origin._data[2]) + ((direction._data[2]) * T);
		hitInfo.Normal._data[0]=norm._data[0];
		hitInfo.Normal._data[1]=norm._data[1];
		hitInfo.Normal._data[2]=norm._data[2];
		
		
		/////////////////////////////////////////////////////////////////////////////////end_edit
		hitInfo.center=C;		
		
		hitInfo.Parameter=T;

		hitInfo.tex_id=Texture_ID;
		hitInfo.indicator="t";
		//cout<<Y<<endl;
		hitInfo.gama=Y;
		hitInfo.beta=B;


		return true;
	}




//	MaterialId  Material;
//	Vector3     Position;
//	Vector3     Normal;

//	float       Parameter;








	return false;
}
