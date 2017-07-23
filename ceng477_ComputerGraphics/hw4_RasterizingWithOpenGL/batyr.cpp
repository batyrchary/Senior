#include <iostream>
using namespace std;
#include <string>
#include <fstream>
#include <stdlib.h>
#include<cmath>
#include <math.h>       /* sin */
#include<vector>
#include <sys/time.h>
#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <cassert>
#include <sstream>
#include <chrono>
#include <GL/glew.h>
#include <GL/gl.h>   // The GL Header File
#include <GL/freeglut.h> // The GL Utility Toolkit (Glut) Header
#include <GL/glu.h>
#include <GL/glut.h>
#define PI 3.14159265
#include <pthread.h>

int gWidth, gHeight;


class Vector3
{
public:
    float _data[3];

    friend std::istream &operator>>(std::istream &stream, Vector3 &vertex)
    {
        return stream >> vertex._data[0] >> vertex._data[1] >> vertex._data[2];
    }

	void normalize()
	{
		float dis=sqrt(_data[0]*_data[0] + _data[1]*_data[1] + _data[2]*_data[2]  );
		_data[0]=_data[0]/dis;
		_data[1]=_data[1]/dis;
		_data[2]=_data[2]/dis;


	}
};

class Color
{
public:
    float _data[3];

    friend std::istream &operator>>(std::istream &stream, Color &color)
    {
        return stream >> color._data[0] >> color._data[1] >> color._data[2];
    }
};

struct light_S
{
	Vector3 position;	
	Color intensity;
};
struct light
{
	GLfloat lightPos[4];	
	GLfloat lightColor[4];
	GLfloat ambientColor[4];
};


struct material
{
	Color ambient_r;
	Color diffuse_r;
	Color specular_r;
	float specExp;
};
struct rotator
{
	Vector3 rotate;	
	float degree;
};
struct Mesh
{
	int type;
	int material_id;
	int number_of_triangles;
	vector<string> t_type;
	vector<int> t_id;
	int start_index;
	int start_normals;	
};
struct Camera
{
	Vector3 position;
	Vector3 gaze;	
	Vector3 Up;	
	float left,right,bottom,top,near,far;
	int HorRes,VerRes;
};

struct norm
{
	Vector3 normal;
	int kac=0;
};


void four_multiplier(float T[4][4], float P[4], float (&P_p)[4])
{
	P_p[0]=(T[0][0]*P[0]) + (T[0][1]*P[1]) + (T[0][2]*P[2]) + (T[0][3]*P[3]);
	P_p[1]=(T[1][0]*P[0]) + (T[1][1]*P[1]) + (T[1][2]*P[2]) + (T[1][3]*P[3]);
	P_p[2]=(T[2][0]*P[0]) + (T[2][1]*P[1]) + (T[2][2]*P[2]) + (T[2][3]*P[3]);
	P_p[3]=(T[3][0]*P[0]) + (T[3][1]*P[1]) + (T[3][2]*P[2]) + (T[3][3]*P[3]);
}

void rotasyon2(Vector3 axis, Vector3 gaze, Vector3 (&finito), float degree)
{
	float d=degree;
	float dis;

	Vector3 u;
	u._data[0]=axis._data[0]; u._data[1]=axis._data[1]; u._data[2]=axis._data[2];

//	u.normalize();
	Vector3 v;
	if(u._data[0] <= u._data[1] && u._data[0] <= u._data[2] ) //a is smallest					
	{
		//cout<<"girdim1"<<endl;
		v._data[0]=0; 
		v._data[1]=-u._data[2];
		v._data[2]=u._data[1];
	}
	else if(u._data[1] <= u._data[0] && u._data[1] <= u._data[2]) // b is smallest
	{
		//	cout<<"girdim"<<endl;
		v._data[0]=-u._data[2]; 
		v._data[1]=0;
		v._data[2]=u._data[0];
	}
	else // c is smallest
	{
	//	cout<<"girdim3"<<endl;
		v._data[0]=-u._data[1]; 
		v._data[1]=u._data[0];
		v._data[2]=0;
	}
//	v.normalize();
	Vector3 w;
	//cx = aybz − azby = 3×7 − 4×6 = −3
	//cy = azbx − axbz = 4×5 − 2×7 = 6
	//cz = axby − aybx = 2×6 − 3×5 = −3
	w._data[0]=(u._data[1])*(v._data[2]) - (u._data[2])*(v._data[1]);  
	w._data[1]=(u._data[2])*(v._data[0]) - (u._data[0])*(v._data[2]);  
	w._data[2]=(u._data[0])*(v._data[1]) - (u._data[1])*(v._data[0]);  
				
//	w.normalize();

	float M_R[4][4];
	M_R[0][0]=u._data[0];	M_R[0][1]=u._data[1];	M_R[0][2]=u._data[2];	M_R[0][3]=0;
	M_R[1][0]=v._data[0];	M_R[1][1]=v._data[1];	M_R[1][2]=v._data[2];	M_R[1][3]=0;
	M_R[2][0]=w._data[0];	M_R[2][1]=w._data[1];	M_R[2][2]=w._data[2];	M_R[2][3]=0;
	M_R[3][0]=0;			M_R[3][1]=0;			M_R[3][2]=0;			M_R[3][3]=1;

	float M_R_I[4][4];
	M_R_I[0][0]=u._data[0];	M_R_I[0][1]=v._data[0];	M_R_I[0][2]=w._data[0];	M_R_I[0][3]=0;
	M_R_I[1][0]=u._data[1];	M_R_I[1][1]=v._data[1];	M_R_I[1][2]=w._data[1];	M_R_I[1][3]=0;
	M_R_I[2][0]=u._data[2];	M_R_I[2][1]=v._data[2];	M_R_I[2][2]=w._data[2];	M_R_I[2][3]=0;
	M_R_I[3][0]=0;			M_R_I[3][1]=0;			M_R_I[3][2]=0;			M_R_I[3][3]=1;

	float R_x[4][4];
	R_x[0][0]=1;	R_x[0][1]=0;				R_x[0][2]=0;				R_x[0][3]=0;
	R_x[1][0]=0;	R_x[1][1]=cos(d*PI/180.0f);	R_x[1][2]=-sin(d*PI/180.0f);	R_x[1][3]=0;
	R_x[2][0]=0;	R_x[2][1]=sin(d*PI/180.0f);	R_x[2][2]=cos(d*PI/180.0f);	R_x[2][3]=0;
	R_x[3][0]=0;	R_x[3][1]=0;				R_x[3][2]=0;				R_x[3][3]=1;


	float X_p[4]; float x[4]={gaze._data[0],gaze._data[1],gaze._data[2],1}; 

//	cout<<"ici="<<gaze._data[0]<<" "<<gaze._data[1]<<" "<<gaze._data[2]<<endl;

	four_multiplier(M_R,x,X_p);
	four_multiplier(R_x,X_p,x);
	four_multiplier(M_R_I,x,X_p);
	

	finito._data[0]=X_p[0];	finito._data[1]=X_p[1];	finito._data[2]=X_p[2];

//	cout<<"ici2="<<X_p[0]<<" "<<X_p[1]<<" "<<X_p[2]<<endl;
//	cout<<"ici3="<<finito._data[0]<<" "<<finito._data[1]<<" "<<finito._data[2]<<endl;
	finito.normalize();
//	cout<<"ici4="<<finito._data[0]<<" "<<finito._data[1]<<" "<<finito._data[2]<<endl;

}

void normal_calculator(Vector3 a,Vector3 b,Vector3 c,Vector3 &normal)
{	
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
	
	normal._data[0]=(b_axc_a[0])/nn;
	normal._data[1]=(b_axc_a[1])/nn;
	normal._data[2]=(b_axc_a[2])/nn;
}

int number_of_light_sources, number_of_materials, number_of_translations, number_of_scales, number_of_rotations, number_of_vertices;
int number_of_meshes;
int totalNumTriangles=0;

GLuint *indices;
GLfloat *vertexData;
GLfloat *normalData;
vector<light> lights;


vector<int> indexes;
vector<Vector3> normals; //triangle normals
vector<Vector3> vertex_normals;
vector<norm> vertex_normals_ortalamasiz;


Color background,ambient;
vector<Vector3> translations;
vector<Vector3> scales;
vector<rotator> rotations;
vector<light_S> light_sources;		
vector<material> materials;
vector<Mesh> meshes;
Camera camera;

string string_musur;
int int_musur;

GLfloat posx,posy,posz,gazex,gazey,gazez,upx,upy,upz,ux,uy,uz;

void kopyalayci()
{

//	indices = new GLuint[indexes.size()]; 
	indices=(GLuint*)malloc(sizeof(GLuint)*indexes.size());
	for(int i=0;i<indexes.size();i++)
		indices[i]=indexes[i]-1;

//	normalData = new GLfloat[vertex_normals.size()*3]; 
	normalData=(GLfloat*)malloc(sizeof(GLfloat)*vertex_normals.size()*3);
	for(int i=0;i<vertex_normals.size();i++)
	{
		normalData[i*3]=vertex_normals[i]._data[0];
		normalData[i*3+1]=vertex_normals[i]._data[1];
		normalData[i*3+2]=vertex_normals[i]._data[2];
	}

	for(int i=0; i<light_sources.size(); i++)
	{
		light_S l=light_sources[i];

		light ll;
		ll.lightPos[0]=l.position._data[0];
		ll.lightPos[1]=l.position._data[1];
		ll.lightPos[2]=l.position._data[2];
		ll.lightPos[3]=1;

		ll.lightColor[0]=l.intensity._data[0];
		ll.lightColor[1]=l.intensity._data[1];
		ll.lightColor[2]=l.intensity._data[2];
		ll.lightColor[3]=1;

		ll.ambientColor[0]=ambient._data[0];
		ll.ambientColor[1]=ambient._data[1];
		ll.ambientColor[2]=ambient._data[2];
		ll.ambientColor[3]=1;
		
		lights.push_back(ll);
	}
}



void initScene(int argc, char** argv)
{		
	ifstream scene;
	scene.open(argv[1]); //sample_scene

//taking background and ambient
	scene >> background >>ambient;

//taking light sources
	scene>>number_of_light_sources;
	for(int i=0; i<number_of_light_sources; i++)
	{
		light_S source;
		scene>>string_musur>>int_musur;	
		scene>>source.position;
		scene>>source.intensity;		
		light_sources.push_back(source);
	}

//taking materials
	scene>>number_of_materials;
	for(int i=0; i<number_of_materials; i++)
	{
		material m;
		scene>>string_musur>>int_musur;	
		scene>>m.ambient_r;
		scene>>m.diffuse_r;
		scene>>m.specular_r>>m.specExp;
		materials.push_back(m);
	}

//taking translations
	scene>>string_musur;
	scene>>number_of_translations;
	for(int i=0; i<number_of_translations; i++)
	{
		Vector3 t;		
		scene>>t;	
		translations.push_back(t);
	}

//taking scales
	scene>>string_musur;
	scene>>number_of_scales;
	for(int i=0; i<number_of_scales; i++)
	{
		Vector3 s;		
		scene>>s;	
		scales.push_back(s);
	}

//taking rotations
	scene>>string_musur;
	scene>>number_of_rotations;
	for(int i=0; i<number_of_rotations; i++)
	{
		rotator r;
		scene>>r.degree>>r.rotate;	
		rotations.push_back(r);
	}

//taking vertices
	scene>>number_of_vertices;

	//vertexData = new GLfloat [number_of_vertices*3];
	vertexData=(GLfloat*)malloc(sizeof(GLfloat)*number_of_vertices*3);


	vertex_normals.resize(number_of_vertices);
	vertex_normals_ortalamasiz.resize(number_of_vertices);
	scene>>string_musur;
	for (int i=0;i<number_of_vertices;i++)
	{
		Vector3 data;
		scene>>data;
		vertexData[3*i]=data._data[0];
		vertexData[3*i+1]=data._data[1];
		vertexData[3*i+2]=data._data[2];	
	}

//taking meshes
	scene>>number_of_meshes;
	
	for (int i=0;i<number_of_meshes;i++)
	{
		Mesh m;
		scene>>string_musur>>int_musur;		
		scene>>m.type>>m.material_id;
		scene>>int_musur; //number of transformations
	
		m.start_index=indexes.size();
		m.start_normals=normals.size();

		for(int j=0; j<int_musur; j++)	//taking transformations for mesh
		{	
			string type;
			int id;
			scene>>type>>id;
			m.t_type.push_back(type);
			m.t_id.push_back(id);
		}
		scene>>int_musur;	//taking number of triangles 
		m.number_of_triangles=int_musur;
		totalNumTriangles=totalNumTriangles+int_musur;

		for(int j=0; j<int_musur; j++)	//taking indexes for triangles
		{
			int index1,index2,index3;
			scene>>index1>>index2>>index3;		
			indexes.push_back(index1);
			indexes.push_back(index2);
			indexes.push_back(index3);

			Vector3 a;
			a._data[0]=vertexData[(index1-1)*3];
			a._data[1]=vertexData[(index1-1)*3+1];
			a._data[2]=vertexData[(index1-1)*3+2];

			Vector3 b;
			b._data[0]=vertexData[(index2-1)*3];
			b._data[1]=vertexData[(index2-1)*3+1];
			b._data[2]=vertexData[(index2-1)*3+2];

			Vector3 c;
			c._data[0]=vertexData[(index3-1)*3];
			c._data[1]=vertexData[(index3-1)*3+1];
			c._data[2]=vertexData[(index3-1)*3+2];

			Vector3 normal;
			normal_calculator(a,b,c,normal);
			normals.push_back(normal);

			norm n1_initial;
			norm n2_initial;
			norm n3_initial;

			n1_initial=vertex_normals_ortalamasiz[index1-1];
			n2_initial=vertex_normals_ortalamasiz[index2-1];
			n3_initial=vertex_normals_ortalamasiz[index3-1];

			if(n1_initial.kac==0)
			{
				n1_initial.normal=normal;
				n1_initial.kac=1;
			}
			else
			{
				n1_initial.normal._data[0]=n1_initial.normal._data[0]+normal._data[0];
				n1_initial.normal._data[1]=n1_initial.normal._data[1]+normal._data[1];
				n1_initial.normal._data[2]=n1_initial.normal._data[2]+normal._data[2];
				n1_initial.kac=n1_initial.kac+1;
				
			}
			vertex_normals_ortalamasiz[index1-1]=n1_initial;
			if(n2_initial.kac==0)
			{
				n2_initial.normal=normal;
				n2_initial.kac=1;
			}
			else
			{
				n2_initial.normal._data[0]=n2_initial.normal._data[0]+normal._data[0];
				n2_initial.normal._data[1]=n2_initial.normal._data[1]+normal._data[1];
				n2_initial.normal._data[2]=n2_initial.normal._data[2]+normal._data[2];
				n2_initial.kac=n2_initial.kac+1;
				
			}
			vertex_normals_ortalamasiz[index2-1]=n2_initial;

			if(n3_initial.kac==0)
			{
				n3_initial.normal=normal;
				n3_initial.kac=1;
			}
			else
			{
				n3_initial.normal._data[0]=n3_initial.normal._data[0]+normal._data[0];
				n3_initial.normal._data[1]=n3_initial.normal._data[1]+normal._data[1];
				n3_initial.normal._data[2]=n3_initial.normal._data[2]+normal._data[2];
				n3_initial.kac=n3_initial.kac+1;
				
			}
			vertex_normals_ortalamasiz[index3-1]=n3_initial;
		}	
		meshes.push_back(m);
	}
// assigning normals to vertexis
		for(int i=0; i<number_of_vertices; i++)
		{
			norm asd=vertex_normals_ortalamasiz[i];
			Vector3 ortalama;
			ortalama._data[0]=(asd.normal._data[0])/asd.kac;
			ortalama._data[1]=(asd.normal._data[1])/asd.kac;
			ortalama._data[2]=(asd.normal._data[2])/asd.kac;
		//converting to unit
float nn=sqrt((ortalama._data[0])*(ortalama._data[0])+(ortalama._data[1])*(ortalama._data[1])+(ortalama._data[2])*(ortalama._data[2]));
	
			Vector3 normalized;
			normalized._data[0]=(ortalama._data[0])/nn;
			normalized._data[1]=(ortalama._data[1])/nn;
			normalized._data[2]=(ortalama._data[2])/nn;
				
			vertex_normals[i]=normalized;
		}
//taking camera
	ifstream camerA;
	camerA.open(argv[2]); //sample_camera

	camerA>>camera.position;
	camerA>>camera.gaze;
	camerA>>camera.Up;
	camerA>>camera.left>>camera.right>>camera.bottom>>camera.top>>camera.near>>camera.far>>camera.HorRes>>camera.VerRes;

	posx=camera.position._data[0];
	posy=camera.position._data[1];
	posz=camera.position._data[2];

	gazex=camera.gaze._data[0];
	gazey=camera.gaze._data[1];
	gazez=camera.gaze._data[2];

	upx=camera.Up._data[0];
	upy=camera.Up._data[1];
	upz=camera.Up._data[2];

	float dis=sqrt(gazex*gazex + gazey*gazey + gazez*gazez);

	//gazex=gazex/dis;
	//gazey=gazey/dis;
	//gazez=gazez/dis;

	dis=sqrt(upx*upx + upy*upy + upz*upz);

	upx=upx/dis;
	upy=upy/dis;
	upz=upz/dis;
			//cx = aybz − azby = 3×7 − 4×6 = −3
			//cy = azbx − axbz = 4×5 − 2×7 = 6
			//cz = axby − aybx = 2×6 − 3×5 = −3	
//	u=v*w
	ux=upy*(-gazez) - upz*(-gazey);
	uy=upz*(-gazex) - upx*(-gazez);
	uz=upx*(-gazey) - upy*(-gazex);

	dis=sqrt(ux*ux + uy*uy + uz*uz);

	ux=ux/dis;
	uy=uy/dis;
	uz=uz/dis;

	upx=(-gazey)*uz - (-gazez)*uy;
	upy=(-gazez)*ux - (-gazex)*uz;
	upz=(-gazex)*uy - (-gazey)*ux;

	dis=sqrt(upx*upx + upy*upy + upz*upz);

	upx=upx/dis;
	upy=upy/dis;
	upz=upz/dis;


	//-gaze*l

//	cout<<"position="<<camera.position._data[0]<<" "<<camera.position._data[1]<<" "<<camera.position._data[2];
//	cout<<"	gaze="<<camera.gaze._data[0]<<" "<<camera.gaze._data[1]<<" "<<camera.gaze._data[2];
//	cout<<"	Up="<<camera.Up._data[0]<<" "<<camera.Up._data[1]<<" "<<camera.Up._data[2];
//	cout<<"	imageplane="<<camera.left<<" "<<camera.right<<" "<<camera.bottom<<" "<<camera.top<<" "<<camera.near<<" "<<camera.far<<" "<<camera.HorRes<<" "<<camera.VerRes<<endl;

////////////////////


////////////////////////////////////print	
/*	cout<<"background="<<background._data[0]<<" "<<background._data[1]<<" "<<background._data[2];
	cout<<"	ambient="<<ambient._data[0]<<" "<<ambient._data[1]<<" "<<ambient._data[2]<<endl;

	for(int i=0;i<light_sources.size(); i++)
	{
		light_S l=light_sources[i];
		cout<<"light_pos="<<l.position._data[0]<<" "<<l.position._data[1]<<" "<<l.position._data[2];
		cout<<"	intensity="<<l.intensity._data[0]<<" "<<l.intensity._data[1]<<" "<<l.intensity._data[2]<<endl;
	}
	for(int i=0;i<materials.size(); i++)
	{
		material m=materials[i];
		cout<<"Mambient="<<m.ambient_r._data[0]<<" "<<m.ambient_r._data[1]<<" "<<m.ambient_r._data[2];
		cout<<"	Mdiffuse="<<m.diffuse_r._data[0]<<" "<<m.diffuse_r._data[1]<<" "<<m.diffuse_r._data[2];
		cout<<"	Mspecular="<<m.specular_r._data[0]<<" "<<m.specular_r._data[1]<<" "<<m.specular_r._data[2];
		cout<<"	SpecExp="<<m.specExp<<endl;
	}
	for(int i=0;i<translations.size(); i++)
	{
		Vector3 l=translations[i];
		cout<<"translations="<<l._data[0]<<" "<<l._data[1]<<" "<<l._data[2]<<"	";
	}
	cout<<endl;
	for(int i=0;i<scales.size(); i++)
	{
		Vector3 l=scales[i];
		cout<<"scales="<<l._data[0]<<" "<<l._data[1]<<" "<<l._data[2]<<"		";
	}
	cout<<endl;
	for(int i=0;i<rotations.size(); i++)
	{
		rotator r=rotations[i];
		cout<<"rotations="<<r.rotate._data[0]<<" "<<r.rotate._data[1]<<" "<<r.rotate._data[2];
		cout<<"	degree="<<r.degree<<endl;
	}


	cout<<endl<<endl<<"Materials"<<endl;

for(int i=0;i<meshes.size(); i++)
{
	Mesh m=meshes[i];
	cout<<"type="<<m.type;
	cout<<"	material_id="<<m.material_id<<endl;
	cout<<"now triangles begin"<<endl;
	cout<<"start_index="<<m.start_index<<endl;
	for(int j=0;j<m.number_of_triangles;j++)
	{	
		int start_index=m.start_index;
		int index1=indexes[start_index+j*3];
		int index2=indexes[start_index+j*3+1];
		int index3=indexes[start_index+j*3+2];

		cout<<"index1="<<index1<<" index2="<<index2<<" index3="<<index3<<endl;
		
		Vector3 vertex1;
		vertex1._data[0]=vertexData[(index1-1)*3];
		vertex1._data[1]=vertexData[(index1-1)*3+1];
		vertex1._data[2]=vertexData[(index1-1)*3+2];

		Vector3 vertex2;
		vertex2._data[0]=vertexData[(index2-1)*3];
		vertex2._data[1]=vertexData[(index2-1)*3+1];
		vertex2._data[2]=vertexData[(index2-1)*3+2];

		Vector3 vertex3;
		vertex3._data[0]=vertexData[(index3-1)*3];
		vertex3._data[1]=vertexData[(index3-1)*3+1];
		vertex3._data[2]=vertexData[(index3-1)*3+2];

		cout<<"vertex1="<<vertex1._data[0]<<" "<<vertex1._data[1]<<" "<<vertex1._data[2]<<endl;
		cout<<"vertex2="<<vertex2._data[0]<<" "<<vertex2._data[1]<<" "<<vertex2._data[2]<<endl;
		cout<<"vertex3="<<vertex3._data[0]<<" "<<vertex3._data[1]<<" "<<vertex3._data[2]<<endl;

	cout<<"normals="<<normals[start_index+j]._data[0]<<" "<<normals[start_index+j]._data[1]<<" "<<normals[start_index+j]._data[2]<<endl;

	cout<<endl<<endl;
	}
}

	cout<<endl<<endl<<"vertexData"<<endl;
	for(int i=0; i<number_of_vertices; i++)
		cout<<vertexData[i*3]<<" "<<vertexData[i*3+1]<<" "<<vertexData[i*3+2]<<" "<<endl;
		
	
	cout<<endl<<endl<<"normals"<<endl;
	for(int i=0; i<number_of_vertices; i++)
	{
		Vector3 n=vertex_normals_ortalamasiz[i].normal;
		int kac=vertex_normals_ortalamasiz[i].kac;
		Vector3 nn=vertex_normals[i];
//		cout<<"normals_ortalamasiz="<<n._data[0]<<" "<<n._data[1]<<" "<<n._data[2]<<" kac="<<kac<<endl;
		cout<<"normals="<<nn._data[0]<<" "<<nn._data[1]<<" "<<nn._data[2]<<endl;
	}

*/

	kopyalayci();
/*
	cout<<"indices"<<endl;
	for(int i=0; i<indexes.size()/3; i++)
	{
		cout<<"r="<<indexes[i*3]<<" "<<indexes[i*3+1]<<" "<<indexes[i*3+2]<<" "<<endl;
		cout<<"p="<<indices[i*3]+1<<" "<<indices[i*3+1]+1<<" "<<indices[i*3+2]+1<<" "<<endl;
	}

	cout<<"normals"<<endl;
	for(int i=0; i<vertex_normals.size(); i++)
	{
		cout<<"r="<<vertex_normals[i]._data[0]<<" "<<vertex_normals[i]._data[1]<<" "<<vertex_normals[i]._data[2]<<" "<<endl;
		cout<<"p="<<normalData[i*3]<<" "<<normalData[i*3+1]<<" "<<normalData[i*3+2]<<" "<<endl;
	}

	cout<<"light"<<endl;
	for(int i=0; i<light_sources.size(); i++)
	{
		cout<<"r"<<endl;
cout<<"position="<<light_sources[i].position._data[0]<<" "<<light_sources[i].position._data[1]<<" "<<light_sources[i].position._data[2]<<endl;
cout<<"intensity="<<light_sources[i].intensity._data[0]<<" "<<light_sources[i].intensity._data[1]<<" "<<light_sources[i].intensity._data[2]<<endl;
cout<<"ambient="<<ambient._data[0]<<" "<<ambient._data[1]<<" "<<ambient._data[2]<<endl;	


		cout<<"p"<<endl;
cout<<"position="<<lights[i].lightPos[0]<<" "<<lights[i].lightPos[1]<<" "<<lights[i].lightPos[2]<<endl;
cout<<"intensity="<<lights[i].lightColor[0]<<" "<<lights[i].lightColor[1]<<" "<<lights[i].lightColor[2]<<endl;
cout<<"ambient="<<lights[i].ambientColor[0]<<" "<<lights[i].ambientColor[1]<<" "<<lights[i].ambientColor[2]<<endl;	

	}
*/	

}
/////////////////////////////////////


void init() 
{
	glEnable(GL_DEPTH_TEST);
	glShadeModel(GL_SMOOTH);
    	glEnable(GL_NORMALIZE);/////////

}

void drawCubeElementsVBO(int start, int kac, int material_id)
{
	static bool firstTime = true;

	static int vertexPosDataSizeInBytes;
	
	if (firstTime)
	{		

		int initial=0x4000;
		glLightModeli(GL_LIGHT_MODEL_LOCAL_VIEWER,GL_TRUE);
		glEnable(GL_LIGHTING);

		for(int j=0;j<lights.size();j++)
			glEnable(initial+j);
	
		for(int j=0;j<lights.size();j++)
		{
			glLightfv (initial+j, GL_AMBIENT, lights[j].ambientColor);
			glLightfv (initial+j, GL_DIFFUSE, lights[j].lightColor);
			glLightfv (initial+j, GL_SPECULAR, lights[j].lightColor);
		}

		for(int j=0;j<lights.size();j++)
		{
			glLightfv(initial+j,GL_POSITION,lights[j].lightPos);
		}
		
		firstTime = false;

		glEnableClientState(GL_VERTEX_ARRAY);
		glEnableClientState(GL_NORMAL_ARRAY);

		GLuint vertexAttribBuffer, indexBuffer;

		glGenBuffers(1, &vertexAttribBuffer);
		glGenBuffers(1, &indexBuffer);

		assert(vertexAttribBuffer > 0 && indexBuffer > 0);

		glBindBuffer(GL_ARRAY_BUFFER, vertexAttribBuffer);
		glBindBuffer(GL_ELEMENT_ARRAY_BUFFER, indexBuffer);


		vertexPosDataSizeInBytes = (sizeof(GLfloat ) *number_of_vertices*3);
		int indexDataSizeInBytes = (sizeof(GLuint ) *totalNumTriangles*3);
		int normalDataSizeInBytes=(sizeof(GLfloat ) *number_of_vertices*3);////////

		
		glBufferData(GL_ARRAY_BUFFER, vertexPosDataSizeInBytes+vertexPosDataSizeInBytes, 0, GL_STATIC_DRAW);
		glBufferSubData(GL_ARRAY_BUFFER, 0, vertexPosDataSizeInBytes, vertexData);
		glBufferSubData(GL_ARRAY_BUFFER, vertexPosDataSizeInBytes, normalDataSizeInBytes, normalData);

		glBufferData(GL_ELEMENT_ARRAY_BUFFER, indexDataSizeInBytes, indices, GL_STATIC_DRAW);

	//	cout<<"#triangles="<<totalNumTriangles<<endl;
	}

	glVertexPointer(3, GL_FLOAT, 0, 0);
	glNormalPointer(GL_FLOAT, 0, (void*)vertexPosDataSizeInBytes);


	material mat=materials[material_id-1];

	GLfloat ambColor[4] = {mat.ambient_r._data[0],mat.ambient_r._data[1],mat.ambient_r._data[2],1.0};
	GLfloat diffColor[4] = {mat.diffuse_r._data[0],mat.diffuse_r._data[1],mat.diffuse_r._data[2],1.0};
	GLfloat specColor[4] = {mat.specular_r._data[0],mat.specular_r._data[1],mat.specular_r._data[2],1.0};
	GLfloat specExp[1]={mat.specExp};
	glMaterialfv(GL_FRONT,GL_AMBIENT,ambColor);
	glMaterialfv(GL_FRONT,GL_DIFFUSE,diffColor);
	glMaterialfv(GL_FRONT,GL_SPECULAR,specColor);
	glMaterialfv(GL_FRONT,GL_SHININESS,specExp);

	glDrawElements(GL_TRIANGLES, kac, GL_UNSIGNED_INT, (void *)(sizeof(GLuint)*start));
}


void display()
{
	glClearColor(background._data[0], background._data[1], background._data[2], 1);
	glClearDepth(1.0f);
	glClearStencil(0);
	glClear(GL_COLOR_BUFFER_BIT | GL_DEPTH_BUFFER_BIT | GL_STENCIL_BUFFER_BIT);

//gluLookAt(camera.position._data[0],camera.position._data[1],camera.position._data[2],		//camera position
//camera.position._data[0]+camera.gaze._data[0],camera.position._data[1]+camera.gaze._data[1],camera.position._data[2]+camera.gaze._data[2],
//camera.Up._data[0],camera.Up._data[1],camera.Up._data[2]);		//camera up vector


for(int i=0; i<number_of_meshes; i++)
{

	glLoadIdentity();
	int initial=0x4000;



		gluLookAt(posx,posy,posz,		//camera position
		posx+gazex,posy+gazey,posz+gazez,
		upx,upy,upz);		//camera up vector


	for(int j=0;j<lights.size();j++)
	{
		glLightfv(initial+j,GL_POSITION,lights[j].lightPos);
	}

	Mesh mesh=meshes[i];

	if(mesh.type == 1)
	{ 
		glPolygonMode(GL_FRONT_AND_BACK, GL_FILL);
	}
	if(mesh.type == 2)
	{
		glPolygonMode(GL_FRONT_AND_BACK, GL_LINE);
	}

	for(int m=mesh.t_type.size()-1;m>=0; m--)
	{
		if(mesh.t_type[m] == "t")
		{
			Vector3 t=translations[mesh.t_id[m]-1];
			glTranslatef(t._data[0] ,t._data[1] ,t._data[2]);
		}
		else if(mesh.t_type[m] == "r")
		{
			rotator r=rotations[mesh.t_id[m]-1];
			glRotatef(r.degree, r.rotate._data[0], r.rotate._data[1], r.rotate._data[2]);
		}
		else if(mesh.t_type[m] == "s")
		{
			Vector3 s=scales[mesh.t_id[m]-1];
			glScalef(s._data[0], s._data[1], s._data[2]);
		}
	}
//	cout<<"<<<<<>>>>>>>>>>"<<endl;
//	cout<<"number_of_mesh="<<meshes.size()<<endl;
//	cout<<"start_index="<<mesh.start_index<<endl;
//	cout<<"number_of_triangles="<<mesh.number_of_triangles<<endl;
//	cout<<"<<<<<>>>>>>>>>>"<<endl;

		int start_index=mesh.start_index;
		int number_of_indices=mesh.number_of_triangles*3;
			drawCubeElementsVBO(start_index,number_of_indices, mesh.material_id);

}

	glutSwapBuffers();

}



void reshape(int w, int h)   // Create The Reshape Function (the viewport)
{
	
    w = w < 1 ? 1 : w;
    h = h < 1 ? 1 : h;

    gWidth = w;
    gHeight = h;

    glViewport(0, 0, w, h);

// Initialize camera
	glMatrixMode(GL_PROJECTION);	// Switch  to projection matrix
    glLoadIdentity();

	glFrustum(camera.left,camera.right,camera.bottom,camera.top,camera.near,camera.far);

	glMatrixMode(GL_MODELVIEW);
	glLoadIdentity();

}


void keyboard(unsigned char key, int x, int y)
{
   switch(key) 
    {
		case 'w':
		{
			posx=posx+gazex*0.05;
			posy=posy+gazey*0.05;
			posz=posz+gazez*0.05;
			break;
		}
		case 's':
		{
			posx=posx-gazex*0.05;
			posy=posy-gazey*0.05;
			posz=posz-gazez*0.05;
			break;
		}
		case 'a':
		{

			Vector3 finito;
			Vector3 axis;
			axis._data[0]=0;	axis._data[1]=1;	axis._data[2]=0;
			float degree=0.5;
			Vector3 gaze;

			gaze._data[0]=gazex;	gaze._data[1]=gazey;	gaze._data[2]=gazez;
			rotasyon2(axis,gaze,finito,degree);

			gazex=finito._data[0];
			gazey=finito._data[1];
			gazez=finito._data[2];

			gaze._data[0]=upx;	gaze._data[1]=upy;	gaze._data[2]=upz;

			rotasyon2(axis,gaze,finito,degree);
			upx=finito._data[0];
			upy=finito._data[1];
			upz=finito._data[2];


			ux=upy*(-gazez) - upz*(-gazey);
			uy=upz*(-gazex) - upx*(-gazez);
			uz=upx*(-gazey) - upy*(-gazex);

			float dis=sqrt(ux*ux + uy*uy + uz*uz);

			ux=ux/dis;
			uy=uy/dis;
			uz=uz/dis;

			break;
		}
		case 'd':
		{

			Vector3 finito;
			Vector3 axis;
			axis._data[0]=0;	axis._data[1]=1;	axis._data[2]=0;
			float degree=-0.5;
			Vector3 gaze;

			gaze._data[0]=gazex;	gaze._data[1]=gazey;	gaze._data[2]=gazez;
			rotasyon2(axis,gaze,finito,degree);

			gazex=finito._data[0];
			gazey=finito._data[1];
			gazez=finito._data[2];

			gaze._data[0]=upx;	gaze._data[1]=upy;	gaze._data[2]=upz;

			rotasyon2(axis,gaze,finito,degree);
			upx=finito._data[0];
			upy=finito._data[1];
			upz=finito._data[2];


			ux=upy*(-gazez) - upz*(-gazey);
			uy=upz*(-gazex) - upx*(-gazez);
			uz=upx*(-gazey) - upy*(-gazex);

			float dis=sqrt(ux*ux + uy*uy + uz*uz);

			ux=ux/dis;
			uy=uy/dis;
			uz=uz/dis;

			break;break;
		}
		case 'u':
		{


//			cout<<"u="<<ux<<" "<<uy<<" "<<uz<<endl;
//			cout<<"up="<<upx<<" "<<upy<<" "<<upz<<endl;
//			cout<<"gaze="<<gazex<<" "<<gazey<<" "<<gazez<<endl;

			Vector3 finito,finito2;
			Vector3 axis;
			axis._data[0]=ux;	axis._data[1]=uy;	axis._data[2]=uz;
			axis.normalize();

			float degree=0.5;

			Vector3 gaze;
			gaze._data[0]=gazex;	gaze._data[1]=gazey;	gaze._data[2]=gazez;
			Vector3 up;
			up._data[0]=upx;		up._data[1]=upy;		up._data[2]=upz;


			gaze.normalize();
			rotasyon2(axis,gaze,finito,degree);
			rotasyon2(axis,up,finito2,degree);

			gazex=finito._data[0];
			gazey=finito._data[1];
			gazez=finito._data[2];
			upx=finito2._data[0];
			upy=finito2._data[1];
			upz=finito2._data[2];



			ux=upy*(-gazez) - upz*(-gazey);
			uy=upz*(-gazex) - upx*(-gazez);
			uz=upx*(-gazey) - upy*(-gazex);

			float dis=sqrt(ux*ux + uy*uy + uz*uz);

			ux=ux/dis;
			uy=uy/dis;
			uz=uz/dis;


//			cout<<"u="<<ux<<" "<<uy<<" "<<uz<<endl;
//			cout<<"up="<<upx<<" "<<upy<<" "<<upz<<endl;
//			cout<<"gaze="<<gazex<<" "<<gazey<<" "<<gazez<<endl;

break;
		}
		
		case 'j':
		{


//			cout<<"u="<<ux<<" "<<uy<<" "<<uz<<endl;
//			cout<<"up="<<upx<<" "<<upy<<" "<<upz<<endl;
//			cout<<"gaze="<<gazex<<" "<<gazey<<" "<<gazez<<endl;

			Vector3 finito,finito2;
			Vector3 axis;
			axis._data[0]=ux;	axis._data[1]=uy;	axis._data[2]=uz;
			axis.normalize();

			float degree=-0.5;

			Vector3 gaze;
			gaze._data[0]=gazex;	gaze._data[1]=gazey;	gaze._data[2]=gazez;
			Vector3 up;
			up._data[0]=upx;		up._data[1]=upy;		up._data[2]=upz;


			gaze.normalize();
			rotasyon2(axis,gaze,finito,degree);
			rotasyon2(axis,up,finito2,degree);

			gazex=finito._data[0];
			gazey=finito._data[1];
			gazez=finito._data[2];
			upx=finito2._data[0];
			upy=finito2._data[1];
			upz=finito2._data[2];

			ux=upy*(-gazez) - upz*(-gazey);
			uy=upz*(-gazex) - upx*(-gazez);
			uz=upx*(-gazey) - upy*(-gazex);

			float dis=sqrt(ux*ux + uy*uy + uz*uz);

			ux=ux/dis;
			uy=uy/dis;
			uz=uz/dis;

			break;
		}
		

		case 27:     // Escape
			exit(0); 
			break; 
		default:  
			break;
    }

}

void idle()
{
    glutPostRedisplay();
}



 void* simpleFunc(void*) { return NULL; } void forcePThreadLink() { pthread_t t1; pthread_create(&t1, NULL, &simpleFunc, NULL); }


int main(int argc, char** argv)   // Create Main Function For Bringing It All Together
{
	initScene(argc, argv);
	
	glutInit(&argc, argv);
	glutInitDisplayMode(GLUT_RGBA | GLUT_DOUBLE | GLUT_DEPTH | GLUT_STENCIL);
	glutInitWindowPosition(100, 100);
	glutInitWindowSize(camera.HorRes, camera.VerRes);
	glutCreateWindow("");
	glewInit();


	init();

	glutDisplayFunc(display);
    glutReshapeFunc(reshape);
    glutKeyboardFunc(keyboard);
    glutIdleFunc(idle);

    glutMainLoop();

    return 0;
}
