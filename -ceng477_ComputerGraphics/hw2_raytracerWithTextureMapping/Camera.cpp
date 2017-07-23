#include "Scene.h"
#include "Camera.h"
#include <math.h> 
#include <algorithm> 
#include <climits>
#define PI 3.14159265
#include "aoa_jpeg.c"
#include "aoa_jpeg.h"
#define EPSILON 0.000001

	vector<UCOLOR**> texturesU;
	vector<int*> textureSizes;

	int triangleId;
	float gama,beta;
	Vector3 x_s,y_s,z_s;

	int sid;

void four_multiplierR(float T[4][4], float P[4], float (&P_p)[4])
{
	P_p[0]=(T[0][0]*P[0]) + (T[0][1]*P[1]) + (T[0][2]*P[2]) + (T[0][3]*P[3]);
	P_p[1]=(T[1][0]*P[0]) + (T[1][1]*P[1]) + (T[1][2]*P[2]) + (T[1][3]*P[3]);
	P_p[2]=(T[2][0]*P[0]) + (T[2][1]*P[1]) + (T[2][2]*P[2]) + (T[2][3]*P[3]);
	P_p[3]=(T[3][0]*P[0]) + (T[3][1]*P[1]) + (T[3][2]*P[2]) + (T[3][3]*P[3]);
}


void four_by_four_multiplierR(float T[4][4], float P[4][4], float (&P_p)[4][4])
{
	P_p[0][0]=(T[0][0]*P[0][0]) + (T[0][1]*P[1][0]) + (T[0][2]*P[2][0]) + (T[0][3]*P[3][0]);
	P_p[0][1]=(T[0][0]*P[0][1]) + (T[0][1]*P[1][1]) + (T[0][2]*P[2][1]) + (T[0][3]*P[3][1]);
	P_p[0][2]=(T[0][0]*P[0][2]) + (T[0][1]*P[1][2]) + (T[0][2]*P[2][2]) + (T[0][3]*P[3][2]);
	P_p[0][3]=(T[0][0]*P[0][3]) + (T[0][1]*P[1][3]) + (T[0][2]*P[2][3]) + (T[0][3]*P[3][3]);

	P_p[1][0]=(T[1][0]*P[0][0]) + (T[1][1]*P[1][0]) + (T[1][2]*P[2][0]) + (T[1][3]*P[3][0]);
	P_p[1][1]=(T[1][0]*P[0][1]) + (T[1][1]*P[1][1]) + (T[1][2]*P[2][1]) + (T[1][3]*P[3][1]);
	P_p[1][2]=(T[1][0]*P[0][2]) + (T[1][1]*P[1][2]) + (T[1][2]*P[2][2]) + (T[1][3]*P[3][2]);
	P_p[1][3]=(T[1][0]*P[0][3]) + (T[1][1]*P[1][3]) + (T[1][2]*P[2][3]) + (T[1][3]*P[3][3]);


	P_p[2][0]=(T[2][0]*P[0][0]) + (T[2][1]*P[1][0]) + (T[2][2]*P[2][0]) + (T[2][3]*P[3][0]);
	P_p[2][1]=(T[2][0]*P[0][1]) + (T[2][1]*P[1][1]) + (T[2][2]*P[2][1]) + (T[2][3]*P[3][1]);
	P_p[2][2]=(T[2][0]*P[0][2]) + (T[2][1]*P[1][2]) + (T[2][2]*P[2][2]) + (T[2][3]*P[3][2]);
	P_p[2][3]=(T[2][0]*P[0][3]) + (T[2][1]*P[1][3]) + (T[2][2]*P[2][3]) + (T[2][3]*P[3][3]);

	P_p[3][0]=(T[3][0]*P[0][0]) + (T[3][1]*P[1][0]) + (T[3][2]*P[2][0]) + (T[3][3]*P[3][0]);
	P_p[3][1]=(T[3][0]*P[0][1]) + (T[3][1]*P[1][1]) + (T[3][2]*P[2][1]) + (T[3][3]*P[3][1]);
	P_p[3][2]=(T[3][0]*P[0][2]) + (T[3][1]*P[1][2]) + (T[3][2]*P[2][2]) + (T[3][3]*P[3][2]);
	P_p[3][3]=(T[3][0]*P[0][3]) + (T[3][1]*P[1][3]) + (T[3][2]*P[2][3]) + (T[3][3]*P[3][3]);
}




void translator2(Vector3 translation, float P[4], float (&P_p)[3])
{
	float T[4][4];

	T[0][0]=1;	T[0][1]=0;	T[0][2]=0;	T[0][3]=translation._data[0];
	T[1][0]=0;	T[1][1]=1;	T[1][2]=0;	T[1][3]=translation._data[1];
	T[2][0]=0;	T[2][1]=0;	T[2][2]=1;	T[2][3]=translation._data[2];
	T[3][0]=0;	T[3][1]=0;	T[3][2]=0;	T[3][3]=1;
	float C_p[4];
			
	four_multiplierR(T,P,C_p);

	P_p[0]=C_p[0];	P_p[1]=C_p[1];	P_p[2]=C_p[2];
	
}



	

float dot_product3(Vector3 v1, Vector3 v2)
{
	//a · b = ax × bx + ay × by + az × bz
	float ret=v1._data[0]*v2._data[0] + v1._data[1]*v2._data[1] + v1._data[2]*v2._data[2];
	return ret; 
}

void four(float T[3][3], float P[3], float (&P_p)[3])
{
	P_p[0]=(T[0][0]*P[0]) + (T[0][1]*P[1]) + (T[0][2]*P[2]);
	P_p[1]=(T[1][0]*P[0]) + (T[1][1]*P[1]) + (T[1][2]*P[2]);
	P_p[2]=(T[2][0]*P[0]) + (T[2][1]*P[1]) + (T[2][2]*P[2]);
}

Camera::Camera(Vector3 position,Vector3 gaze,string outputFile,Vector3 F,Vector3 U,Vector3 L, float Left1, float Right1, float Bottom1, float Top1, float Distance1, int Width1, int Height1, int c_id)
{
	this->_position=position;
	this->camera_id=c_id;
	this->_gaze=gaze;
	_space.Forward=F;
	_space.Up=U;
	_space.Left=L;

	_imagePlane.Left=Left1;
	_imagePlane.Right=Right1;
	_imagePlane.Bottom=Bottom1;
	_imagePlane.Top=Top1;
	_imagePlane.Distance=Distance1;
	_imagePlane.Width=Width1;
	_imagePlane.Height=Height1;

	_outputFile=outputFile;
		
}



double* initial(int i,int id)
{
	double *ret= new double[2];
	
	//cout<<textureSizes[id][0]<< " "<<textureSizes[id][1]<<endl; 
	
	if(i==0)
	{

		ret[0]=(textureSizes[id][0])/2.0; //olmasi gereken
		ret[1] = 0*(textureSizes[id][1])/3.0; //olmasi gereken

//		ret[0]=1*(textureSizes[id][0])/2.0; //left icin
//		ret[1] = 1*(textureSizes[id][1])/3.0; //left icin

//		ret[0]=1*(textureSizes[id][0])/2.0; //back icin
//		ret[1] = 2*(textureSizes[id][1])/3.0; //back icin



	}
	else if(i==2)
	{

//		ret[0]=0*(textureSizes[id][0])/2.0;
//		ret[1] = 0*(textureSizes[id][1])/3.0;

		ret[0]=1*(textureSizes[id][0])/2.0;
		ret[1] = 1*(textureSizes[id][1])/3.0;



	}

	else if(i==1)
	{
		ret[0] = (textureSizes[id][0])/2.0;	//olmasi gereken
		ret[1] = 0;						//olmasi gereken

//		ret[0]=0*(textureSizes[id][0])/2.0;	//left icin
//		ret[1] = 1*(textureSizes[id][1])/3.0;	//left icin

//		ret[0]=0*(textureSizes[id][0])/2.0;	//back icin
//		ret[1] = 2*(textureSizes[id][1])/3.0;	//back icin


	
	}
	

	else if(i==3)
	{
//		ret[0] = 0*(textureSizes[id][0])/2.0; //eski
//		ret[1] = 0*(textureSizes[id][1])/3.0;	//eski

		ret[0]=0*(textureSizes[id][0])/2.0;
		ret[1] = 1*(textureSizes[id][1])/3.0;

	
	}



	else if(i==4)
	{

//		ret[0]=0*(textureSizes[id][0])/2.0;
//		ret[1] = 1*(textureSizes[id][1])/3.0;


		ret[0]=1*(textureSizes[id][0])/2.0; //back icin
		ret[1] = 2*(textureSizes[id][1])/3.0; //back icin



		
	}
	else if(i==5)
	{
		//ret[0] = 0*(textureSizes[id][0])/2.0;
		//ret[1] = 1*(textureSizes[id][1])/3.0;
	
		ret[0]=0*(textureSizes[id][0])/2.0;	//back icin
		ret[1] = 2*(textureSizes[id][1])/3.0;	//back icin


	}


	 else if(i==6)
	{
		ret[0]=1*(textureSizes[id][0])/2.0;
		ret[1] = 1*(textureSizes[id][1])/3.0;


//		ret[0] = 0.5*(textureSizes[id][0]);
//		ret[1] = (textureSizes[id][1])/3.0;
	
	}
	else if(i==8)
	{
//		ret[0]=0*(textureSizes[id][0])/2.0;
//		ret[1] = 2*(textureSizes[id][1])/3.0;




		ret[0]=0*(textureSizes[id][0])/2.0; //olmasi gereken
		ret[1] = 2*(textureSizes[id][1])/3.0; //olmasi gereken



	}
	
	
		
	
	
	else if(i==10)
	{
		ret[0]=1*(textureSizes[id][0])/2.0;
		ret[1] = 2*(textureSizes[id][1])/3.0;

//		ret[0]= (textureSizes[id][0]);
//		ret[1]= (textureSizes[id][1])/3.0;
	}


	
	else if(i==7)
	{
		ret[0] = (textureSizes[id][0])/2.0;
		ret[1] = (textureSizes[id][1])/3.0;
	
	}



	else if(i==9)
	{
//		ret[0] = 0*(textureSizes[id][0])/2.0;
//		ret[1] = 2*(textureSizes[id][1])/3.0;


		ret[0] = 0*(textureSizes[id][0])/2.0;	//olmasi gereken
		ret[1] = 2*(textureSizes[id][1])/3.0;

	
	}



	else if(i==11)
	{
		ret[0] = (textureSizes[id][0])/2.0;
		ret[1] = 2*(textureSizes[id][1])/3.0;
	
	}


	
	return ret;
}



float determinant2(float mat[3][3])
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



void inverser(float A[3][3],float (&A_i)[3][3])
{

	float cof[3][3];
	cof[0][0]=A[1][1]*A[2][2]-A[1][2]*A[2][1]; 		cof[0][1]=-(A[1][0]*A[2][2]-A[1][2]*A[2][0]);		cof[0][2]=A[1][0]*A[2][1]-A[1][1]*A[2][0];
	cof[1][0]=-(A[0][1]*A[2][2]-A[0][2]*A[2][1]); 		cof[1][1]=A[0][0]*A[2][2]-A[0][2]*A[2][0];		cof[1][2]=-(A[0][0]*A[2][1]-A[0][1]*A[2][0]);
	cof[2][0]=A[0][1]*A[1][2]-A[0][2]*A[1][1]; 		cof[2][1]=-(A[0][0]*A[1][2]-A[0][2]*A[1][0]);		cof[2][2]=A[0][0]*A[1][1]-A[0][1]*A[1][0];

	float det=determinant2(A); 

	A_i[0][0]=(cof[0][0])/det;	A_i[0][1]=(cof[0][1])/det;	A_i[0][2]=(cof[0][2])/det;
	A_i[1][0]=(cof[1][0])/det;	A_i[1][1]=(cof[1][1])/det;	A_i[1][2]=(cof[1][2])/det;
	A_i[2][0]=(cof[2][0])/det;	A_i[2][1]=(cof[2][1])/det;	A_i[2][2]=(cof[2][2])/det;

}





Color Camera::for_reflect(Ray ray,int count) const
{

			float ret_R=0;
			float ret_G=0;
			float ret_B=0;
			Vector3 direct=ray.Direction();

///////////////////////////////////////////////////////////////////////////////////////////////////edit
			RayHitInfo hitInfo_trianglez;
			RayHitInfo hitInfo_spherez;
			RayHitInfo hitInfo_triangle;
			RayHitInfo hitInfo_sphere;
			bool did_tri=false;	
			bool did_sp=false;
			
			int kac_tri=0;
			int kac_sp=0;
			float t_tri;
			float t_sp;
			float tmax=INT_MAX;
			
			float var_par=INT_MAX;
			


			vector<Mesh> meshler=CurrentScene->getMeshes();
			int mesh_size=meshler.size();
			for(int ii=0; ii<mesh_size; ii++)
			{
				int triangle_num=meshler[ii].triangles.size();
				for(int iii=0; iii<triangle_num; iii++)
				{
					if(meshler[ii].triangles[iii].Intersect(ray, hitInfo_trianglez))
					{
					
				if (hitInfo_trianglez.Parameter>0.001  &&  hitInfo_trianglez.Parameter<tmax && hitInfo_trianglez.Parameter<var_par)
						{
							kac_tri++;
							if(kac_tri==1)
							{
								t_tri=hitInfo_trianglez.Parameter;			
							}
						
							if(t_tri>=hitInfo_trianglez.Parameter && hitInfo_trianglez.Parameter<var_par)
							{
								hitInfo_triangle.tex_id=hitInfo_trianglez.tex_id;
								hitInfo_triangle.indicator=hitInfo_trianglez.indicator;
								hitInfo_triangle.center=hitInfo_trianglez.center;
								triangleId=iii;
								gama=hitInfo_trianglez.gama;
								beta=hitInfo_trianglez.beta;
								var_par=hitInfo_trianglez.Parameter;
								
								
								hitInfo_triangle.Parameter=hitInfo_trianglez.Parameter;
								hitInfo_triangle.Material=hitInfo_trianglez.Material;
								hitInfo_triangle.Position=hitInfo_trianglez.Position;
								hitInfo_triangle.Normal=hitInfo_trianglez.Normal;
							}
						
							did_tri=true;
				//			Color b(110,110,110);
				//			ret.Pixel(i, j)=b;
						}
					}
				}


}








			
			vector<Sphere> sphereler=CurrentScene->getSpheres();
			int sphere_size=sphereler.size();
			for(int jj=0; jj<sphere_size; jj++)
			{
				RayHitInfo hitInfo;
				if(sphereler[jj].Intersect(ray, hitInfo_spherez))
				{
				
					if (hitInfo_spherez.Parameter>0.001 &&  hitInfo_spherez.Parameter<tmax)
					{
						kac_sp++;
						if(kac_sp==1)
						{
							t_sp=hitInfo_spherez.Parameter;			
						}
						if(t_sp>=hitInfo_spherez.Parameter)
						{
							hitInfo_sphere.tex_id=hitInfo_spherez.tex_id;
							hitInfo_sphere.indicator=hitInfo_spherez.indicator;
							hitInfo_sphere.center=hitInfo_spherez.center;
							x_s=hitInfo_spherez.x_s;
							y_s=hitInfo_spherez.y_s;
							z_s=hitInfo_spherez.z_s;
		
							sid=jj;			
				
	//		cout<<"aaaa="<<hitInfo_spherez.tex_id<<endl;

							hitInfo_sphere.Parameter=hitInfo_spherez.Parameter;
							hitInfo_sphere.Material=hitInfo_spherez.Material;
							hitInfo_sphere.Position=hitInfo_spherez.Position;
							hitInfo_sphere.Normal=hitInfo_spherez.Normal;
						}
	
						did_sp=true;
				//		Color b(200,200,200);
				//		ret.Pixel(i, j)=b;
					}
				}

			}
			

			
		

			RayHitInfo hitinfo;
			if(did_tri && did_sp) //hit both of them
			{
				if(hitInfo_triangle.Parameter < hitInfo_sphere.Parameter)
				{

					hitinfo.tex_id=hitInfo_triangle.tex_id;
					hitinfo.indicator=hitInfo_triangle.indicator;
					hitinfo.center=hitInfo_triangle.center;
				//cout<<"center5= "<<hitinfo.center._data[0]<<" "<<hitinfo.center._data[1]<<" "<<hitinfo.center._data[2]<<endl;

					
					hitinfo.Material=hitInfo_triangle.Material;
					hitinfo.Position=hitInfo_triangle.Position;
					hitinfo.Normal=hitInfo_triangle.Normal;
					hitinfo.Parameter=hitInfo_triangle.Parameter;
				}
				else
				{

					hitinfo.indicator=hitInfo_sphere.indicator;
					hitinfo.tex_id=hitInfo_sphere.tex_id;
					hitinfo.center=hitInfo_sphere.center;


					hitinfo.Material=hitInfo_sphere.Material;
					hitinfo.Position=hitInfo_sphere.Position;
					hitinfo.Normal=hitInfo_sphere.Normal;
					hitinfo.Parameter=hitInfo_sphere.Parameter;
				}
			}
			else if(did_tri) //hit triangle
			{


				hitinfo.indicator=hitInfo_triangle.indicator;
				hitinfo.tex_id=hitInfo_triangle.tex_id;
				hitinfo.center=hitInfo_triangle.center;

				hitinfo.Material=hitInfo_triangle.Material;
					hitinfo.Position=hitInfo_triangle.Position;
					hitinfo.Normal=hitInfo_triangle.Normal;
					hitinfo.Parameter=hitInfo_triangle.Parameter;
			}
			else if(did_sp) //hit sphere
			{

					hitinfo.tex_id=hitInfo_sphere.tex_id;
					hitinfo.indicator=hitInfo_sphere.indicator;
					hitinfo.center=hitInfo_sphere.center;
				//cout<<"center4= "<<hitinfo.center._data[0]<<" "<<hitinfo.center._data[1]<<" "<<hitinfo.center._data[2]<<endl;
				//cout<<"center6= "<<hitInfo_sphere.center._data[0]<<" "<<hitInfo_sphere.center._data[1]<<" "<<hitInfo_sphere.center._data[2]<<endl;


					hitinfo.Material=hitInfo_sphere.Material;
					hitinfo.Position=hitInfo_sphere.Position;
					hitinfo.Normal=hitInfo_sphere.Normal;
					hitinfo.Parameter=hitInfo_sphere.Parameter;
			}
			//	cout<<"center3= "<<hitinfo.center._data[0]<<" "<<hitinfo.center._data[1]<<" "<<hitinfo.center._data[2]<<endl;

	
			
			if(did_tri || did_sp) //boyama islemi
			{
				//int MID;	Color Ambient;		Color Diffuse;		Color Reflectance;	Specular specular;	
			///////////////ambient
				Color kd_ambient=(CurrentScene->get_materials())[hitinfo.Material - 1].Ambient;
			Color ambient((((CurrentScene->getAmbient()).R()) * kd_ambient.R()),(((CurrentScene->getAmbient()).G()) * kd_ambient.G()),(((CurrentScene->getAmbient()).B()) * kd_ambient.B()));
				
				ret_R=ret_R+ambient.R();
				ret_G=ret_G+ambient.G();
				ret_B=ret_B+ambient.B();
			////////////////end_ambient

			Color kd_diffuse=(CurrentScene->get_materials())[hitinfo.Material - 1].Diffuse;
			Color ks=(CurrentScene->get_materials())[hitinfo.Material - 1].specular.RGB;
			float exp=(CurrentScene->get_materials())[hitinfo.Material - 1].specular.SpecExp;
			Color kd_reflectance=(CurrentScene->get_materials())[hitinfo.Material - 1].Reflectance;

			Vector3 normal=hitinfo.Normal;
			float n_dis=sqrt(normal._data[0]*normal._data[0] + normal._data[1]*normal._data[1] + normal._data[2]*normal._data[2]);
				
			Vector3 normal_unit;
			normal_unit._data[0]=(normal._data[0])/n_dis;
			normal_unit._data[1]=(normal._data[1])/n_dis;
			normal_unit._data[2]=(normal._data[2])/n_dis;
				
			Vector3 wo_unit; //unit vector from position to camera
			float denominator=(sqrt(direct._data[0]*direct._data[0] + direct._data[1]*direct._data[1] + direct._data[2]*direct._data[2]));			
			float x_d=(direct._data[0])/denominator;						
			float y_d=(direct._data[1])/denominator;						
			float z_d=(direct._data[2])/denominator;						
					
			wo_unit._data[0]=-x_d;					
			wo_unit._data[1]=-y_d;
			wo_unit._data[2]=-z_d;

			int num_of_light=(CurrentScene->get_lights()).size();
			for(int l=0; l<num_of_light; l++)
			{
			
				Color I_light_intensity=(CurrentScene->get_lights())[l].RGB;
				Vector3 pos_light=(CurrentScene->get_lights())[l].XYZ;			

				Vector3 wi; //from light to position
				wi._data[0]=pos_light._data[0] - hitinfo.Position._data[0];
				wi._data[1]=pos_light._data[1] - hitinfo.Position._data[1];
				wi._data[2]=pos_light._data[2] - hitinfo.Position._data[2];
				double wi_distance=sqrt(wi._data[0]*wi._data[0] + wi._data[1]*wi._data[1] + wi._data[2]*wi._data[2] );

				double att = 1.0/(4*3.1415*wi_distance*wi_distance); 	
 				if (att>1.0) 
 					att=1.0;				


				Vector3 wi_unit; //unit vector from light to position
				wi_unit._data[0]=(wi._data[0])/wi_distance;
				wi_unit._data[1]=(wi._data[1])/wi_distance;
				wi_unit._data[2]=(wi._data[2])/wi_distance;
			
			
				float radianceR=I_light_intensity.R() * att;
				float radianceG=I_light_intensity.G() * att;
				float radianceB=I_light_intensity.B() * att;


				//cout<<"radiance R="<<radianceR<<endl;
				//cout<<"radiance G="<<radianceG<<endl;
				//cout<<"radiance B="<<radianceB<<endl;


//////////////////////////////////////////////////////////////////////////

				Vector3 direction;
				direction=wi;
			
				
				double MT=sqrt(direction._data[0]*direction._data[0]+direction._data[1]*direction._data[1]+direction._data[2]*direction._data[2]);

				direction._data[0] = (direction._data[0])/MT ;
				direction._data[1] = (direction._data[1])/MT ;
				direction._data[2] = (direction._data[2])/MT ;

				Ray ray_l(hitinfo.Position,direction);		
				
				bool did_tri2=false;
				bool did_sp2=false;
				RayHitInfo hitInfo_triangle2;
				RayHitInfo hitInfo_sphere2;
			
				for(int m=0; m<mesh_size; m++)
				{
					int shadow_triangle_num=meshler[m].triangles.size();
					for(int mm=0; mm<shadow_triangle_num; mm++)
					{
						if(meshler[m].triangles[mm].Intersect(ray_l, hitInfo_triangle2))
						{
							if (hitInfo_triangle2.Parameter>0.001 && hitInfo_triangle2.Parameter<=MT)
							{	
								did_tri2=true;
								break;
							}	
						}
					}
					if(did_tri2)
						break;
				}
			
				for(int ss=0; ss<sphere_size; ss++)
				{
					if(sphereler[ss].Intersect(ray_l, hitInfo_sphere2))
					{
						if (hitInfo_sphere2.Parameter>0.001 && hitInfo_sphere2.Parameter<=MT)
						{
							did_sp2=true;
							break;
						}				
					}
				}
				
				if(did_sp2 || did_tri2)
					continue;


///////////////////////////////////////////////diffuse




				//cout<<gama<<endl;

				if(hitinfo.tex_id!=0)
				{
				
					if(hitinfo.indicator=="s")
					{
			
						Vector3 poss;
						poss=hitinfo.Position;	
						
						Vector3 poss_i;
						poss_i=hitinfo.Position;	
						Vector3 center_i;
						center_i=hitinfo.center;
						




						for(int i=CurrentScene->rot_tran[sid].size()-1; i>=0; i--)
						{
							transformation rt;
							rt.id=CurrentScene->rot_tran[sid][i].id;
							rt.type=CurrentScene->rot_tran[sid][i].type;
							
							if(rt.type=="t")
							{



								Vector3 translation=CurrentScene->translations[rt.id-1];
								translation._data[0]=-translation._data[0];
								translation._data[1]=-translation._data[1];
								translation._data[2]=-translation._data[2];


								float P[4]={center_i._data[0],center_i._data[1],center_i._data[2],1};
								float scenter[3];
								translator2(translation,P,scenter);

								float sposs[3];
								float x[4]={poss_i._data[0], poss_i._data[1], poss_i._data[2], 1}; 
								translator2(translation,x,sposs);

								center_i._data[0]=scenter[0];
								center_i._data[1]=scenter[1];
								center_i._data[2]=scenter[2];

								poss_i._data[0]=sposs[0];
								poss_i._data[1]=sposs[1];
								poss_i._data[2]=sposs[2];


							}
							else
							{


								rotator rot=CurrentScene->Rotates[rt.id-1];

								Vector3 vv= rot.rotate;	
								float d=rot.degree;

							vv._data[0]=-vv._data[0];
							vv._data[1]=-vv._data[1];
							vv._data[2]=-vv._data[2];




								float dis=sqrt(vv._data[0]*vv._data[0] + vv._data[1]*vv._data[1] + vv._data[2]*vv._data[2]);
								Vector3 u;
								u._data[0]=vv._data[0]/dis; u._data[1]=vv._data[1]/dis; u._data[2]=vv._data[2]/dis;
					
								Vector3 v;
								if(u._data[0] <= u._data[1] && u._data[0] <= u._data[2] ) //a is smallest					
								{
									v._data[0]=0; 
									v._data[1]=-u._data[2];
									v._data[2]=u._data[1];
								}
								else if(u._data[1] <= u._data[0] && u._data[1] <= u._data[2]) // b is smallest
								{
									v._data[0]=-u._data[2]; 
									v._data[1]=0;
									v._data[2]=u._data[0];
								}
								else // c is smallest
								{
									v._data[0]=-u._data[1]; 
									v._data[1]=u._data[0];
									v._data[2]=0;
								}
								Vector3 w;
					//cx = aybz − azby = 3×7 − 4×6 = −3
					//cy = azbx − axbz = 4×5 − 2×7 = 6
					//cz = axby − aybx = 2×6 − 3×5 = −3
								w._data[0]=(u._data[1])*(v._data[2]) - (u._data[2])*(v._data[1]);  
								w._data[1]=(u._data[2])*(v._data[0]) - (u._data[0])*(v._data[2]);  
								w._data[2]=(u._data[0])*(v._data[1]) - (u._data[1])*(v._data[0]);  


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
								R_x[1][0]=0;	R_x[1][1]=cos(d*PI/180);		R_x[1][2]=-sin(d*PI/180);	R_x[1][3]=0;
								R_x[2][0]=0;	R_x[2][1]=sin(d*PI/180);		R_x[2][2]=cos(d*PI/180);		R_x[2][3]=0;
								R_x[3][0]=0;	R_x[3][1]=0;				R_x[3][2]=0;				R_x[3][3]=1;




								float P[4]={center_i._data[0],center_i._data[1],center_i._data[2],1};
								float C_p[4];
								float tmp[4][4];
								four_by_four_multiplierR(M_R_I,R_x,tmp);
								float tmp2[4][4];
								four_by_four_multiplierR(tmp,M_R,tmp2);
								four_multiplierR(tmp2,P,C_p);

								float X_p[4]; float x[4]={poss_i._data[0],poss_i._data[1],poss_i._data[2],1}; 
								four_multiplierR(tmp2,x,X_p);

				

						center_i._data[0]=C_p[0];	center_i._data[1]=C_p[1];	center_i._data[2]=C_p[2];
						poss_i._data[0]=X_p[0];		poss_i._data[1]=X_p[1];		poss_i._data[2]=X_p[2];


							}						

						}





Vector3 center_back;
						
if(CurrentScene->rot_varmi[sid])
{
			
		poss._data[0]=poss_i._data[0];	poss._data[1]=poss_i._data[1];	poss._data[2]=poss_i._data[2];

		center_back=hitinfo.center;

		hitinfo.center._data[0]=0;
		hitinfo.center._data[1]=0;
		hitinfo.center._data[2]=0;
//cout<<poss._data[0]<<" "<<poss._data[1]<<" "<<poss._data[2]<<endl;
}			

float aa=sqrt((poss._data[1]-hitinfo.center._data[1])*(poss._data[1]-hitinfo.center._data[1]) +  (poss._data[0]-hitinfo.center._data[0])*(poss._data[0]-hitinfo.center._data[0]) + (poss._data[2]-hitinfo.center._data[2])*(poss._data[2]-hitinfo.center._data[2])  );
		
						float teta;
						teta=acos((poss._data[1]-hitinfo.center._data[1])/aa);
						float gama=-atan2((poss._data[2]-hitinfo.center._data[2]),(poss._data[0]-hitinfo.center._data[0]));
						gama=gama+PI;

		
						if(teta<0)
							teta=teta+PI;
						if(gama<0)
							gama=gama+2*PI;
		
						float u=gama/(2*PI);
						float v=(teta)/PI;
		
						int j=round(u*textureSizes[hitinfo.tex_id-1][1]);		
						int i=round(v*textureSizes[hitinfo.tex_id-1][0]);		




							if(j>=textureSizes[hitinfo.tex_id-1][1] )
								j=textureSizes[hitinfo.tex_id-1][1]-1;
							if(i>=textureSizes[hitinfo.tex_id-1][0] )
								i=textureSizes[hitinfo.tex_id-1][0]-1;

		
						float R=texturesU[hitinfo.tex_id-1][i][j][0];
						float G=texturesU[hitinfo.tex_id-1][i][j][1];
						float B=texturesU[hitinfo.tex_id-1][i][j][2];
		
						Color tempColor(R,G,B);
		
					
					
					
						float coso = max((float)0, dot_product3(normal_unit,direction));			
		
						float RR=(kd_diffuse.R() * coso * tempColor.R());
						float GG=(kd_diffuse.G() * coso * tempColor.G());
						float BB=(kd_diffuse.B() * coso * tempColor.B());		
								
						ret_R=ret_R+RR;
						ret_G=ret_G+GG;
						ret_B=ret_B+BB;
if(CurrentScene->rot_varmi[sid])
	hitinfo.center=center_back;

					}
					else
					{
					

						

						Vector3 poss;
						poss=hitinfo.Position;		
					
						double* uv = new double[2];

						double *sP = initial(triangleId,hitinfo.tex_id-1) ;
	
			
						Color tempColor;
					


						if( triangleId==2 || triangleId==4 || triangleId==6 || triangleId==8 || triangleId==0 || triangleId==10)
						{
							float alfa=1-beta-gama;


							uv[0]=sP[0] + (1-alfa)*((textureSizes[hitinfo.tex_id-1][0])/2.0);
							uv[1]=sP[1] + (gama)*((textureSizes[hitinfo.tex_id-1][1])/3.0); 



							if(triangleId==2 || triangleId==4)
							{
								uv[0]=sP[0] -alfa*((textureSizes[hitinfo.tex_id-1][0])/2.0);
								uv[1]=sP[1] - (gama)*((textureSizes[hitinfo.tex_id-1][1])/3.0);


							}
							else if(triangleId==8)
							{
								uv[0]=sP[0] + (alfa)*((textureSizes[hitinfo.tex_id-1][0])/2.0);
								uv[1]=sP[1] + (gama)*((textureSizes[hitinfo.tex_id-1][1])/3.0);

							}



							int i=round(uv[0] ); //height
						 	int j=round(uv[1] );

							if(j>=textureSizes[hitinfo.tex_id-1][1] )
								j=textureSizes[hitinfo.tex_id-1][1]-1;
							if(i>=textureSizes[hitinfo.tex_id-1][0] )
								i=textureSizes[hitinfo.tex_id-1][0]-1;
							
							
								
							float R,G,B;
					
								 R=texturesU[hitinfo.tex_id-1][i][j][0];
								 G=texturesU[hitinfo.tex_id-1][i][j][1];
								 B=texturesU[hitinfo.tex_id-1][i][j][2];
							
							
							
							Color fa(R,G,B);
							tempColor=fa;	
							
		
						}
			
			





						
						else if(triangleId==1 || triangleId==3 || triangleId==5 || triangleId==7 || triangleId==9 || triangleId==11)
						{
						
							float alfa=1-beta-gama;
							uv[0]=sP[0] + (beta)*((textureSizes[hitinfo.tex_id-1][0])/2);
				
						uv[1]=sP[1] + (gama)*((textureSizes[hitinfo.tex_id-1][1])/3)+ ((textureSizes[hitinfo.tex_id-1][1])/3)*beta ;
				
							if(triangleId==3 || triangleId==5)
							{
								uv[0]=sP[0] + (beta)*((textureSizes[hitinfo.tex_id-1][0])/2);
				
								uv[1]=sP[1] - (gama)*((textureSizes[hitinfo.tex_id-1][1])/3)- ((textureSizes[hitinfo.tex_id-1][1])/3)*(beta) ;

							}
							else if(triangleId==9)
							{
								uv[0]=sP[0] + (1-beta)*((textureSizes[hitinfo.tex_id-1][0])/2);
				
						uv[1]=sP[1] + (gama)*((textureSizes[hitinfo.tex_id-1][1])/3)+ ((textureSizes[hitinfo.tex_id-1][1])/3)*beta ;
				
							}
						

							int i=round(uv[0] ); //height
						 	int j=round(uv[1] );

							if(j>=textureSizes[hitinfo.tex_id-1][1] )
								j=textureSizes[hitinfo.tex_id-1][1]-1;
							if(i>=textureSizes[hitinfo.tex_id-1][0] )
								i=textureSizes[hitinfo.tex_id-1][0]-1;
							


							float R=texturesU[hitinfo.tex_id-1][i][j][0];
							float G=texturesU[hitinfo.tex_id-1][i][j][1];
							float B=texturesU[hitinfo.tex_id-1][i][j][2];
				
							Color fa(R,G,B);
							tempColor=fa;	
		
						}
						
									
						float coso = max((float)0, dot_product3(normal_unit,direction));			
		
						float RR=(kd_diffuse.R() * coso * tempColor.R());
						float GG=(kd_diffuse.G() * coso * tempColor.G());
						float BB=(kd_diffuse.B() * coso * tempColor.B());		
								
						ret_R=ret_R+RR;
						ret_G=ret_G+GG;
						ret_B=ret_B+BB;


					}
					
					
				}
				else
				{

					float coso = max((float)0, dot_product3(normal_unit,direction));			
	
					float RR=(kd_diffuse.R() * coso * radianceR);
					float GG=(kd_diffuse.G() * coso * radianceG);
					float BB=(kd_diffuse.B() * coso * radianceB);		
								

					ret_R=ret_R+RR;
					ret_G=ret_G+GG;
					ret_B=ret_B+BB;
				}


////////////////////////////////////////////////end_diffuse
			
/////////////////////////////////// specular shading

				Vector3 h;
				h._data[0]=wi_unit._data[0] + wo_unit._data[0];
				h._data[1]=wi_unit._data[1] + wo_unit._data[1];
				h._data[2]=wi_unit._data[2] + wo_unit._data[2];
			
				float den_h=sqrt(h._data[0] * h._data[0] + h._data[1] * h._data[1] + h._data[2] * h._data[2]);
				Vector3 h_unit;
				h_unit._data[0]=(h._data[0]) / den_h ;
				h_unit._data[1]=(h._data[1]) / den_h ;
				h_unit._data[2]=(h._data[2]) / den_h ;

				float cosa=max(float(0.0), dot_product3(normal_unit, h_unit));

 					// printf ("7 ^ 3 = %f\n", pow (7.0, 3.0) 
				float RS=(ks.R() * pow(cosa,exp) * radianceR);
				float GS=(ks.G() * pow(cosa,exp) * radianceG);
				float BS=(ks.B() * pow(cosa,exp) * radianceB);
					
//				Color initial2;
//				initial2=ret.Pixel(i, j);
//				float RRR2=RS+initial2.R();
//				float GGG2=GS+initial2.G();
//				float BBB2=BS+initial2.B();

//				Color diffuse_initial_spec(RRR2,GGG2,BBB2);				
//				ret.Pixel(i, j)=diffuse_initial_spec;
				
				ret_R=ret_R+RS;
				ret_G=ret_G+GS;
				ret_B=ret_B+BS;
				
///////////////////////specular _shading_end
//////////////////////Reflectance_mirror	
			
				
			
//////////////////////Reflectance_mirror_end	

			
			}


			if (kd_reflectance.R() != 0 || kd_reflectance.G() != 0 || kd_reflectance.B() != 0 )
				{
				
				
					Vector3 wr;
					wr._data[0]=-wo_unit._data[0] + 2*normal_unit._data[0]*(dot_product3(normal_unit,wo_unit));
					wr._data[1]=-wo_unit._data[1] + 2*normal_unit._data[1]*(dot_product3(normal_unit,wo_unit));
					wr._data[2]=-wo_unit._data[2] + 2*normal_unit._data[2]*(dot_product3(normal_unit,wo_unit));
		

					float wr_den = sqrt(wr._data[0]*wr._data[0] + wr._data[1]*wr._data[1] + wr._data[2]*wr._data[2]);
					Vector3 wr_unit;
					wr_unit._data[0]=wr._data[0] / wr_den;
					wr_unit._data[1]=wr._data[1] / wr_den;
					wr_unit._data[2]=wr._data[2] / wr_den;



					Ray reflectRay(hitinfo.Position, wr_unit);

					Color dol(0,0,0);
					Color R;
					if (count>CurrentScene->get_rayReflect()) 
						return dol;
					else
					{
		
            					R = for_reflect(reflectRay,count+1);
		
					}
					
					
				
					
					ret_R+=R.R()*kd_reflectance.R();
					ret_G+=R.G()*kd_reflectance.G();
					ret_B+=R.B()*kd_reflectance.B();
				}











		Color ret(ret_R,ret_G,ret_B);
		return ret;



			}
			else
			{
				Color a(0,0,0);  
				a=CurrentScene->getBackground();	
				//ret.Pixel(i, j)=a;
				return a;
			
			}
			
			
///////////////////////////////////////////////////////////////////////////////////////////////////end_edit




}





Image Camera::Render() const
{


	Image ret(_imagePlane.Width,_imagePlane.Height);

////////////////////////////////////////////////////////////////////////////// asagina bakmadim
int width=_imagePlane.Width;
int height=_imagePlane.Height;


		for(int te=0;te<CurrentScene->textures.size();te++)
				{
				string text=CurrentScene->textures[te];
				
			char* Tname = new char(text.length() + 1);
			for(int k = 0 ; k < text.length() ; k++) Tname[k] = text[k]; 
			Tname[text.length()] = '\0'; 
		
			int *x = new int[1];
			int *y = new int[1];
			read_jpeg_header(Tname, x, y);
		
			UCOLOR** new_image ;

			new_image = new UCOLOR*[*y];
			for(int t = 0 ; t < *y ; t++)
				new_image[t] = new UCOLOR[*x];

			read_jpeg(Tname, new_image, x, y);
		
			int *sizes = new int[2];
			sizes[1] = *x; sizes[0] = *y;
		
		//	cout<<"nn="<<sizes[0]<<" "<<sizes[1]<<endl;
			textureSizes.push_back(sizes);
			texturesU.push_back(new_image);
				
				
				}




	for(int i=0; i<height; i++)
	{
		for(int j=0; j<width; j++)
		{
			/////////////////////////////////////////////Compute ray
			Vector3 m;
			m._data[0]=_position._data[0] + ((_gaze._data[0])*(_imagePlane.Distance));
			m._data[1]=_position._data[1] + ((_gaze._data[1])*(_imagePlane.Distance));
			m._data[2]=_position._data[2] + ((_gaze._data[2])*(_imagePlane.Distance));
			
			Vector3 q;
			q._data[0]=m._data[0] + (_imagePlane.Left * _space.Left._data[0]) + (_imagePlane.Top * _space.Up._data[0]);
			q._data[1]=m._data[1] + (_imagePlane.Left * _space.Left._data[1]) + (_imagePlane.Top * _space.Up._data[1]);
			q._data[2]=m._data[2] + (_imagePlane.Left * _space.Left._data[2]) + (_imagePlane.Top * _space.Up._data[2]);

			//s u = (r – l)(i + 0.5)/n x
			//s v = (t – b)(j + 0.5)/n y
			
			float s_u=((_imagePlane.Right - _imagePlane.Left)*(j + 0.5))/width;
			float s_v=((_imagePlane.Top - _imagePlane.Bottom)*(i + 0.5))/height;
			
			Vector3 s;
			s._data[0]=q._data[0] + (s_u*(_space.Left._data[0])) - (s_v*(_space.Up._data[0])); 			
			s._data[1]=q._data[1] + (s_u*(_space.Left._data[1])) - (s_v*(_space.Up._data[1])); 			
			s._data[2]=q._data[2] + (s_u*(_space.Left._data[2])) - (s_v*(_space.Up._data[2])); 			
		
			Vector3 direct;
			direct._data[0]=s._data[0] - _position._data[0];
			direct._data[1]=s._data[1] - _position._data[1];
			direct._data[2]=s._data[2] - _position._data[2];

			//direct._data[0]=(direct._data[0])/ (sqrt(direct._data[0]*direct._data[0]+direct._data[1]*direct._data[1]+direct._data[2]*direct._data[2])); 
			//direct._data[1]=(direct._data[1])/ (sqrt(direct._data[0]*direct._data[0]+direct._data[1]*direct._data[1]+direct._data[2]*direct._data[2])); 
			//direct._data[2]=(direct._data[2])/ (sqrt(direct._data[0]*direct._data[0]+direct._data[1]*direct._data[1]+direct._data[2]*direct._data[2])); 

		
			Ray ray(_position,direct);
			////////////////////////////////////////////
			
			Color initial;
			initial=ret.Pixel(i, j);
			
			Color after=for_reflect(ray,0);
			
			float RRR=after.R()+initial.R();
			float GGG=after.G()+initial.G();
			float BBB=after.B()+initial.B();

			Color after_initial(RRR,GGG,BBB);				

			ret.Pixel(i, j)=after_initial;
			
		
			
			
				
		}
	}    	
    	

		return ret;
	}
