#include "Scene.h"
#include "Camera.h"
#include <math.h> 
#include <algorithm> 
#include <climits>

float dot_product3(Vector3 v1, Vector3 v2)
{
	//a · b = ax × bx + ay × by + az × bz
	float ret=v1._data[0]*v2._data[0] + v1._data[1]*v2._data[1] + v1._data[2]*v2._data[2];
	return ret; 
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
			
			
			vector<Mesh> meshler=CurrentScene->getMeshes();
			int mesh_size=meshler.size();
			for(int ii=0; ii<mesh_size; ii++)
			{
				int triangle_num=meshler[ii].triangles.size();
				for(int iii=0; iii<triangle_num; iii++)
				{
					if(meshler[ii].triangles[iii].Intersect(ray, hitInfo_trianglez))
					{
					
						if (hitInfo_trianglez.Parameter>0.001  &&  hitInfo_trianglez.Parameter<tmax)
						{
							kac_tri++;
							if(kac_tri==1)
							{
								t_tri=hitInfo_trianglez.Parameter;			
							}
						
							if(t_tri>=hitInfo_trianglez.Parameter)
							{
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
					hitinfo.Material=hitInfo_triangle.Material;
					hitinfo.Position=hitInfo_triangle.Position;
					hitinfo.Normal=hitInfo_triangle.Normal;
					hitinfo.Parameter=hitInfo_triangle.Parameter;
				}
				else
				{
					hitinfo.Material=hitInfo_sphere.Material;
					hitinfo.Position=hitInfo_sphere.Position;
					hitinfo.Normal=hitInfo_sphere.Normal;
					hitinfo.Parameter=hitInfo_sphere.Parameter;
				}
			}
			else if(did_tri) //hit triangle
			{
				hitinfo.Material=hitInfo_triangle.Material;
					hitinfo.Position=hitInfo_triangle.Position;
					hitinfo.Normal=hitInfo_triangle.Normal;
					hitinfo.Parameter=hitInfo_triangle.Parameter;
			}
			else if(did_sp) //hit sphere
			{
					hitinfo.Material=hitInfo_sphere.Material;
					hitinfo.Position=hitInfo_sphere.Position;
					hitinfo.Normal=hitInfo_sphere.Normal;
					hitinfo.Parameter=hitInfo_sphere.Parameter;
			}
			
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

				float coso = max((float)0, dot_product3(normal_unit,direction));			

				float RR=(kd_diffuse.R() * coso * radianceR);
				float GG=(kd_diffuse.G() * coso * radianceG);
				float BB=(kd_diffuse.B() * coso * radianceB);		
				
			//	Color initial;
			//	initial=ret.Pixel(i, j);
			//	float RRR=RR+initial.R();
			//	float GGG=GG+initial.G();
			//	float BBB=BB+initial.B();

			//	Color diffuse_initial(RRR,GGG,BBB);				

			//	ret.Pixel(i, j)=diffuse_initial;


				ret_R=ret_R+RR;
				ret_G=ret_G+GG;
				ret_B=ret_B+BB;


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
