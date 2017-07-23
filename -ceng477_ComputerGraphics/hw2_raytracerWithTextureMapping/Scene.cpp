#include "Scene.h"
#include "Vector3.h"
#define PI 3.14159265
#include<iostream>
//#include "aoa_jpeg.c"
//#include "aoa_jpeg.h"


using namespace std;
void four_multiplier(float T[4][4], float P[4], float (&P_p)[4])
{
	P_p[0]=(T[0][0]*P[0]) + (T[0][1]*P[1]) + (T[0][2]*P[2]) + (T[0][3]*P[3]);
	P_p[1]=(T[1][0]*P[0]) + (T[1][1]*P[1]) + (T[1][2]*P[2]) + (T[1][3]*P[3]);
	P_p[2]=(T[2][0]*P[0]) + (T[2][1]*P[1]) + (T[2][2]*P[2]) + (T[2][3]*P[3]);
	P_p[3]=(T[3][0]*P[0]) + (T[3][1]*P[1]) + (T[3][2]*P[2]) + (T[3][3]*P[3]);
}
void four_by_four_multiplier(float T[4][4], float P[4][4], float (&P_p)[4][4])
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
void translator(Vector3 translation, float P[4], float (&P_p)[3])
{
	float T[4][4];

	T[0][0]=1;	T[0][1]=0;	T[0][2]=0;	T[0][3]=translation._data[0];
	T[1][0]=0;	T[1][1]=1;	T[1][2]=0;	T[1][3]=translation._data[1];
	T[2][0]=0;	T[2][1]=0;	T[2][2]=1;	T[2][3]=translation._data[2];
	T[3][0]=0;	T[3][1]=0;	T[3][2]=0;	T[3][3]=1;
	float C_p[4];
			
	four_multiplier(T,P,C_p);

	P_p[0]=C_p[0];	P_p[1]=C_p[1];	P_p[2]=C_p[2];
	
}

void translator_tri(Vector3 translation, Vector3 a, Vector3 &a1)
{
	float T[4][4];

	T[0][0]=1;	T[0][1]=0;	T[0][2]=0;	T[0][3]=translation._data[0];
	T[1][0]=0;	T[1][1]=1;	T[1][2]=0;	T[1][3]=translation._data[1];
	T[2][0]=0;	T[2][1]=0;	T[2][2]=1;	T[2][3]=translation._data[2];
	T[3][0]=0;	T[3][1]=0;	T[3][2]=0;	T[3][3]=1;

//	cout<<"fark="<<translation._data[0]<<" "<<translation._data[1]<<" "<<translation._data[2]<<endl;


	float X_p[4]; float x[4]={a._data[0],a._data[1],a._data[2],1}; 
	four_multiplier(T,x,X_p);

//	cout<<"deep="<<X_p[0]<<" "<<X_p[1]<<" "<<X_p[2]<<endl;

	a1._data[0]=X_p[0];	a1._data[1]=X_p[1];	a1._data[2]=X_p[2];
}


void two_diff(Vector3 a, Vector3 o, Vector3 &ao, Vector3 &oa)
{
	oa._data[0]=a._data[0]-o._data[0];		
	oa._data[1]=a._data[1]-o._data[1];		
	oa._data[2]=a._data[2]-o._data[2];		

	ao._data[0]=-(a._data[0]-o._data[0]);		
	ao._data[1]=-(a._data[1]-o._data[1]);		
	ao._data[2]=-(a._data[2]-o._data[2]);		


}

void tri_scale(Vector3 scalee,Vector3 a1, Vector3 &a11)
{
	float SC[4][4];
	SC[0][0]=scalee._data[0];		SC[0][1]=0;					SC[0][2]=0;			SC[0][3]=0;
 	SC[1][0]=0;					SC[1][1]=scalee._data[1];		SC[1][2]=0;			SC[1][3]=0;
 	SC[2][0]=0;					SC[2][1]=0;					SC[2][2]=scalee._data[2];SC[2][3]=0;
  	SC[3][0]=0;					SC[3][1]=0;					SC[3][2]=0;			SC[3][3]=1;
 

	float a1_p[4]; float a1_r[4]={a1._data[0],a1._data[1],a1._data[2],1}; 
	four_multiplier(SC,a1_r,a1_p);

	a11._data[0]=a1_p[0];
	a11._data[1]=a1_p[1];
	a11._data[2]=a1_p[2];

}


vector<Mesh> Scene::getMeshes()
{
	return (this->_meshes);
}
void Scene::add_meshes(Mesh mesh)
{
//	cout<<"ay girdi"<<endl;
	_meshes.push_back(mesh);
}

void Scene::add_spheres(Sphere s)
{

//	cout<<"zaaazz"<<endl;
	_spheres.push_back(s);
}
vector<Sphere> Scene::getSpheres()
{
	return (this->_spheres);
}


vector<Material> Scene::get_materials()
{
	return this->_materials;
}
Vertex Scene::getVertex(int id)
{
	return this->_vertices[id];
}


vector<PointLight> Scene::get_lights()
{
	return this->_lights;
}



void Scene::set_rayReflect(int rayReflect)
{
	this->_rayReflect=rayReflect;
}
	
int Scene::get_rayReflect()
{
	return this->_rayReflect;
}

void Scene::setBackground(Color* Background_Color)
{
	this->_background=Background_Color;
}

Color Scene::getBackground()
{
	return this->_background;
}
void Scene::setAmbient(Color* Ambient_Color)
{
	this->_ambient=Ambient_Color;
}
Color Scene::getAmbient()
{
	return this->_ambient;
}


void Scene::addPoint_Light(PointLight _light)
{
	this->_lights.push_back(_light);
}

void Scene::seePoint_Light()
{
	for(int i=0; i<this->_lights.size(); i++)
	{
cout<<"R="<<this->_lights[i].RGB.R()<<"G="<<this->_lights[i].RGB.G()<<"B="<<this->_lights[i].RGB.B()<<"X="<<this->_lights[i].XYZ._data[0]<<"Y="<<this->_lights[i].XYZ._data[1]<<"Z="<<this->_lights[i].XYZ._data[2]<<endl;
	}
		
}
void Scene::add_Materials(Material M)
{
	this->_materials.push_back(M);
}
	
void Scene::seeMaterials()
{
	for(int i=0; i<this->_materials.size(); i++)
	{
		cout<<this->_materials[i].MID<<endl;
		cout<<this->_materials[i].Ambient.R()<<" "<<this->_materials[i].Ambient.G()<<" "<<this->_materials[i].Ambient.B()<<endl;
		cout<<this->_materials[i].Diffuse.R()<<" "<<this->_materials[i].Diffuse.G()<<" "<<this->_materials[i].Diffuse.B()<<endl;
		cout<<this->_materials[i].specular.RGB.R()<<" "<<this->_materials[i].specular.RGB.G()<<" "<<this->_materials[i].specular.RGB.B()<<" "<<this->_materials[i].specular.SpecExp<<endl;
		cout<<this->_materials[i].Reflectance.R()<<" "<<this->_materials[i].Reflectance.G()<<" "<<this->_materials[i].Reflectance.B()<<endl;

	}
}
void Scene::add_cameras(Camera camera)
{
	_cameras.push_back(camera);
}
void Scene::see_cameras()
{
	int num=this->_cameras.size();
	for(int i=0; i<num; i++)
	{


		cout<<"cid="<<this->_cameras[i].camera_id<<"position="<<this->_cameras[i]._position._data[0]<<","<<this->_cameras[i]._position._data[1]<<","<<this->_cameras[i]._position._data[2]<<endl;
		cout<<"gaze="<<_cameras[i]._gaze._data[0]<<","<<_cameras[i]._gaze._data[1]<<","<<_cameras[i]._gaze._data[2]<<endl;
		cout<<"image_plane="<<"L="<<_cameras[i]._imagePlane.Left<<" R"<<_cameras[i]._imagePlane.Right<<" B"<<_cameras[i]._imagePlane.Bottom<<" T="<<_cameras[i]._imagePlane.Top<<" D="<<_cameras[i]._imagePlane.Distance<<" W="<<_cameras[i]._imagePlane.Width<<" H="<<_cameras[i]._imagePlane.Height<<endl;

		cout<<"_space="<<"F="<<_cameras[i]._space.Forward._data[0]<<","<<_cameras[i]._space.Forward._data[1]<<","<<_cameras[i]._space.Forward._data[2]<<" U="<<_cameras[i]._space.Up._data[0]<<","<<_cameras[i]._space.Up._data[1]<<","<<_cameras[i]._space.Up._data[2]<<" L="<<_cameras[i]._space.Left._data[0]<<","<<_cameras[i]._space.Left._data[1]<<","<<_cameras[i]._space.Left._data[2]<<endl;

		    
	}

}

void Scene::add_texture(string text)
{
	this->textures.push_back(text);
}
		
void Scene::see_texture()
{
	for(int i=0;i<this->textures.size(); i++)	
		cout<<"texture_names="<<this->textures[i]<<endl;
}









Scene* CurrentScene;

void ReadScene(int argc, char** argv)
{

	float RGB[3];
	float XYZ[3];
	
	Scene* Current_Scene = new Scene();

	ifstream scene;
	scene.open(argv[1]); //sample_scene

///////////////////////////////////////////////////////////////////////////////////////////////////////
	int Ray_Reflection_Count; 
	scene >> Ray_Reflection_Count;
	Current_Scene->set_rayReflect(Ray_Reflection_Count);
//	cout<<"rayReflect_Count = "<<Current_Scene->get_rayReflect()<<endl;

///////////////////////////////////////////////////////////////////////////////////////////////////////
Color* Background_Color = new Color();
	scene >> *Background_Color;
	Current_Scene->setBackground(Background_Color);
//	cout<<"Background_Color ="<<Current_Scene->getBackground().R()<<" "<<Current_Scene->getBackground().G()<<" "<<Current_Scene->getBackground().B()<<endl;

//////////////////////////////////////////////////////////////////////////////////////////////////////////////
	Color* Ambient_Light = new Color();
	scene >> *Ambient_Light;
	Current_Scene->setAmbient(Ambient_Light);
//	cout<<"Ambient_Color ="<<Current_Scene->getAmbient().R()<<" "<<Current_Scene->getAmbient().G()<<" "<<Current_Scene->getAmbient().B()<<endl;


//////////////////////////////////////////////////////////////////////////////////////////////////////////////
	int Point_Light_Count;
	scene >> Point_Light_Count;
//	cout<<"Point_Light_Count"<< Point_Light_Count<<endl;
		for(int i=0; i<Point_Light_Count; i++)
		{
			Color light;
			Vector3 pos;
			
			scene >> pos;
			scene >> light;
			PointLight Plight;
			Plight.RGB=light;
			Plight.XYZ=pos;
			Current_Scene->addPoint_Light(Plight);
		}
//	Current_Scene->seePoint_Light();
/////////////////////////////////////////////////////////////////////////////////////////////////////////////////
	int Material_count;
	scene >>Material_count;  
//	cout<<"Material_count = "<<Material_count<<endl;
		for(int i=0; i<Material_count; i++)
		{

			Material M;
			
			Color ambient;
			Color diffuse;
			Color rgb;
			Color reflectance;
			string material;
			int mid;
			float specEX;
			
				
			scene>>material>>mid;
			scene>>ambient;
			scene>>diffuse;
			scene>>rgb>>specEX;
			scene>>reflectance;
			
			M.MID=mid;
			M.Ambient=ambient;
			M.Diffuse=diffuse;
			M.specular.RGB=rgb;
			M.specular.SpecExp=specEX;
			M.Reflectance=reflectance;
			
			Current_Scene->add_Materials(M);
		}

//		Current_Scene->seeMaterials();
/////////////////////////////////////////////////////////////////////////////////////////////////
		int texture_count;
		string text;
		scene >>text;
		scene >>texture_count;		

//		cout<<"text="<<text<<endl;
//		cout<<"texture_count="<<texture_count<<endl;

		for(int i=0;i<texture_count; i++)
		{
			scene>>text;
			Current_Scene->add_texture(text);

		
		
		
		/*	char* Tname = new char(text.length() + 1);
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
		
			cout<<"nn="<<sizes[0]<<" "<<sizes[1]<<endl;
			Current_Scene->textureSizes.push_back(sizes);
			Current_Scene->texturesU.push_back(new_image);
*/
		}
//		Current_Scene->see_texture();
////////////////////////////////////////////////////////////////////////////////////////////////
		string translation;
		int translation_count;
		scene>>translation;
		scene>>translation_count;

//		cout<<"string="<<translation<<endl;
//		cout<<"count="<<translation_count<<endl;
		Vector3 trans;
		Current_Scene->trans_count=translation_count;
		for(int i=0;i<translation_count; i++)
		{
			scene>>trans;
			Current_Scene->translations.push_back(trans);
		}

		for(int i=0;i<translation_count; i++)
		{
			Vector3 t=Current_Scene->translations[i];

	//	cout<<"trans="<<t._data[0]<<" "<<t._data[1]<<" "<<t._data[2]<<endl;
		}

/////////////////////////////////////////////////////////////////////////////////////////
		string Scaling;
		int Scaling_count;
		scene>>Scaling;
		scene>>Scaling_count;

		//cout<<endl<<"string="<<Scaling<<endl;
		//cout<<"count="<<Scaling_count<<endl;
		Vector3 Scale;
		Current_Scene->Scale_count=Scaling_count;
		for(int i=0;i<Scaling_count; i++)
		{
			scene>>Scale;
			Current_Scene->Scales.push_back(Scale);
		}

		for(int i=0;i<Scaling_count; i++)
		{
			Vector3 t=Current_Scene->Scales[i];

		//cout<<"Scales="<<t._data[0]<<" "<<t._data[1]<<" "<<t._data[2]<<endl;
		}
/////////////////////////////////////////////////////////////////////////////////////////



		string Rotation;
		int Rotation_count;
		scene>>Rotation;
		scene>>Rotation_count;

		//cout<<endl<<"string="<<Rotation<<endl;
		//cout<<"count="<<Rotation_count<<endl;
		Vector3 Rotate;
		float a_degree;
		Current_Scene->Rotate_count=Rotation_count;
		for(int i=0;i<Rotation_count; i++)
		{
			scene>>a_degree;
			scene>>Rotate;
			rotator ob;
			ob.rotate=Rotate;
			ob.degree=a_degree;			
			Current_Scene->Rotates.push_back(ob);
		}

		for(int i=0;i<Rotation_count; i++)
		{
			Vector3 t=Current_Scene->Rotates[i].rotate;
			float deg=Current_Scene->Rotates[i].degree;
		//	cout<<"deg="<<deg<<endl;		
		//	cout<<"Rotates="<<t._data[0]<<" "<<t._data[1]<<" "<<t._data[2]<<endl<<endl;
		}
//////////////////////////////////////////////////////////////////////////////////

		int Model_Count;
		scene>>Model_Count;

//		cout<<"Model_Count="<<Model_Count<<endl;

	for(int i=0; i<Model_Count; i++)
	{

		string which_model;	
		scene>>which_model;
		
		int material_id;
		scene>>material_id;

		int texture_id;
		scene>>texture_id;
	
		int transformation_count;
		scene>>transformation_count;

//		cout<<"which = "<<which_model<<endl;
//		cout<<"MID = "<<material_id<<endl;
//		cout<<"texid = "<<texture_id<<endl;



		Sphere s;
		s.Material_ID=material_id;
		s.Texture_id=texture_id;
		Mesh m(material_id,texture_id);

	//	cout<<"A="<<s.Texture_id<<endl;

//		Triangle f1,f11,f2,f22,f3,f33,f4,f44,f5,f55,f6,f66;
		vector<transformation> transformer;
		bool varmi=false;
		for(int i=0; i<transformation_count; i++)
		{
			transformation t;
			scene>>t.type>>t.id;
//			in.transformer.push_back(t);
//			transformer.push_back(t);

//			cout<<"type = "<<t.type;
//			cout<<" id = "<<t.id<<endl;

			////////////////////////////////////////////////////////////////////// doing transformation
		
			if(which_model == "#SphereInstance")
			{

				if(t.type=="t")
				{
					transformation rt;
					rt.id=t.id;
					rt.type="t";						

//					Current_Scene->rot_tran.push_back(rt);
					transformer.push_back(rt);


					Vector3 translation=Current_Scene->translations[t.id-1];
					float P[4]={s.center[0],s.center[1],s.center[2],1};
					translator(translation,P,s.center);

					float x[4]={s.center[0]+s.x[0],s.center[1]+s.x[1],s.center[2]+s.x[2],1}; 
					translator(translation,x,s.x);

					float y[4]={s.center[0]+s.y[0],s.center[1]+s.y[1],s.center[2]+s.y[2],1}; 
					translator(translation,y,s.y);

					float z[4]={s.center[0]+s.z[0],s.center[1]+s.z[1],s.center[2]+s.z[2],1};
					translator(translation,z,s.z);


				}
				else if(t.type == "r")//shunu kontrol etmedim
				{


					transformation rt;
					rt.id=t.id;
					rt.type="r";						

					varmi=true;
//					Current_Scene->rot_tran.push_back(rt);
					transformer.push_back(rt);

					rotator rot=Current_Scene->Rotates[t.id-1];

					Vector3 vv= rot.rotate;	
					float d=rot.degree;
					
					Current_Scene->degrees.push_back(-d);
					Current_Scene->axiss.push_back(vv);
					
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
					M_R[3][0]=0;		M_R[3][1]=0;		M_R[3][2]=0;		M_R[3][3]=1;

					float M_R_I[4][4];
					M_R_I[0][0]=u._data[0];	M_R_I[0][1]=v._data[0];	M_R_I[0][2]=w._data[0];	M_R_I[0][3]=0;
					M_R_I[1][0]=u._data[1];	M_R_I[1][1]=v._data[1];	M_R_I[1][2]=w._data[1];	M_R_I[1][3]=0;
					M_R_I[2][0]=u._data[2];	M_R_I[2][1]=v._data[2];	M_R_I[2][2]=w._data[2];	M_R_I[2][3]=0;
					M_R_I[3][0]=0;		M_R_I[3][1]=0;		M_R_I[3][2]=0;		M_R_I[3][3]=1;


					float R_x[4][4];
					R_x[0][0]=1;	R_x[0][1]=0;				R_x[0][2]=0;				R_x[0][3]=0;
					R_x[1][0]=0;	R_x[1][1]=cos(d*PI/180);		R_x[1][2]=-sin(d*PI/180);		R_x[1][3]=0;
					R_x[2][0]=0;	R_x[2][1]=sin(d*PI/180);		R_x[2][2]=cos(d*PI/180);		R_x[2][3]=0;
					R_x[3][0]=0;	R_x[3][1]=0;				R_x[3][2]=0;				R_x[3][3]=1;

					float P[4]={s.center[0],s.center[1],s.center[2],1};
					float C_p[4];
					float tmp[4][4];
					four_by_four_multiplier(M_R_I,R_x,tmp);
					float tmp2[4][4];
					four_by_four_multiplier(tmp,M_R,tmp2);
					four_multiplier(tmp2,P,C_p);

					float X_p[4]; float x[4]={s.center[0]+s.x[0],s.center[1]+s.x[1],s.center[2]+s.x[2],1}; 
					four_multiplier(tmp2,x,X_p);
					float Y_p[4]; float y[4]={s.center[0]+s.y[0],s.center[1]+s.y[1],s.center[2]+s.y[2],1}; 
					four_multiplier(tmp2,y,Y_p);
					float Z_p[4]; float z[4]={s.center[0]+s.z[0],s.center[1]+s.z[1],s.center[2]+s.z[2],1};
					four_multiplier(tmp2,z,Z_p);	
				

						s.center[0]=C_p[0];	s.center[1]=C_p[1];	s.center[2]=C_p[2];
						s.x[0]=X_p[0];		s.x[1]=X_p[1];		s.x[2]=X_p[2];
						s.y[0]=Y_p[0];		s.y[1]=Y_p[1];		s.y[2]=Y_p[2];
						s.z[0]=Z_p[0];		s.z[1]=Z_p[1];		s.z[2]=Z_p[2];

				}
				else
				{
						Vector3 scalee=Current_Scene->Scales[t.id-1];


						s.Radius=s.Radius*scalee._data[0];
						s.x[0]=s.center[0] + ((s.x[0]-s.center[0])*s.Radius);
						s.x[1]=s.center[1] + ((s.x[1]-s.center[1])*s.Radius);
						s.x[2]=s.center[2] + ((s.x[2]-s.center[2])*s.Radius);
						s.y[0]=s.center[0] + ((s.y[0]-s.center[0])*s.Radius);
						s.y[1]=s.center[1] + ((s.y[1]-s.center[1])*s.Radius);
						s.y[2]=s.center[2] + ((s.y[2]-s.center[2])*s.Radius);
						s.z[0]=s.center[0] + ((s.z[0]-s.center[0])*s.Radius);
						s.z[1]=s.center[1] + ((s.z[1]-s.center[1])*s.Radius);
						s.z[2]=s.center[2] + ((s.z[2]-s.center[2])*s.Radius);
				}
				
			}
			else
			{
				if(t.type=="t")
				{
						Vector3 translation=Current_Scene->translations[t.id-1];

//						vector<Triangle> reserve;
						Vector3 cent=m.center;
						translator_tri(translation,cent,m.center);

						Vector3 a1=m.triangles[0].VID[0]; //f1
						Vector3 a2=m.triangles[0].VID[1];
						Vector3 a3=m.triangles[0].VID[2];
 
						Vector3 a4=m.triangles[7].VID[0]; //f44
						Vector3 a6=m.triangles[7].VID[1];
						Vector3 a5=m.triangles[7].VID[2];

						Vector3 a8=m.triangles[5].VID[1]; //f33
						Vector3 a7=m.triangles[5].VID[2];

						Vector3 a11,a22,a33,a44,a55,a66,a77,a88;


						translator_tri(translation,a1,a11);
						translator_tri(translation,a2,a22);
						translator_tri(translation,a3,a33);
						translator_tri(translation,a4,a44);
						translator_tri(translation,a5,a55);
						translator_tri(translation,a6,a66);
						translator_tri(translation,a7,a77);
						translator_tri(translation,a8,a88);
			

						int MID=m.triangles[0].Material_ID;
						int tex_id=m.triangles[0].Texture_ID;

						vector<Triangle> reserve;
				
						Triangle f1(MID,tex_id,a11,a22,a33,cent);
						Triangle f11(MID,tex_id,a11,a33,a44,cent);

						Triangle f2(MID,tex_id,a77,a88,a22,cent);
						Triangle f22(MID,tex_id,a77,a22,a11,cent);

						Triangle f3(MID,tex_id,a55,a66,a88,cent);
						Triangle f33(MID,tex_id,a55,a88,a77,cent);

						Triangle f4(MID,tex_id,a44,a33,a66,cent);
						Triangle f44(MID,tex_id,a44,a66,a55,cent);

						Triangle f5(MID,tex_id,a22,a88,a66,cent);
						Triangle f55(MID,tex_id,a22,a66,a33,cent);
		
						Triangle f6(MID,tex_id,a77,a11,a44,cent);
						Triangle f66(MID,tex_id,a77,a44,a55,cent);


						reserve.push_back(f1);
						reserve.push_back(f11);
						reserve.push_back(f2);
						reserve.push_back(f22);
						reserve.push_back(f3);
						reserve.push_back(f33);
						reserve.push_back(f4);
						reserve.push_back(f44);
						reserve.push_back(f5);
						reserve.push_back(f55);
						reserve.push_back(f6);
						reserve.push_back(f66);

//				cout<<"bitch = "<<m.center._data[0]<<" "<<m.center._data[1]<<" "<<m.center._data[2]<<endl;

						m.triangles.clear();
						for(int k=0; k<reserve.size();k++)
							m.triangles.push_back(reserve[k]);


						
				/*		for(int i=0; i<m.triangles.size(); i++)
						{

						Vector3 a=m.triangles[i].VID[0];
						Vector3 a1,a2,a3;

						translator_tri(translation,a,a1);
						
							Vector3 b=m.triangles[i].VID[1];
							Vector3 c=m.triangles[i].VID[2];

							translator_tri(translation,b,a2);
							translator_tri(translation,c,a3);

							Triangle r(m.triangles[i].Material_ID,m.triangles[i].Texture_ID,a1,a2,a3,cent);
							reserve.push_back(r);

						}
				//cout<<"icinde = "<<m.center._data[0]<<" "<<m.center._data[1]<<" "<<m.center._data[2]<<endl;

						m.triangles.clear();
						for(int k=0; k<reserve.size();k++)
							m.triangles.push_back(reserve[k]);
*/
			}
			else if(t.type == "r")//shunu kontrol etmedim
			{
				rotator rot=Current_Scene->Rotates[t.id-1];

				Vector3 vv= rot.rotate;	
				float d=rot.degree;
					
		//		cout<<"d= "<<d<<endl;
		//		cout<<"vv="<<vv._data[0]<<" "<<vv._data[1]<<" "<<vv._data[2]<<endl;	
					
				float dis=sqrt(vv._data[0]*vv._data[0] + vv._data[1]*vv._data[1] + vv._data[2]*vv._data[2]);
				Vector3 u;
				u._data[0]=vv._data[0]/dis; u._data[1]=vv._data[1]/dis; u._data[2]=vv._data[2]/dis;

		//		cout<<"u="<<u._data[0]<<" "<<u._data[1]<<" "<<u._data[2]<<endl;	
				Vector3 cent=m.center;

				
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
				Vector3 w;
					//cx = aybz − azby = 3×7 − 4×6 = −3
					//cy = azbx − axbz = 4×5 − 2×7 = 6
					//cz = axby − aybx = 2×6 − 3×5 = −3
				w._data[0]=(u._data[1])*(v._data[2]) - (u._data[2])*(v._data[1]);  
				w._data[1]=(u._data[2])*(v._data[0]) - (u._data[0])*(v._data[2]);  
				w._data[2]=(u._data[0])*(v._data[1]) - (u._data[1])*(v._data[0]);  

				

			//	cout<<"v="<<v._data[0]<<" "<<v._data[1]<<" "<<v._data[2]<<endl;	

			//	cout<<"u="<<u._data[0]<<" "<<u._data[1]<<" "<<u._data[2]<<endl;	
			//	cout<<"w="<<w._data[0]<<" "<<w._data[1]<<" "<<w._data[2]<<endl;	
		
				dis=sqrt(w._data[0]*w._data[0] + w._data[1]*w._data[1] + w._data[2]*w._data[2]);
				w._data[0]=w._data[0]/dis; w._data[1]=w._data[1]/dis; w._data[2]=w._data[2]/dis;
			//	cout<<"w="<<w._data[0]<<" "<<w._data[1]<<" "<<w._data[2]<<endl;	


				float M_R[4][4];
				M_R[0][0]=u._data[0];	M_R[0][1]=u._data[1];	M_R[0][2]=u._data[2];	M_R[0][3]=0;
				M_R[1][0]=v._data[0];	M_R[1][1]=v._data[1];	M_R[1][2]=v._data[2];	M_R[1][3]=0;
				M_R[2][0]=w._data[0];	M_R[2][1]=w._data[1];	M_R[2][2]=w._data[2];	M_R[2][3]=0;
				M_R[3][0]=0;		M_R[3][1]=0;		M_R[3][2]=0;		M_R[3][3]=1;

				float M_R_I[4][4];
				M_R_I[0][0]=u._data[0];	M_R_I[0][1]=v._data[0];	M_R_I[0][2]=w._data[0];	M_R_I[0][3]=0;
				M_R_I[1][0]=u._data[1];	M_R_I[1][1]=v._data[1];	M_R_I[1][2]=w._data[1];	M_R_I[1][3]=0;
				M_R_I[2][0]=u._data[2];	M_R_I[2][1]=v._data[2];	M_R_I[2][2]=w._data[2];	M_R_I[2][3]=0;
				M_R_I[3][0]=0;		M_R_I[3][1]=0;		M_R_I[3][2]=0;		M_R_I[3][3]=1;


				float R_x[4][4];
				R_x[0][0]=1;	R_x[0][1]=0;				R_x[0][2]=0;				R_x[0][3]=0;
				R_x[1][0]=0;	R_x[1][1]=cos(d*PI/180);		R_x[1][2]=-sin(d*PI/180);	R_x[1][3]=0;
				R_x[2][0]=0;	R_x[2][1]=sin(d*PI/180);		R_x[2][2]=cos(d*PI/180);		R_x[2][3]=0;
				R_x[3][0]=0;	R_x[3][1]=0;				R_x[3][2]=0;				R_x[3][3]=1;







///////////////////////////////////////////////////////////


				Vector3 a1=m.triangles[0].VID[0]; //f1
				Vector3 a2=m.triangles[0].VID[1];
				Vector3 a3=m.triangles[0].VID[2];
 
				Vector3 a4=m.triangles[7].VID[0]; //f44
				Vector3 a6=m.triangles[7].VID[1];
				Vector3 a5=m.triangles[7].VID[2];

				Vector3 a8=m.triangles[5].VID[1]; //f33
				Vector3 a7=m.triangles[5].VID[2];




				Vector3 a11,a22,a33,a44,a55,a66,a77,a88;
				
					float tmp[4][4];
					four_by_four_multiplier(M_R_I,R_x,tmp);
					float tmp2[4][4];
					four_by_four_multiplier(tmp,M_R,tmp2);

		
					float X_p[4]; float x[4]={a1._data[0],a1._data[1],a1._data[2],1}; 
					four_multiplier(tmp2,x,X_p);
					a11._data[0]=X_p[0];	a11._data[1]=X_p[1];	a11._data[2]=X_p[2];


					x[0]=m.center._data[0]; x[1]=m.center._data[1]; x[2]=m.center._data[2]; 					
					four_multiplier(tmp2,x,X_p);
					m.center._data[0]=X_p[0];	m.center._data[1]=X_p[1];	m.center._data[2]=X_p[2];



					x[0]=a2._data[0]; x[1]=a2._data[1]; x[2]=a2._data[2]; 					
					four_multiplier(tmp2,x,X_p);
					a22._data[0]=X_p[0];	a22._data[1]=X_p[1];	a22._data[2]=X_p[2];




					x[0]=a3._data[0]; x[1]=a3._data[1]; x[2]=a3._data[2]; 					
					four_multiplier(tmp2,x,X_p);
					a33._data[0]=X_p[0];	a33._data[1]=X_p[1];	a33._data[2]=X_p[2];

					x[0]=a4._data[0]; x[1]=a4._data[1]; x[2]=a4._data[2]; 					
					four_multiplier(tmp2,x,X_p);
					a44._data[0]=X_p[0];	a44._data[1]=X_p[1];	a44._data[2]=X_p[2];



					x[0]=a5._data[0]; x[1]=a5._data[1]; x[2]=a5._data[2]; 					
					four_multiplier(tmp2,x,X_p);
					a55._data[0]=X_p[0];	a55._data[1]=X_p[1];	a55._data[2]=X_p[2];



					x[0]=a6._data[0]; x[1]=a6._data[1]; x[2]=a6._data[2]; 					
					four_multiplier(tmp2,x,X_p);
					a66._data[0]=X_p[0];	a66._data[1]=X_p[1];	a66._data[2]=X_p[2];

					x[0]=a7._data[0]; x[1]=a7._data[1]; x[2]=a7._data[2]; 					
					four_multiplier(tmp2,x,X_p);
					a77._data[0]=X_p[0];	a77._data[1]=X_p[1];	a77._data[2]=X_p[2];

					x[0]=a8._data[0]; x[1]=a8._data[1]; x[2]=a8._data[2]; 					
					four_multiplier(tmp2,x,X_p);
					a88._data[0]=X_p[0];	a88._data[1]=X_p[1];	a88._data[2]=X_p[2];






				int MID=m.triangles[0].Material_ID;
				int tex_id=m.triangles[0].Texture_ID;

				vector<Triangle> reserve;
				
				Triangle f1(MID,tex_id,a11,a22,a33,cent);
				Triangle f11(MID,tex_id,a11,a33,a44,cent);

				Triangle f2(MID,tex_id,a77,a88,a22,cent);
				Triangle f22(MID,tex_id,a77,a22,a11,cent);

				Triangle f3(MID,tex_id,a55,a66,a88,cent);
				Triangle f33(MID,tex_id,a55,a88,a77,cent);

				Triangle f4(MID,tex_id,a44,a33,a66,cent);
				Triangle f44(MID,tex_id,a44,a66,a55,cent);

				Triangle f5(MID,tex_id,a22,a88,a66,cent);
				Triangle f55(MID,tex_id,a22,a66,a33,cent);

				Triangle f6(MID,tex_id,a77,a11,a44,cent);
				Triangle f66(MID,tex_id,a77,a44,a55,cent);


				reserve.push_back(f1);
				reserve.push_back(f11);
				reserve.push_back(f2);
				reserve.push_back(f22);
				reserve.push_back(f3);
				reserve.push_back(f33);
				reserve.push_back(f4);
				reserve.push_back(f44);
				reserve.push_back(f5);
				reserve.push_back(f55);
				reserve.push_back(f6);
				reserve.push_back(f66);

//				cout<<"bitch = "<<m.center._data[0]<<" "<<m.center._data[1]<<" "<<m.center._data[2]<<endl;

				m.triangles.clear();
				for(int k=0; k<reserve.size();k++)
					m.triangles.push_back(reserve[k]);








//cout<<"rotate dayım"<<endl;








			/*	vector<Triangle> reserve;
				for(int i=0; i<m.triangles.size(); i++)
				{
					Vector3 a=m.triangles[i].VID[0];
					float X_p[4]; float x[4]={a._data[0],a._data[1],a._data[2],1}; 
		
					float tmp[4][4];
					four_by_four_multiplier(M_R_I,R_x,tmp);
					float tmp2[4][4];
					four_by_four_multiplier(tmp,M_R,tmp2);
					four_multiplier(tmp2,x,X_p);
	
					
					Vector3 b=m.triangles[i].VID[1];
					float Y_p[4]; float y[4]={b._data[0],b._data[1],b._data[2],1}; 
					four_multiplier(tmp2,y,Y_p);

					Vector3 c=m.triangles[i].VID[2];
					float Z_p[4]; float z[4]={c._data[0],c._data[1],c._data[2],1}; 
					four_multiplier(tmp2,z,Z_p);

					Vector3 a1,a2,a3;
					a1._data[0]=X_p[0];	a1._data[1]=X_p[1];	a1._data[2]=X_p[2];
					a2._data[0]=Y_p[0];	a2._data[1]=Y_p[1];	a2._data[2]=Y_p[2];
					a3._data[0]=Z_p[0];	a3._data[1]=Z_p[1];	a3._data[2]=Z_p[2];
					
					Triangle r(m.triangles[i].Material_ID,m.triangles[i].Texture_ID,a1,a2,a3);
					reserve.push_back(r);

				}
				m.triangles.clear();
				for(int k=0; k<reserve.size();k++)
				m.triangles.push_back(reserve[k]);
*/














			}
			else
			{

				Vector3 scalee=Current_Scene->Scales[t.id-1];

				Vector3 origin;origin._data[0]=0;origin._data[1]=0;origin._data[2]=0;
				

				Vector3 a1=m.triangles[0].VID[0]; //f1
				Vector3 a2=m.triangles[0].VID[1];
				Vector3 a3=m.triangles[0].VID[2];
 
				Vector3 a4=m.triangles[7].VID[0]; //f44
				Vector3 a6=m.triangles[7].VID[1];
				Vector3 a5=m.triangles[7].VID[2];

				Vector3 a8=m.triangles[5].VID[1]; //f33
				Vector3 a7=m.triangles[5].VID[2];



				Vector3 a1o, o1a;
				bool transfer_back=false;
if(m.center._data[0]!=0 || m.center._data[1]!=0 || m.center._data[2]!=0 )
{

				two_diff(m.center, origin, a1o, o1a);

				translator_tri(a1o,m.triangles[0].VID[0], a1);
				translator_tri(a1o,m.triangles[0].VID[1], a2);
				translator_tri(a1o,m.triangles[0].VID[2], a3);
				translator_tri(a1o,m.triangles[7].VID[0], a4);
				translator_tri(a1o,m.triangles[7].VID[1], a6);
				translator_tri(a1o,m.triangles[7].VID[2], a5);
				translator_tri(a1o,m.triangles[5].VID[1], a8);
				translator_tri(a1o,m.triangles[5].VID[2], a7);
	
				transfer_back=true;
}

				Vector3 a11,a22,a33,a44,a55,a66,a77,a88;
				tri_scale(scalee, a1, a11);
				tri_scale(scalee, a2, a22);
				tri_scale(scalee, a3, a33);
				tri_scale(scalee, a4, a44);
				tri_scale(scalee, a5, a55);
				tri_scale(scalee, a6, a66);
				tri_scale(scalee, a7, a77);
				tri_scale(scalee, a8, a88);


if(transfer_back)
{
				Vector3 a111,a222,a333,a444,a555,a666,a777,a888;
				a111=a11;
				a222=a22;
				a333=a33;
				a444=a44;
				a555=a55;
				a666=a66;
				a777=a77;
				a888=a88;


				translator_tri(o1a,a111,a11);
				translator_tri(o1a, a222,a22);
				translator_tri(o1a, a333,a33);
				translator_tri(o1a, a444,a44);
				translator_tri(o1a, a555,a55);
				translator_tri(o1a, a666,a66);
				translator_tri(o1a, a777,a77);
				translator_tri(o1a, a888,a88);

}
				int MID=m.triangles[0].Material_ID;
				int tex_id=m.triangles[0].Texture_ID;
				Vector3 cent=m.center;
				vector<Triangle> reserve;
				
				Triangle f1(MID,tex_id,a11,a22,a33,cent);
				Triangle f11(MID,tex_id,a11,a33,a44,cent);

				Triangle f2(MID,tex_id,a77,a88,a22,cent);
				Triangle f22(MID,tex_id,a77,a22,a11,cent);

				Triangle f3(MID,tex_id,a55,a66,a88,cent);
				Triangle f33(MID,tex_id,a55,a88,a77,cent);

				Triangle f4(MID,tex_id,a44,a33,a66,cent);
				Triangle f44(MID,tex_id,a44,a66,a55,cent);

				Triangle f5(MID,tex_id,a22,a88,a66,cent);
				Triangle f55(MID,tex_id,a22,a66,a33,cent);

				Triangle f6(MID,tex_id,a77,a11,a44,cent);
				Triangle f66(MID,tex_id,a77,a44,a55,cent);


				reserve.push_back(f1);
				reserve.push_back(f11);
				reserve.push_back(f2);
				reserve.push_back(f22);
				reserve.push_back(f3);
				reserve.push_back(f33);
				reserve.push_back(f4);
				reserve.push_back(f44);
				reserve.push_back(f5);
				reserve.push_back(f55);
				reserve.push_back(f6);
				reserve.push_back(f66);

//				cout<<"bitch = "<<m.center._data[0]<<" "<<m.center._data[1]<<" "<<m.center._data[2]<<endl;

				m.triangles.clear();
				for(int k=0; k<reserve.size();k++)
					m.triangles.push_back(reserve[k]);


		}




	}
}
		if(which_model == "#SphereInstance")
		{
			Current_Scene->add_spheres(s);
			Current_Scene->rot_tran.push_back(transformer);
			Current_Scene->rot_varmi.push_back(varmi);
		}		
		else
		{
			Current_Scene->add_meshes(m);
		}


	
}

		//Current_Scene->see_instances();
//////////////////////////////////////////////////////////////////////////

/////////////////////////////////////////////////////////////////////////////////

	ifstream camerA;
	camerA.open(argv[2]); //sample_camera

	int Camera_Count;
	camerA>>Camera_Count;

	
	for(int i=0; i<Camera_Count; i++)
	{
		int c_id;
		string Cid;
		camerA>>Cid>>c_id;
		
		Vector3 position;
		camerA>>position;
		

		Vector3 gaze;
		camerA>>gaze;
		
		Vector3 Up;
		camerA>>Up;
		

		Vector3 Forward;
		Forward._data[0]=-gaze._data[0];///////////////////////////////////////////////////
		Forward._data[1]=-gaze._data[1]; ///////////////////////////////////////////////////////77
		Forward._data[2]=-gaze._data[2]; //sanki normalize etmeli/////////////////////////////////////////
		
	
		//cx = aybz − azby = 3×7 − 4×6 = −3
		//cy = azbx − axbz = 4×5 − 2×7 = 6
		//cz = axby − aybx = 2×6 − 3×5 = −3

		Vector3 Left; //cross product olcak

		Left._data[0]=(Up._data[1])*(Forward._data[2]) - (Up._data[2])*(Forward._data[1]);  
		Left._data[1]=(Up._data[2])*(Forward._data[0]) - (Up._data[0])*(Forward._data[2]);  
		Left._data[2]=(Up._data[0])*(Forward._data[1]) - (Up._data[1])*(Forward._data[0]);  


		float Left1, Right1, Bottom1, Top1, Distance1;
		int Width1, Height1;

		camerA>>Left1>>Right1>>Bottom1>>Top1>>Distance1>>Width1>>Height1;

		string outputFile;

		camerA>>outputFile;

		Camera camera(position,gaze,outputFile,Forward,Up,Left, Left1, Right1, Bottom1, Top1, Distance1, Width1, Height1,c_id);

		Current_Scene->add_cameras(camera);
		
	}
//	Current_Scene->see_cameras();

	CurrentScene=Current_Scene;
}


