#include "Scene.h"

Scene* CurrentScene;

/////////////////////////////////////////////edit
vector<Material> Scene::get_materials()
{
	return this->_materials;
}


vector<PointLight> Scene::get_lights()
{
	return this->_lights;
}

////////////////////////////////////////////end edit
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
vector<Sphere> Scene::getSpheres()
{
	return (this->_spheres);
}

vector<Mesh> Scene::getMeshes()
{
	return (this->_meshes);
}

void Scene::addVertices(Vertex _vertice)
{
	this->_vertices.push_back(_vertice);
}
void Scene::seeVertices()
{
	for(int i=0; i<this->_vertices.size(); i++)
	{
		cout<<"X ="<<this->_vertices[i].Position()._data[0]<<"Y ="<<this->_vertices[i].Position()._data[1]<<"Z ="<<this->_vertices[i].Position()._data[2]<<endl;
	}
}

Vertex Scene::getVertex(int id)
{
	return this->_vertices[id];
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


void Scene::add_meshes(Mesh mesh)
{
	_meshes.push_back(mesh);
}




void Scene::see_meshes()
{
	int num=this->_meshes.size();
	for(int i=0; i<num; i++)
	{
		int num2=_meshes[i].triangles.size();
		for(int j=0; j<num2; j++)
		{
			cout<<"model_id="<<_meshes[i].Model_id<<"Material_id="<<_meshes[i].triangles[j].Material_ID<<" id1="<<_meshes[i].triangles[j].VID[0]<<" id2="<<_meshes[i].triangles[j].VID[1]<<" id3="<<_meshes[i].triangles[j].VID[2]<<endl;
		}
	}
}		

void Scene::add_spheres(Sphere s)
{
	_spheres.push_back(s);
}

void Scene::see_spheres()
{
	int num=this->_spheres.size();
	for(int i=0; i<num; i++)
	{
		cout<<"SID="<<_spheres[i].SID<<" Material_id="<<_spheres[i].Material_ID<<" radius="<<_spheres[i].Radius<<" VID="<<_spheres[i].vertex_ID_Center<<endl;
	}


}

void Scene::add_cameras(Camera camera)
{
	_cameras.push_back(camera);
}
void Scene::see_cameras()
{
/*	
	
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
	
*/	int num=this->_cameras.size();
	for(int i=0; i<num; i++)
	{


		cout<<"cid="<<this->_cameras[i].camera_id<<"position="<<this->_cameras[i]._position._data[0]<<","<<this->_cameras[i]._position._data[1]<<","<<this->_cameras[i]._position._data[2]<<endl;
		cout<<"gaze="<<_cameras[i]._gaze._data[0]<<","<<_cameras[i]._gaze._data[1]<<","<<_cameras[i]._gaze._data[2]<<endl;
		cout<<"image_plane="<<"L="<<_cameras[i]._imagePlane.Left<<" R"<<_cameras[i]._imagePlane.Right<<" B"<<_cameras[i]._imagePlane.Bottom<<" T="<<_cameras[i]._imagePlane.Top<<" D="<<_cameras[i]._imagePlane.Distance<<" W="<<_cameras[i]._imagePlane.Width<<" H="<<_cameras[i]._imagePlane.Height<<endl;

		cout<<"_space="<<"F="<<_cameras[i]._space.Forward._data[0]<<","<<_cameras[i]._space.Forward._data[1]<<","<<_cameras[i]._space.Forward._data[2]<<" U="<<_cameras[i]._space.Up._data[0]<<","<<_cameras[i]._space.Up._data[1]<<","<<_cameras[i]._space.Up._data[2]<<" L="<<_cameras[i]._space.Left._data[0]<<","<<_cameras[i]._space.Left._data[1]<<","<<_cameras[i]._space.Left._data[2]<<endl;

		    
	}

}




void ReadScene(int argc, char** argv)
{
	
	
    /*
     * Reads the scene and cameras from arguments
     * Then sets the CurrentScene pointer
     */
	float RGB[3];
	float XYZ[3];
	
	Scene* Current_Scene = new Scene();
	

	ifstream scene;
	scene.open(argv[1]); //sample_scene

///////////////////////////////////////////////////////////////////////////////////////////////////////
	int Ray_Reflection_Count; 
	scene >> Ray_Reflection_Count;
	Current_Scene->set_rayReflect(Ray_Reflection_Count);
///	cout<<"rayReflect_Count = "<<Current_Scene->get_rayReflect()<<endl;
/////////////////////////////////////////////////////////////////////////////////////////////////////////////7
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

		
	int Vertex_Count;
	scene>>Vertex_Count;
//	cout<<"vertex_Count ="<<Vertex_Count<<endl;
	
	string Vertex_Data;
	string Vertex_Data2;
	scene>>Vertex_Data>>Vertex_Data2;
//	cout<<"vertex_Data ="<<Vertex_Data<<Vertex_Data2<<endl;

	for(int i=0; i<Vertex_Count; i++)
	{
		Vector3 vec;		
		scene>>vec;
		Vertex ver(vec);
		Current_Scene->addVertices(ver);		
	}
//		Current_Scene->seeVertices();


/////////////////////////////////////////////////////////////////////////

	int Model_Count;
	scene>>Model_Count;
//	cout<<"Model_Count="<<Model_Count<<endl;

	for(int i=0; i<Model_Count; i++)
	{

		string which_model;	
		int model_id;
		scene>>which_model>>model_id;

		if("#Mesh"==which_model)
		{
		
			Mesh mesh;
			mesh.Model_id=model_id;
			
			int Triangle_Count;
			scene>>Triangle_Count;
		//	cout<<"Triangle_Count="<<Triangle_Count<<endl;
	
			int material_id;
			scene>>material_id;

		//	cout<<"Material_ID="<<material_id<<endl;
		
			for(int i=0; i<Triangle_Count; i++)
			{
				
				Vector3 arg;
				scene>>arg;
				Triangle tri(arg,material_id);
				mesh.triangles.push_back(tri);
			}
	
			Current_Scene->add_meshes(mesh);
	//		Current_Scene->see_meshes();
	
		}
////////////////////////////////////////////////////////////////////////
		else
		{
			int sid=model_id;
		//	cout<<"SID="<<sid<<endl;

			int material_id;
			scene>>material_id;
			float r;
			scene>>r;			
			int VID;
			scene>>VID;
		
			
			Sphere s(sid, material_id, r, VID);
			Current_Scene->add_spheres(s);
		}
		
	}
//	Current_Scene->see_spheres();
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
	//Current_Scene->see_cameras();





	CurrentScene=Current_Scene;
}
