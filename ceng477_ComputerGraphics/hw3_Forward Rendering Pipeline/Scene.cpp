#include "Scene.h"
#define PI 3.14159265
#include<iostream>

Scene* CurrentScene;
struct vect
{
	int triple[3];
};

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



void translator_tri(Vector3 translation, Vector3 a, Vector3 &a1)
{
	float T[4][4];

	T[0][0]=1;	T[0][1]=0;	T[0][2]=0;	T[0][3]=translation._data[0];
	T[1][0]=0;	T[1][1]=1;	T[1][2]=0;	T[1][3]=translation._data[1];
	T[2][0]=0;	T[2][1]=0;	T[2][2]=1;	T[2][3]=translation._data[2];
	T[3][0]=0;	T[3][1]=0;	T[3][2]=0;	T[3][3]=1;

	float X_p[4]; float x[4]={a._data[0],a._data[1],a._data[2],1}; 
	four_multiplier(T,x,X_p);

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


void ReadScene(int argc, char** argv)
{
	Scene* Current_Scene = new Scene();
	ifstream scene;
	scene.open(argv[1]); //sample_scene
////////////////////////////////////////////////////
	Color* Background_Color = new Color();
	scene >> *Background_Color;
	Current_Scene->_background=Background_Color;
////////////////////////////////////////////////
	string text;
	scene>>text;
	scene>>Current_Scene->num_of_vertices;	


////////////////////////////////////////////////// taking colors
	scene>>text;
	for(int i=0; i<Current_Scene->num_of_vertices; i++)
	{
		Color* color = new Color();
		scene>> *color;
		Color c;
		c=color;
		Current_Scene->color_of_vertices.push_back(c);				
	}


/////////////////////////////////////////////////////////// taking positions
	scene>>text;
	for(int i=0; i<Current_Scene->num_of_vertices; i++)
	{
		Vector3 vec;
		scene>>vec;		
		Current_Scene->_vertices.push_back(vec);		
	}

//////////////////////////////////////////////////////////////// taking translations
	scene>>text;
	int num;
	scene>>num;
	for(int i=0; i<num; i++)
	{
		Vector3 vec;
		scene>>vec;		
		Current_Scene->translations.push_back(vec);		
	}


////////////////////////////////////////////////////////////////// taking scales

	scene>>text;
	scene>>num;
	for(int i=0; i<num; i++)
	{
		Vector3 vec;
		scene>>vec;		
		Current_Scene->scales.push_back(vec);		
	}
//////////////////////////////////////////////////////////////////////// taking rotations
	scene>>text;
	scene>>num;

	Vector3 Rotate;
	float a_degree;

	for(int i=0; i<num; i++)
	{
		scene>>a_degree;
		scene>>Rotate;
		rotator ob;
		ob.rotate=Rotate;
		ob.degree=a_degree;			
		Current_Scene->rotations.push_back(ob);
	}

//////////////////////////////////////////////////////////////////// taking models

	scene>>text;	
	int number_of_models;
	scene>>number_of_models;

	for(int i=0;i<number_of_models;i++)
	{
		Mesh mesh;
		int model_id;
		int model_type;
		int number_of_transformation;
		vector<transformation> transformations;

		scene>>model_id;
		scene>>model_type;
		scene>>number_of_transformation;
		
		mesh.Model_id=model_id;
		mesh.Model_type=model_type;
		mesh.number_of_transformation=number_of_transformation;


		for(int j=0;j<number_of_transformation;j++)
		{
			transformation t;
			scene>>t.type>>t.id;
			transformations.push_back(t);
			mesh.transformations.push_back(t);
		}
		
		int number_of_triangles;
		int index1,index2,index3;		
		scene>>number_of_triangles;
		mesh.number_of_triangles=number_of_triangles;
		vector<Triangle> triangles;

		vector<vect>triples;

		Vector3 counter;
		counter._data[0]=0;
		counter._data[1]=0;
		counter._data[2]=0;

////////////unique indexleri alyom start
		vector<int> indexes;
		for(int k=0; k < number_of_triangles; k++)
		{
			bool varmi1=false;
			bool varmi2=false;
			bool varmi3=false;

			scene>>index1>>index2>>index3;
			vect tri;
			tri.triple[0]=index1;
			tri.triple[1]=index2;
			tri.triple[2]=index3;

			triples.push_back(tri);
		

			for(int v=0; v<indexes.size(); v++)
			{
				if(indexes[v]==index1)
					varmi1=true;
				
				if(indexes[v]==index2)
					varmi2=true;

				if(indexes[v]==index3)
					varmi3=true;
			}
			if(!varmi1)
				indexes.push_back(index1);
			if(!varmi2)
				indexes.push_back(index2);			
			if(!varmi3)
				indexes.push_back(index3);
		}
////////////unique indexleri alyom end

//////////////her bir unique indexleri modify(transform) yapyom start
		vector<Vector3> modified;
		
		int s=indexes.size();
		for(int k=0; k < s; k++)
		{
			Vector3 a=Current_Scene->_vertices[indexes[k]-1];			
			Vector3 c=counter;
			for(int l=0;l<number_of_transformation;l++)
			{
				transformation t;
				t=transformations[l];

				if(t.type=="t")
				{
						Vector3 translation=Current_Scene->translations[t.id-1];
						Vector3 aa;
						translator_tri(translation,a,aa);

						a=aa;						
				}
				else if(t.type == "r")//shunu kontrol etmedim
				{
					rotator rot=Current_Scene->rotations[t.id-1];
					Vector3 vv= rot.rotate;	
					float d=rot.degree;
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
	
					dis=sqrt(w._data[0]*w._data[0] + w._data[1]*w._data[1] + w._data[2]*w._data[2]);
					w._data[0]=w._data[0]/dis; w._data[1]=w._data[1]/dis; w._data[2]=w._data[2]/dis;

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
					Vector3 aa;
				
					float tmp[4][4];
					four_by_four_multiplier(M_R_I,R_x,tmp);
					float tmp2[4][4];
					four_by_four_multiplier(tmp,M_R,tmp2);
		
					float X_p[4]; float x[4]={a._data[0],a._data[1],a._data[2],1}; 
					four_multiplier(tmp2,x,X_p);
					aa._data[0]=X_p[0];	aa._data[1]=X_p[1];	aa._data[2]=X_p[2];

					a=aa;							
				}
				else
				{
					Vector3 scalee=Current_Scene->scales[t.id-1];
					Vector3 a1=a;
					Vector3 a11;
					tri_scale(scalee, a1, a11);
					a=a11;	

				}
			}//transformation ends
		
			modified.push_back(a);
		}

/////////////triangle leri olusturyan
		for(int t=0; t<triples.size(); t++)
		{
			index1=triples[t].triple[0];
			index2=triples[t].triple[1];
			index3=triples[t].triple[2];

			Triangle tr(mesh.Model_id,mesh.Model_type,index1,index2,index3); //artik triangleler index tutyo 
			mesh.triangles.push_back(tr);
		}

	

		for(int cc=0; cc<s; cc++)
		{
			Current_Scene->_vertices[indexes[cc]-1]=modified[cc];

		}


/////////////modified leri geri global vertexlere yuklyom end
		Current_Scene->_meshes.push_back(mesh);
	}
///////////////////////////////////////////////////////////////////////////
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
		

		//cx = aybz − azby = 3×7 − 4×6 = −3
		//cy = azbx − axbz = 4×5 − 2×7 = 6
		//cz = axby − aybx = 2×6 − 3×5 = −3

		Vector3 Xaxis; //cross product olcak


		gaze._data[0]=-gaze._data[0];
		gaze._data[1]=-gaze._data[1];
		gaze._data[2]=-gaze._data[2];

		float dis=sqrt(gaze._data[0]*gaze._data[0]+gaze._data[1]*gaze._data[1] +gaze._data[2]*gaze._data[2]);

		Xaxis._data[0]=(gaze._data[1])*(Up._data[2]) - (gaze._data[2])*(Up._data[1]);  
		Xaxis._data[1]=(gaze._data[2])*(Up._data[0]) - (gaze._data[0])*(Up._data[2]);  
		Xaxis._data[2]=(gaze._data[0])*(Up._data[1]) - (gaze._data[1])*(Up._data[0]);  

		//Correct Up vector

		Up._data[0]=(gaze._data[1])*(Xaxis._data[2]) - (gaze._data[2])*(Xaxis._data[1]);  
		Up._data[1]=(gaze._data[2])*(Xaxis._data[0]) - (gaze._data[0])*(Xaxis._data[2]);  
		Up._data[2]=(gaze._data[0])*(Xaxis._data[1]) - (gaze._data[1])*(Xaxis._data[0]);  

		float dis3=sqrt(Up._data[0]*Up._data[0]+Up._data[1]*Up._data[1] +Up._data[2]*Up._data[2]);
		Up._data[0]=Up._data[0]/dis3;
		Up._data[1]=Up._data[1]/dis3;
		Up._data[2]=Up._data[2]/dis3;


		float dis2=sqrt(Xaxis._data[0]*Xaxis._data[0]+Xaxis._data[1]*Xaxis._data[1] +Xaxis._data[2]*Xaxis._data[2]);
		Xaxis._data[0]=Xaxis._data[0]/dis2;
		Xaxis._data[1]=Xaxis._data[1]/dis2;
		Xaxis._data[2]=Xaxis._data[2]/dis2;


		gaze._data[0]=gaze._data[0]/dis;
		gaze._data[1]=gaze._data[1]/dis;
		gaze._data[2]=gaze._data[2]/dis;



		float Left1, Right1, Bottom1, Top1, Near, Far;
		int Width1, Height1;

		camerA>>Left1>>Right1>>Bottom1>>Top1>>Near>>Far>>Width1>>Height1;

		string outputFile;

		camerA>>outputFile;

		Camera camera(position,gaze,outputFile,Up,Xaxis, Left1, Right1, Bottom1, Top1, Near, Far, Width1, Height1,c_id);

		Current_Scene->_cameras.push_back(camera);






/*cout<<"Cameras"<<endl;
cout<<"camera_count="<<Camera_Count<<" camera_id="<<Cid<<c_id<<endl;
cout<<"position="<<position._data[0]<<" "<<position._data[1]<<" "<<position._data[2]<<endl;
cout<<"gaze="<<gaze._data[0]<<" "<<gaze._data[1]<<" "<<gaze._data[2]<<endl;
cout<<"up="<<Up._data[0]<<" "<<Up._data[1]<<" "<<Up._data[2]<<endl;
cout<<"Xaxis="<<Xaxis._data[0]<<" "<<Xaxis._data[1]<<" "<<Xaxis._data[2]<<endl;

cout<<"Left1="<<Left1<<" Right1="<<Right1<<" Bottom1="<<Bottom1<<" Top1="<<Top1<<" Near="<<Near<<" Far="<<Far<<" Width="<<Width1<<" Height="<<Height1<<endl; 
*/

		
	}

	CurrentScene=Current_Scene;


	Mesh m=CurrentScene->_meshes[0];


	//cout<<"Model_id="<<m.Model_id<<endl;
//	cout<<"Model_type="<<m.Model_type<<endl;

//	cout<<"Triangles=>>>>"<<endl;

//	cout<<"triangle.size="<<m.triangles.size()<<endl;
/*	for(int t=0; t<m.triangles.size(); t++)
	{
		Triangle tr=m.triangles[t];
		Vector3 a,b,c;
		a=CurrentScene->_vertices[tr.VID[0]-1];
		b=CurrentScene->_vertices[tr.VID[1]-1];
		c=CurrentScene->_vertices[tr.VID[2]-1];
		
		cout<<t<<endl;
		cout<<a._data[0]<<" "<<a._data[1]<<" "<<a._data[2]<<" || ";

		cout<<b._data[0]<<" "<<b._data[1]<<" "<<b._data[2]<<" || ";

		cout<<c._data[0]<<" "<<c._data[1]<<" "<<c._data[2]<<endl;

	}*/
}


