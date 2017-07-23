#include "Scene.h"
#include "Camera.h"
#include <math.h> 
#include <algorithm> 
#include <climits>
#define PI 3.14159265

#define EPSILON 0.000001


void burak(float T[4][4], float P[4], float (&P_p)[4])
{
	P_p[0]=(T[0][0]*P[0]) + (T[0][1]*P[1]) + (T[0][2]*P[2]) + (T[0][3]*P[3]);
	P_p[1]=(T[1][0]*P[0]) + (T[1][1]*P[1]) + (T[1][2]*P[2]) + (T[1][3]*P[3]);
	P_p[2]=(T[2][0]*P[0]) + (T[2][1]*P[1]) + (T[2][2]*P[2]) + (T[2][3]*P[3]);
	P_p[3]=(T[3][0]*P[0]) + (T[3][1]*P[1]) + (T[3][2]*P[2]) + (T[3][3]*P[3]);
}




void four_by_four_multiplier_camera(float T[4][4], float P[4][4], float (&P_p)[4][4])
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




Camera::Camera(Vector3 position,Vector3 gaze,string outputFile,Vector3 Up,Vector3 Xaxis, float Left1, float Right1, float Bottom1, float Top1, float Near, float Far, int Width1, int Height1,int c_id)
{
	this->camera_id=c_id;

	_space._position=position;
	_space._gaze._data[0]=gaze._data[0];
	_space._gaze._data[1]=gaze._data[1];
	_space._gaze._data[2]=gaze._data[2];
	

	_space.Xaxis=Xaxis;
	_space.Up=Up;

	_imagePlane.Left=Left1;
	_imagePlane.Right=Right1;
	_imagePlane.Bottom=Bottom1;
	_imagePlane.Top=Top1;
	_imagePlane.Near=Near;
	_imagePlane.Far=Far;

	_imagePlane.Width=Width1;
	_imagePlane.Height=Height1;

	_outputFile=outputFile;
		

}



Image Camera::Render() const
{

	Color back=CurrentScene->_background;

	Image ret(_imagePlane.Width,_imagePlane.Height,back);

	int width=_imagePlane.Width;
	int height=_imagePlane.Height;



	float nx=width;
	float ny=height;

	float r=_imagePlane.Right;
	float l=_imagePlane.Left;
	float t=_imagePlane.Top;
	float b=_imagePlane.Bottom;
	float n=_imagePlane.Near;
	float f=_imagePlane.Far;

	Vector3 e=_space._position;
	
	Vector3 wcam=_space._gaze;
	Vector3 vcam=_space.Up;
	Vector3 ucam=_space.Xaxis;

/*

int yy=ny-10;
	for(int xx=0; xx<width; xx++)
	{
		Color initial(255,255,255);

//		if(xp> 0 && xp < width && yp > 0 && yp < height)
			ret.Pixel(yy, xx)=initial;


	}
*/



/*
cout<<"<<<<<<<<<<<>>>>>>>>>>>>>>"<<endl;
cout<<"position="<<e._data[0]<<" "<<e._data[1]<<" "<<e._data[2]<<endl;
cout<<"gaze="<<wcam._data[0]<<" "<<wcam._data[1]<<" "<<wcam._data[2]<<endl;
cout<<"up="<<vcam._data[0]<<" "<<vcam._data[1]<<" "<<vcam._data[2]<<endl;
cout<<"Xaxis="<<ucam._data[0]<<" "<<ucam._data[1]<<" "<<ucam._data[2]<<endl;

cout<<"Left1="<<l<<" Right1="<<r<<" Bottom1="<<b<<" Top1="<<t<<" Near="<<n<<" Far="<<f<<" Width="<<nx<<" Height="<<ny<<endl; 
*/



	float MCAM1[4][4];
	float MCAM2[4][4];

	MCAM1[0][0]=ucam._data[0];	MCAM1[0][1]=ucam._data[1];	MCAM1[0][2]=ucam._data[2];	MCAM1[0][3]=0;	
	MCAM1[1][0]=vcam._data[0];	MCAM1[1][1]=vcam._data[1];	MCAM1[1][2]=vcam._data[2];	MCAM1[1][3]=0;	
	MCAM1[2][0]=wcam._data[0];	MCAM1[2][1]=wcam._data[1];	MCAM1[2][2]=wcam._data[2];	MCAM1[2][3]=0;	
	MCAM1[3][0]=0;				MCAM1[3][1]=0;				MCAM1[3][2]=0;				MCAM1[3][3]=1;	
	

	MCAM2[0][0]=1;		MCAM2[0][1]=0;		MCAM2[0][2]=0;		MCAM2[0][3]=-e._data[0];	
	MCAM2[1][0]=0;		MCAM2[1][1]=1;		MCAM2[1][2]=0;		MCAM2[1][3]=-e._data[1];	
	MCAM2[2][0]=0;		MCAM2[2][1]=0;		MCAM2[2][2]=1;		MCAM2[2][3]=-e._data[2];	
	MCAM2[3][0]=0;		MCAM2[3][1]=0;		MCAM2[3][2]=0;		MCAM2[3][3]=1;	
	
//	float MCAM[4][4];
	//MCAM=MCAM1*MCAM2;

//	four_by_four_multiplier_camera(MCAM1,MCAM2,MCAM);


	float MPERS[4][4];
	MPERS[0][0]=(2.0f*n)/(r-l);	MPERS[0][1]=0;			MPERS[0][2]=(l+r)/(r-l);			MPERS[0][3]=0;	
	MPERS[1][0]=0;			MPERS[1][1]=(2.0f*n)/(t-b);	MPERS[1][2]=(t+b)/(t-b);			MPERS[1][3]=0;	
	MPERS[2][0]=0;			MPERS[2][1]=0;			MPERS[2][2]=(-1)*((f+n)/(f-n));		MPERS[2][3]=(-1)*((2*f*n)/(f-n));	
	MPERS[3][0]=0;			MPERS[3][1]=0;			MPERS[3][2]=-1;				MPERS[3][3]=0;	



	float MVPS[4][4];
	
	MVPS[0][0]=nx/2.0f;	MVPS[0][1]=0;		MVPS[0][2]=0;		MVPS[0][3]=(nx-1)/2.0f;	
	MVPS[1][0]=0;		MVPS[1][1]=ny/2.0f;	MVPS[1][2]=0;		MVPS[1][3]=(ny-1)/2.0f;	
	MVPS[2][0]=0;		MVPS[2][1]=0;		MVPS[2][2]=1/2.0f;	MVPS[2][3]=1/2.0f;	
	MVPS[3][0]=0;		MVPS[3][1]=0;		MVPS[3][2]=0;		MVPS[3][3]=0;	



//[xvp,yvp,zvp]=MVPS*MPERS*MCAM*[xw,yw,zw,1];

//	float temp2[4][4];
//	float MS[3][4];
//	four_by_four_multiplier_camera(MPERS,MCAM,temp2);
//	four_by_four_multiplier_camera2(MVPS,temp2,MS);

//	four_by_four_multiplier_camera2(MVPS,MPERS,temp2);
//	four_by_four_multiplier_camera2(temp2,MCAM,MS);


////////////////////////////////////////////////////////////////////////


	float**  zbuffer=new float*[height];
	for(int s=0;s<height;s++)
		zbuffer[s]=new float[width];

	for(int s=0; s<height; s++)
	{
		for(int ss=0; ss<width; ss++)
		{
			zbuffer[s][ss]=INT_MAX;
		}
	}



	vector<Vector3> camera_vertices;
	int size=CurrentScene->_vertices.size();

	Color initial(255,255,255);
	vector<Mesh> meshler=CurrentScene->_meshes;

	int size2=meshler.size();


	vector<bool> cull;
	
	for(int i=0; i<size2; i++)
	{
		for(int j=0;j<meshler[i].triangles.size(); j++)
		{
			Triangle tri=meshler[i].triangles[j];

			Vector3 a,b,c;
			a=CurrentScene->_vertices[tri.VID[0]-1];
			b=CurrentScene->_vertices[tri.VID[1]-1];
			c=CurrentScene->_vertices[tri.VID[2]-1];



			float b_a[3];
			float c_a[3];

			b_a[0]=b._data[0]-a._data[0];	c_a[0]=c._data[0]-a._data[0];
			b_a[1]=b._data[1]-a._data[1];	c_a[1]=c._data[1]-a._data[1];
			b_a[2]=b._data[2]-a._data[2];	c_a[2]=c._data[2]-a._data[2];	


			Vector3 v;
			v._data[0]=a._data[0]-e._data[0];
			v._data[1]=a._data[1]-e._data[1];
			v._data[2]=a._data[2]-e._data[2];


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


			float value=(norm._data[0]*v._data[0] + norm._data[1]*v._data[1] + norm._data[2]*v._data[2]);

			if(value>0)// backfacing
			{
//				cout<<"oygirdim"<<endl;

				cull.push_back(true);
			}
			else
			{
				cull.push_back(false);
			}


		}
	}





	for(int i=0; i<size; i++)
	{

		Vector3 a=CurrentScene->_vertices[i];
		
		float w[4];
		w[0]=a._data[0];
		w[1]=a._data[1];
		w[2]=a._data[2];
		w[3]=1;

		float plane[3];


		float MCAM[4][4];

		four_by_four_multiplier_camera(MCAM1,MCAM2,MCAM);


		float ww[4];
		
		burak(MCAM,w,ww);
		burak(MPERS,ww,w);

		w[0]=w[0]/w[3];
		w[1]=w[1]/w[3];
		w[2]=w[2]/w[3];
		w[3]=w[3]/w[3];


//		cout<<"xp="<<w[0]<<endl;
//cout<<"yp="<<w[1]<<endl;
//cout<<"zp="<<w[2]<<endl;



		burak(MVPS,w,ww);

		float xp=ww[0];
		float yp=ww[1];
		float zp=ww[2];

		Vector3 b;


		b._data[0]=xp;
		b._data[1]=yp;
		b._data[2]=zp;
		
		camera_vertices.push_back(b);

	}
///////////////////////////////////////////////////

/*	for(int i=0; i<camera_vertices.size(); i++)
	{
		Vector3 a=camera_vertices[i];

		int xp=round(a._data[0]);		
		int yp=round(a._data[1]);		
		int zp=round(a._data[2]);		

		Color initial(255,255,255);


		if(xp> 0 && xp < width && yp > 0 && yp < height)
			ret.Pixel(yp, nx-xp)=initial;
	}
*/
/////////////////////////////////////////////////////////
	
	int cul=0;
	for(int i=0; i<size2; i++)
	{
		for(int j=0;j<meshler[i].triangles.size(); j++)
		{

			if(cull[cul])//backfacing
			{
//				cout<<"waygirdim"<<endl;
				cul=cul+1;
				
			}
						
			else
			{
				cul=cul+1;

				Triangle tri=meshler[i].triangles[j];

				Vector3 a[3];
				a[0]=camera_vertices[tri.VID[0]-1];
				a[1]=camera_vertices[tri.VID[1]-1];
				a[2]=camera_vertices[tri.VID[2]-1];

				Color aa,bb,cc;
		
				aa=CurrentScene->color_of_vertices[tri.VID[0]-1];
				bb=CurrentScene->color_of_vertices[tri.VID[1]-1];
				cc=CurrentScene->color_of_vertices[tri.VID[2]-1];

		
				int x1,y1,x2,y2,x0,y0;
				float z0,z1,z2;	

				x0=a[0]._data[0]; y0=a[0]._data[1]; x1=a[1]._data[0]; y1=a[1]._data[1]; x2=a[2]._data[0]; y2=a[2]._data[1];

				z0=a[0]._data[2]; 
				z1=a[1]._data[2];
				z2=a[2]._data[2];

				int Ax,Ay,Bx,By,Cx,Cy;
				float Az,Bz,Cz;
				Ax=x0; Ay=y0; Az=z0; Bx=x1; By=y1; Bz=z1; Cx=x2; Cy=y2; Cz=z2;
				

				float eq1,eq2;

				eq1=((Bx-Ax)*(Cz-Az) - (Cx-Ax)*(Bz-Az))/((Bx-Ax)*(Cy-Ay) - (Cx-Ax)*(By-Ay));
			
				eq2=((By-Ay)*(Cz-Az) - (Cy-Ay)*(Bz-Az))/((Bx-Ax)*(Cy-Ay) - (Cx-Ax)*(By-Ay));


	//			cout<<"indexes="<<tri.VID[0]<<" "<<tri.VID[1]<<" "<<tri.VID[2]<<endl;
	//			cout<<"ilk=>x0="<<x0<<" y0="<<y0<<" x1="<<x1<<" y1="<<y1<<" x2="<<x2<<" y2="<<y2<<endl;


				int xmin,xmax;

				if(x0<=x1 && x0<=x2)
					xmin=x0;
				else if(x1<=x0 && x1<=x2)
					xmin=x1;
				else if(x2<=x1 && x2<=x0)
					xmin=x2;

				if(x0>=x2 && x0>=x1)
					xmax=x0;
				else if(x1>=x2 && x1>=x0)
					xmax=x1;
				else if(x2>=x1 && x2>=x0)
					xmax=x2;

				int ymin,ymax;
		
				if(y0<=y2 && y0<=y1)
					ymin=y0;
				else if(y1<=y2 && y1<=y0)
					ymin=y1;
				else if(y2<=y1 && y2<=y0)
					ymin=y2;
	
				if(y0>=y1 && y0>=y2)
					ymax=y0;
				else if(y1>=y2 && y1>=y0)
					ymax=y1;
				else if(y2>=y1 && y2>=y0)
					ymax=y2;


				if(tri.Model_type==1)//solid
				{
				
					float f01 = x2*(y0-y1) + y2*(x1-x0) + x0*y1 - y0*x1;
					float f12 = x0*(y1-y2) + y0*(x2-x1) + x1*y2 - y1*x2;
					float f20 = x1*(y2-y0) + y1*(x0-x2) + x2*y0 - y2*x0;

					for(int y=ymin; y<=ymax; y++)
					{
						for(int x=xmin; x<=xmax; x++)
						{
							float f011 = x*(y0-y1) + y*(x1-x0) + x0*y1 - y0*x1;
							float f122 = x*(y1-y2) + y*(x2-x1) + x1*y2 - y1*x2;
							float f200 = x*(y2-y0) + y*(x0-x2) + x2*y0 - y2*x0;

							float alpha,beta,gama;			
			
							alpha=f122/f12;
							beta=f200/f20;
							gama=f011/f01;
			
							if(alpha >= 0 && beta >= 0 && gama >= 0)
							{
							
								float R=aa.R()*alpha + bb.R()*beta + cc.R()*gama;
								float G=aa.G()*alpha + bb.G()*beta + cc.G()*gama;
								float B=aa.B()*alpha + bb.B()*beta + cc.B()*gama;
			
								float z=z0 + eq1*(y-y0) - eq2*(x-x0);
								Color finito(round(R),round(G),round(B));

								if(zbuffer[y][x] >=z)
								{
									zbuffer[y][x]=z;

									if(x> 0 && x < width && y > 0 && y < height)
										ret.Pixel(y, nx-x)=finito;
								}
//								Color finito(round(R),round(G),round(B));
//								if(x> 0 && x < width && y > 0 && y < height)
//									ret.Pixel(y, nx-x)=finito;

							}

						}
					}	
				}

				else
				{

					int tmp0y, tmp0x, tmp1y, tmp1x, tmp2y, tmp2x, tmpy, tmpx;
					tmp0y=y0; tmp0x=x0; tmp1y=y1; tmp1x=x1; tmp2y=y2; tmp2x=x2;

					Color tmpc;
					Color c0=aa;
					Color c1=bb;
					Color c2=cc;

	//			cout<<"x0="<<x0<<" y0="<<y0<<" x1="<<x1<<" y1="<<y1<<" x2="<<x2<<" y2="<<y2<<endl;

					for(int i=0; i<3; i++)
					{
	
						if(i==1)
						{
							tmpy=y0; tmpx=x0;
							y0=y1; x0=x1;
							y1=y2; x1=x2;
					
							tmpc=c0;		
							c0=c1;
							c1=c2;
						}
						else if(i==2)
						{
							y0=y1; x0=x1;
							y1=tmpy; x1=tmpx;

							c0=c1;
							c1=tmpc;
						}
					//////////////////////////// y0 dan y1

						int xmin1=x0<=x1 ? x0 : x1;
						int xmax1=x0<=x1 ? x1 : x0;
						int ymin1=y0<=y1 ? y0 : y1;
						int ymax1=y0<=y1 ? y1 : y0;

						Color cxi=x0<=x1 ? c0 : c1;
						Color cxf=x0<=x1 ? c1 : c0;

						Color cyi=y0<=y1 ? c0 : c1;
						Color cyf=y0<=y1 ? c1 : c0;
					
						float slope01=((float)y1-(float)y0)/((float)x1-(float)x0);
					

						if((x0-x1)==0) //vertical line case 1-2
						{
//							cout<<"verticalim"<<"	   y0="<<y0<<" x0="<<x0<<"	   y1="<<y1<<" x1="<<x1<<endl;

							int x=x0;
							Color c=cyi;/////////////////
//							Color dc=(cyf-cyi)/(ymax1-ymin1);//////////////////

							float dcR=(cyf.R() - cyi.R())/(ymax1-ymin1);
							float dcG=(cyf.G() - cyi.G())/(ymax1-ymin1);
							float dcB=(cyf.B() - cyi.B())/(ymax1-ymin1);


							for(int y=ymin1; y<=ymax1; y++)
							{
								float z=z0 + eq1*(y-y0) - eq2*(x-x0);

								if(zbuffer[y][x] >=z)
								{
									zbuffer[y][x]=z;

									if(x> 0 && x < width && y > 0 && y < height)
										ret.Pixel(y, nx-x)=c;
								}

//								if(x> 0 && x < width && y > 0 && y < height)						
//									ret.Pixel(y, nx-x)=c;
						
								int R=round(c.R() + dcR);
								int G=round(c.G() + dcG);
								int B=round(c.B() + dcB);
								Color nc(R,G,B);

								c=nc;///////////////////////////
							}
						}
						else
						{
						
							if(slope01==0) //case 3-4
							{
//								cout<<"horizontalim"<<"	   y0="<<y0<<" x0="<<x0<<"	   y1="<<y1<<" x1="<<x1<<endl;
								int y=y0;

								Color c=cxi;/////////////////
	//							Color dc=(cxf-cxi)/(xmax1-xmin1);

								float dcR=(cxf.R() - cxi.R())/(xmax1-xmin1);
								float dcG=(cxf.G() - cxi.G())/(xmax1-xmin1);
								float dcB=(cxf.B() - cxi.B())/(xmax1-xmin1);

								for(int x=xmin1; x<=xmax1; x++)
								{

									float z=z0 + eq1*(y-y0) - eq2*(x-x0);

									if(zbuffer[y][x] >=z)
									{
										zbuffer[y][x]=z;

										if(x> 0 && x < width && y > 0 && y < height)
											ret.Pixel(y, nx-x)=c;
									}

//									if(x> 0 && x < width && y > 0 && y < height)						
//										ret.Pixel(y, nx-x)=c;
						
									int R=round(c.R() + dcR);
									int G=round(c.G() + dcG);
									int B=round(c.B() + dcB);
									Color nc(R,G,B);

									c=nc;///////////////////////////							
								}
							}
							else if(slope01>0) //case 5-6
							{
						
							
								if(slope01>0 && slope01<=1)
								{
//									cout<<"normalim"<<"	   y0="<<y0<<" x0="<<x0<<"	   y1="<<y1<<" x1="<<x1<<endl;
									int y=ymin1;
									int d=2*(ymin1-ymax1)+(xmax1-xmin1);
						
									Color c=cxi;/////////////////
		//							Color dc=(cxf-cxi)/(xmax1-xmin1);

									float dcR=(cxf.R() - cxi.R())/(xmax1-xmin1);
									float dcG=(cxf.G() - cxi.G())/(xmax1-xmin1);
									float dcB=(cxf.B() - cxi.B())/(xmax1-xmin1);

				
									for(int x=xmin1; x<=xmax1; x++)
									{


										float z=z0 + eq1*(y-y0) - eq2*(x-x0);

										if(zbuffer[y][x] >=z)
										{
											zbuffer[y][x]=z;

											if(x> 0 && x < width && y > 0 && y < height)
												ret.Pixel(y, nx-x)=c;
										}


//										if(x> 0 && x < width && y > 0 && y < height)
//											ret.Pixel(y, nx-x)=c;
						
										if(d<0)
										{
											y=y+1;
											d=d+2*((ymin1-ymax1)+(xmax1-xmin1));
										}
										else
											d=d+2*(ymin1-ymax1);
									
										int R=round(c.R() + dcR);
										int G=round(c.G() + dcG);
										int B=round(c.B() + dcB);
										Color nc(R,G,B);

										c=nc;///////////////////////////								
									}
								}
								else if(slope01>1)
								{
//									cout<<"1den buyugum"<<"	   y0="<<y0<<" x0="<<x0<<"	   y1="<<y1<<" x1="<<x1<<endl;;
									int x=xmin1;
									int d=2*(xmin1-xmax1)+(ymax1-ymin1);				

									Color c=cyi;/////////////////
			//						Color dc=(cyf-cyi)/(ymax1-ymin1);

									float dcR=(cyf.R() - cyi.R())/(ymax1-ymin1);
									float dcG=(cyf.G() - cyi.G())/(ymax1-ymin1);
									float dcB=(cyf.B() - cyi.B())/(ymax1-ymin1);

									for(int y=ymin1; y<=ymax1; y++)
									{


										float z=z0 + eq1*(y-y0) - eq2*(x-x0);

										if(zbuffer[y][x] >=z)
										{
											zbuffer[y][x]=z;

											if(x> 0 && x < width && y > 0 && y < height)
												ret.Pixel(y, nx-x)=c;

										}

//										if(x> 0 && x < width && y > 0 && y < height)
///											ret.Pixel(y, nx-x)=c;
						
										if(d<0)
										{
											x=x+1;
											d=d+2*((xmin1-xmax1)+(ymax1-ymin1));
										}
										else
											d=d+2*(xmin1-xmax1);
								
										int R=round(c.R() + dcR);
										int G=round(c.G() + dcG);
										int B=round(c.B() + dcB);
										Color nc(R,G,B);

										c=nc;///////////////////////////
									}
								}
							}
							else if(slope01<0)
							{
								if(slope01<0 && slope01>=-1)
								{
//									cout<<"eksi normalim"<<"	   y0="<<y0<<" x0="<<x0<<"	   y1="<<y1<<" x1="<<x1<<endl;
									int y=ymin1;
									int d=2*(ymax1-ymin1)+(xmin1-xmax1);				


									Color c=cxf;/////////////////
			//						Color dc=(cxi-cxf)/(xmax1-xmin1);

									float dcR=(cxf.R() - cxi.R())/(xmax1-xmin1);
									float dcG=(cxf.G() - cxi.G())/(xmax1-xmin1);
									float dcB=(cxf.B() - cxi.B())/(xmax1-xmin1);

									for(int x=xmax1; x>=xmin1; x--)
									{

										float z=z0 + eq1*(y-y0) - eq2*(x-x0);

										if(zbuffer[y][x] >=z)
										{
											zbuffer[y][x]=z;

											if(x> 0 && x < width && y > 0 && y < height)
												ret.Pixel(y, nx-x)=c;
										}

//										if(x> 0 && x < width && y > 0 && y < height)
//											ret.Pixel(y, nx-x)=c;
						
										if(d>=0)
										{
											y=y+1;
											d=d+2*((ymax1-ymin1)+(xmin1-xmax1));
										}
										else
											d=d+2*(ymax1-ymin1);

										int R=round(c.R() + dcR);
										int G=round(c.G() + dcG);
										int B=round(c.B() + dcB);
										Color nc(R,G,B);
	
										c=nc;///////////////////////////
									}
								}
								else if(slope01<-1)
								{
//									cout<<"eksi 1den buyugum"<<"	   y0="<<y0<<" x0="<<x0<<"	   y1="<<y1<<" x1="<<x1<<endl;;
									
									int x=xmin1;
									int d=2*(xmax1-xmin1)+(ymin1-ymax1);				
						
									Color c=cyf;/////////////////
			//						Color dc=(cyi-cyf)/(ymax1-ymin1);

									float dcR=(cyf.R() - cyi.R())/(ymax1-ymin1);
									float dcG=(cyf.G() - cyi.G())/(ymax1-ymin1);
									float dcB=(cyf.B() - cyi.B())/(ymax1-ymin1);

									for(int y=ymax1; y>=ymin1; y--)	
									{


										float z=z0 + eq1*(y-y0) - eq2*(x-x0);


										if(zbuffer[y][x] >=z)
										{
											zbuffer[y][x]=z;

											if(x> 0 && x < width && y > 0 && y < height)
												ret.Pixel(y, nx-x)=c;
										}


//										if(x> 0 && x < width && y > 0 && y < height)
//											ret.Pixel(y, nx-x)=c;
						
										if(d>=0)
										{
											x=x+1;
											d=d+2*((xmax1-xmin1)+(ymin1-ymax1));
										}
										else
											d=d+2*(xmax1-xmin1);

										int R=round(c.R() + dcR);
										int G=round(c.G() + dcG);
										int B=round(c.B() + dcB);
										Color nc(R,G,B);

										c=nc;///////////////////////////
									}

								}

							}

						}

					}

				}

			}

		}
	}


	return ret;
}


