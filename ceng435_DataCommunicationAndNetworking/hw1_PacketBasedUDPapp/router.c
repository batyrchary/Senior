/* Sample UDP server */
#include <sys/socket.h>
#include <netinet/in.h>
#include <stdio.h>

int packet_size=5;
void parse_message(char* destip, char* sourceip, char* myip, char* mesg, char* new_mesg)
{

		char c;
		int index=0;
		char all_ips[5][16];		//all ips are kept here
		
		int d_index;
		char dd_index[2];			//destination index in char format		in modified header this will be destination part of header
		char ss_index[2]; ss_index[0]=mesg[0];	ss_index[1]='\0';	//source index in char format
		char mm_index[2]; mm_index[0]=mesg[2]; 	mm_index[1]='\0';	//routers index in char format in modified header this will be source part of header

		int s_index=mesg[0] - '0';				//source index in int format
		int m_index=mesg[2] - '0';			//routers index in int format and taken from header part of message(destination part is assigned to it) 



		index=4;
		int j,i;
		for( j=0; j<5; j++)		// in this for loop we extract all ips from message
		{
			for( i=0; ; i++)
			{
				c=mesg[index];
				if(c!='|')
				{	
				
					all_ips[j][i]=c;
					index++;
				}
				else
				{
					all_ips[j][i]='\0';
					index++;
					break;
				}
			}
		}

		
		strcpy(myip,all_ips[m_index]);	// we take routers ip from all ips with help of router index(which was destination index in header)


		if(m_index==4) //here we check if we arrived to server
		{
			d_index=3;
		}

		else if(m_index<s_index)		// here we check if our packet goes to client way 
		{
			d_index=m_index-1;		// if it goes to client way destination ip will we 1 less than our current source ip(router ip)
		}
		else if(m_index>s_index)		//// here we check if our packet goes to server way
		{
			d_index=m_index+1;		//// if it goes to client way destination ip will we 1 more than our current source ip(router ip)
		}	

		dd_index[0]=d_index+'0';
		dd_index[1]='\0';

		strcpy(destip,all_ips[d_index]);	// here we assign destip to our new detination ip that we will give as argument
		strcpy(sourceip,all_ips[s_index]);	//// here we assign sourceip to our new source ip that we will give as argument



		char mesg_r[5*16+9 + packet_size +1];
		for(i=0;mesg[index]!='\0'; index++,i++)		//here we read the message part of message
		{
			mesg_r[i]=mesg[index];
		}
		mesg_r[i]=NULL;

//		printf("mesg_r=%s\n",mesg_r);		//below we concatenate every thing to new_mesg 
		strcat(new_mesg,mm_index);		// index of new source
		strcat(new_mesg,"|");
		strcat(new_mesg,dd_index);		//index of new destination
		strcat(new_mesg,"|");
		strcat(new_mesg,all_ips[0]);		//  all ips
		strcat(new_mesg,"|");
		strcat(new_mesg,all_ips[1]);
		strcat(new_mesg,"|");
		strcat(new_mesg,all_ips[2]);
		strcat(new_mesg,"|");
		strcat(new_mesg,all_ips[3]);
		strcat(new_mesg,"|");
		strcat(new_mesg,all_ips[4]);
		strcat(new_mesg,"|");
		strcat(new_mesg,mesg_r);		//and message


			
}

int main(int argc, char**argv)
{
	int sockfd,n,routerfd;
	struct sockaddr_in servaddr,cliaddr,router;

	socklen_t len;

	sockfd=socket(AF_INET,SOCK_DGRAM,0);	//here we create UDP socket 
	bzero(&servaddr,sizeof(servaddr));
	servaddr.sin_family = AF_INET;	
	servaddr.sin_addr.s_addr=htonl(INADDR_ANY);	
	servaddr.sin_port=htons(32000);

	bind(sockfd,(struct sockaddr *)&servaddr,sizeof(servaddr));


	
	for (;;) 
	{
		char mesg[5*16+9 + packet_size +1];	
		char new_mesg[5*16+9 + packet_size +1];
		char myip[16];
		char sourceip[16];
		char destip[16];


		len = sizeof(cliaddr);
		n = recvfrom(sockfd,mesg,(5*16+9 + packet_size +1),0,(struct sockaddr*)&cliaddr,&len);	//recieves message

		parse_message(destip, sourceip, myip, mesg,new_mesg);					//modifies message and creates new header

		new_mesg[strlen(new_mesg)]=NULL;


		routerfd=socket(AF_INET,SOCK_DGRAM,0);	//creates UDP socket for sending packet
		bzero(&router,sizeof(router));
		router.sin_family = AF_INET;
		router.sin_addr.s_addr=inet_addr(destip);	
		router.sin_port=htons(32000);


		
		sendto(routerfd,new_mesg,sizeof(new_mesg),0,(struct sockaddr *)&router,sizeof(router)); //sends packet

		printf("---------------\n");
		mesg[n] = 0;
		printf("Received the following:\n");
		printf("%s",mesg);
		printf("---------------\n");

		memset(new_mesg, 0, sizeof (new_mesg) );	// we have to clear all arrays inorder to our program function proparly
		memset(mesg, 0, sizeof (mesg) );			
		memset(myip, 0, sizeof (myip) );
		memset(sourceip, 0, sizeof (sourceip) );
		memset(destip, 0, sizeof (destip) );

	}	
		

}
