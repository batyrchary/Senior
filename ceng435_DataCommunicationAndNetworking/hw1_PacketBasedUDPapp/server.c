/* Sample UDP server */
#include <sys/socket.h>
#include <netinet/in.h>
#include <stdio.h>

struct timeval tval_before; //time struct for learning system type

int packet_size=5; //message length to be added after the header
void parse_message(char* destip, char* sourceip, char* myip, char* mesg, char* new_mesg) //this function parses the received message
{
		char c;
		int index=0;
		char all_ips[5][16];	//all_ips keeps the all IPs of all nodes
		
		int d_index;
		char dd_index[2];
		char ss_index[2]; ss_index[0]=mesg[0];	ss_index[1]='\0';  //these are the indexes for the IPs in the header
		char mm_index[2]; mm_index[0]=mesg[2]; 	mm_index[1]='\0';

		int s_index=mesg[0] - '0';
		int m_index=mesg[2] - '0';

		index=4;
		int j,i;
		for( j=0; j<5; j++)
		{
			for( i=0; ; i++)
			{
				c=mesg[index];
				if(c!='|')
				{	
				
					all_ips[j][i]=c;		//In this loop we reads IPs from the received message and takes them to the all_ips
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

		strcpy(myip,all_ips[m_index]);

		if(m_index==4)
		{
			d_index=3;			//this is additional to the router code which makes our router a server.
		}					//if index is 4 so if it is a server it makes the destination router D so 3.

		else if(m_index<s_index)
		{
			d_index=m_index-1;
		}
		else if(m_index>s_index)
		{
			d_index=m_index+1;
		}

		dd_index[0]=d_index+'0';		
		dd_index[1]='\0';

		strcpy(destip,all_ips[d_index]);	//it takes the destination IP and source IP
		strcpy(sourceip,all_ips[s_index]);

		char mesg_r[5*16+9 + packet_size +1];
		for(i=0;mesg[index]!='\0'; index++,i++)
		{
			mesg_r[i]=mesg[index];		//copies message part of the packet
		}
		mesg_r[i]=NULL;

		strcat(new_mesg,mm_index);
		strcat(new_mesg,"|");
		strcat(new_mesg,dd_index);		//here we concetanate indexes and IPs and we create header
		strcat(new_mesg,"|");
		strcat(new_mesg,all_ips[0]);
		strcat(new_mesg,"|");
		strcat(new_mesg,all_ips[1]);
		strcat(new_mesg,"|");
		strcat(new_mesg,all_ips[2]);
		strcat(new_mesg,"|");
		strcat(new_mesg,all_ips[3]);
		strcat(new_mesg,"|");
		strcat(new_mesg,all_ips[4]);
		strcat(new_mesg,"|");
		strcat(new_mesg,mesg_r);		
}

int main(int argc, char**argv)
{
	int sockfd,n,routerfd;
	struct sockaddr_in servaddr,cliaddr,router;

	socklen_t len;

	sockfd=socket(AF_INET,SOCK_DGRAM,0);
	bzero(&servaddr,sizeof(servaddr));		//here we create UDP socket for receiving message to the next hop
	servaddr.sin_family = AF_INET;
	servaddr.sin_addr.s_addr=htonl(INADDR_ANY);	//we set the address to receive from any address
	servaddr.sin_port=htons(32000);

	bind(sockfd,(struct sockaddr *)&servaddr,sizeof(servaddr)); //here we bind the socket for receiving
	
	for (;;) 
	{
		char mesg[5*16+9 + packet_size +1];
		char new_mesg[5*16+9 + packet_size +1];
		char myip[16];
		char sourceip[16];
		char destip[16];


		len = sizeof(cliaddr);
		n = recvfrom(sockfd,mesg,(5*16+9 + packet_size +1),0,(struct sockaddr*)&cliaddr,&len);  // this receives the message

		parse_message(destip, sourceip, myip, mesg,new_mesg);	//we call the parsing function

		new_mesg[strlen(new_mesg)]=NULL;

		routerfd=socket(AF_INET,SOCK_DGRAM,0);
		bzero(&router,sizeof(router));			//here we create UDP socket for sending message to the next hop
		router.sin_family = AF_INET;
		router.sin_addr.s_addr=inet_addr(destip);	//we give next hop's ip adress
		router.sin_port=htons(32000);

		if(new_mesg[strlen(new_mesg)-2]=='*')		
		{
			sendto(routerfd,new_mesg,sizeof(new_mesg),0,(struct sockaddr *)&router,sizeof(router));	//here if the server receives the end of the message it sends the acknowledgement
			gettimeofday(&tval_before, NULL);							//message back. '*' indicates the end of message.
			printf("Time: %ld.%06ld\n", (long int)tval_before.tv_sec, (long int)tval_before.tv_usec);//this prints the system time for calculating end-to-end delay
		}

		printf("---------------\n");
		mesg[n] = 0;
		printf("Received the following:\n");
		printf("%s",mesg);
		printf("---------------\n");

		memset(new_mesg, 0, sizeof (new_mesg) );
		memset(myip, 0, sizeof (myip) );
		memset(mesg, 0, sizeof (mesg) );		//clears the arrays
		memset(sourceip, 0, sizeof (sourceip) );
		memset(destip, 0, sizeof (destip) );						
	}	
}
