/* Sample UDP client */
#include <sys/socket.h>
#include <netinet/in.h>
#include <stdio.h>
#include <string.h>

int main(int argc, char**argv)
{
	struct timeval tval_before;	//time struct for learning system type
	FILE * inputfile;		//for reading input file
	int sockfd,n,len,clientfd;

	struct sockaddr_in servaddr,cliaddr; //sockets

	char myip[16];
	char remoteip[16];

	if (argc != 2)
	{
		printf("need input.txt\n");
		exit(1);
	}

	inputfile=fopen(argv[1],"r");

	int packet_size=5;			//message length to be added after the header
	char all_ips[5][16];			//all_ips keeps the all IPs of all nodes
	char header[5*16+7+2];			//header keeps source IP index and destination IP index and the IPs of all nodes separated by the '|' character
	char packet[packet_size];		//packet keeps the message
	char sendline[5*16+7+2 + packet_size +1],c;	//message to be sent. header and packet are concetanated before sending message.
	char recvline[5*16+7+2 + packet_size +1];	//acknowledgement message to be received
	sendline[0]='\0';

	int i;
	if(inputfile)
	{
		int j;
		for( j=0; j<5; j++)
		{
			for( i=0; ; i++)
			{
				c=(c = getc(inputfile));		//In this loop we reads IPs from the input file and takes them to the all_ips
				if(c!='|')
				{			
					all_ips[j][i]=c;
				}
				else
				{
					all_ips[j][i]='\0';
					break;
				}
			}
		}

		strcpy(myip,all_ips[0]);
		strcpy(remoteip,all_ips[1]);


		strcat(header,"0|1|");
		strcat(header,all_ips[0]);		//here we concetanate indexes and IPs and we create header
		strcat(header,"|");
		strcat(header,all_ips[1]);
		strcat(header,"|");
		strcat(header,all_ips[2]);
		strcat(header,"|");
		strcat(header,all_ips[3]);
		strcat(header,"|");
		strcat(header,all_ips[4]);
		strcat(header,"|");

		sockfd=socket(AF_INET,SOCK_DGRAM,0);  //here we create UDP socket for sending message to the next hop
		bzero(&servaddr,sizeof(servaddr));	
		servaddr.sin_family = AF_INET;
		servaddr.sin_addr.s_addr=inet_addr(remoteip);	//we give next hop's ip adress
		servaddr.sin_port=htons(32000);
	
		clientfd=socket(AF_INET,SOCK_DGRAM,0);
		bzero(&cliaddr,sizeof(cliaddr));	//here we create UDP socket for receiving message to the next hop
		cliaddr.sin_family = AF_INET;
		cliaddr.sin_addr.s_addr=htonl(INADDR_ANY);	//we set the address to receive from any address
		cliaddr.sin_port=htons(32000);
		bind(clientfd,(struct cliaddr *)&cliaddr,sizeof(cliaddr)); //here we bind the socket for receiving
		

		int index=0;
		while ((c = getc(inputfile)) != EOF)
		{
			packet[index]=c;
			index++;		
			if(index==packet_size)
			{
				packet[index]='\0';
				strcat(sendline,header);		//In this loop we take message and concetanate with header.
				strcat(sendline,packet);

				sendto(sockfd,sendline,strlen(sendline), 0, (struct sockaddr *)&servaddr,sizeof(servaddr));	//then we send the message here		
				memset(sendline, 0, sizeof (sendline) ); //we clear the arrays.
				memset(packet, 0, sizeof (packet) );

				index=0;			
			}
			packet[index]='\0';
		}

		if(index>1)
		{
			packet[index]='\0';
			strcat(sendline,header);		//this part sends the remaning characters
			strcat(sendline,packet);
			sendto(sockfd,sendline,strlen(sendline), 0, (struct sockaddr *)&servaddr,sizeof(servaddr));		
		}		

		gettimeofday(&tval_before, NULL);
		printf("Time: seconds %ld miliseconds %ld\n", (long int)tval_before.tv_sec, (long int)tval_before.tv_usec); //this writes the time after the last packet sent.

		printf("*************\n");
		n = recvfrom(clientfd,recvline,sizeof(recvline),0,(struct cliaddr*)&cliaddr,sizeof(cliaddr)); // this receives the acknowledgement message
		recvline[n]=0;
		printf("%s\n",recvline);
		printf("*************\n");	
	}
}

