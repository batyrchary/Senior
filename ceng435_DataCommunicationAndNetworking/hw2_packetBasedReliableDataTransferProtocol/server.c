#include <signal.h>
#include <sys/socket.h>
#include <netinet/in.h>
#include <stdio.h>
#include <stdbool.h>

#define HEADER_SIZE 10
#define PACKET_SIZE 999
#define SEQ_NUM_LENGTH 8
typedef struct
{
	int port;

} ip;
char output[5000001];	//this is output buffer. we take received datas to this buffer

pthread_mutex_t mutex;

bool finito=false;	//this is finish flag

void copyChecksum(uint16_t checksum,char* packet)
{
	checksum&=0x0000FFFF;
	packet[8]=(char)((checksum>>8)&(0x000000FF));	//this functions copies the produced checksum to the packet with the help of
	packet[9]=(char)(checksum&(0x000000FF));	//Shift and And operators
}

bool checkChecksum(uint16_t checksum,char* packet)
{
	uint16_t first=0,second=0;
	
	first=packet[8];
	second=packet[9];			//this function checks the checksum for received ACKs
						//it takes two checksum characters from packet. Assigns them to two 16 bits integers.
	first=first<<8;				//with shift and or operator it combines that two integers and produces checksum.
	first=first|second;			//After or operation it returns whether it is true or not

	if((first|checksum)==0xFFFF)
	{	
		return true;
	}
	return false; 
}

uint16_t gen_crc16(char* data, int size)
{
    uint16_t out = 0;
    int i=0;
    for(;i<SEQ_NUM_LENGTH;i+=2)
	{
		uint16_t first=0,second=0;

		first=data[i];second=data[i+1];

		first=first<<8;
		first=first|second;		//this is checksum generator
		out=out+first;			//like in the previous functions it takes two character from the data part of the packet
	}					//with shift and or operators characters are embedded to a 16bits integer
						//after adding all 2 characters groups checksum is produced
    i=HEADER_SIZE;
    for(;i<size;i+=2)
    {
    	uint16_t first=0,second=0;

		first=data[i];second=data[i+1];

    	first=first<<8;
    	first=first|second;
    	out=out+first;
    }
    return out;
}
uint16_t gen_crc162(char* data, int size)
{
    uint16_t out = 0;
    int t=0;
    int i=0;
    for(;i<SEQ_NUM_LENGTH;i+=2)
	{
		uint16_t first=0,second=0;

		first=data[i];second=data[i+1];			//this is awkward copy of of previous function

		first=first<<8;
		//printf("%c%c",data[i],data[i+1]);
		first=first|second;
		out=out+first;
		t++;
	}

    i=HEADER_SIZE;
    for(;i<size;i+=2)
    {
    	uint16_t first=0,second=0;

		first=data[i];second=data[i+1];

    	first=first<<8;
    	first=first|second;
    	out=out+first;
    }
    return out;
}

int onkat(int degree)
{
	int ret=1;
	int i;
	for(i=0; i<degree;i++)		//this is for converting sequence number to integer
	{
		ret=ret*10;
	}
	return ret;

}
void* receiver(void* p)
{
	pthread_mutex_lock(&mutex);
		ip* remote = (ip*) p;
	
		int sockfd,n;
		struct sockaddr_in servaddr,cliaddr;		//this is for creating receiving sockets
		socklen_t len;
		char mesg[999];
		sockfd=socket(AF_INET,SOCK_DGRAM,0);
		bzero(&servaddr,sizeof(servaddr));
		servaddr.sin_family = AF_INET;
		servaddr.sin_addr.s_addr=htonl(INADDR_ANY);
		servaddr.sin_port=htons(remote->port);
		bind(sockfd,(struct sockaddr *)&servaddr,sizeof(servaddr));
	pthread_mutex_unlock(&mutex);
	for (;;) 
	{
		if(finito)
		{
			break;
		}
		len = sizeof(cliaddr);
		n = recvfrom(sockfd,mesg,999,0,(struct sockaddr*)&cliaddr,&len); //this is for receiving packets

		if(checkChecksum(gen_crc16(mesg,n-1),mesg))
		{
			if(mesg[0]=='|')		//after receiving packets we checks the checksum if checksum is correct we take the data to the buffer
			{				//then we send ACK message
				int p=0;
				for(; p<n-10; p++)
				{
					output[4999280+p]=mesg[p+10];
					output[5000000-1]='\n';
				}
				output[5000000]=NULL;
				int l;
				for(l=0;l<8;l++)
				{
					mesg[l+HEADER_SIZE]=mesg[l];
				}
				uint16_t checker=gen_crc162(mesg,HEADER_SIZE+SEQ_NUM_LENGTH);
				copyChecksum(~checker,mesg);
				//printf("checker=%x\n",~checker);
				mesg[HEADER_SIZE+SEQ_NUM_LENGTH]=NULL;

				sendto(sockfd,mesg,HEADER_SIZE+SEQ_NUM_LENGTH+1,0,(struct sockaddr *)&cliaddr,sizeof(cliaddr));
				finito=true;
				break;
			}
			else
			{
				pthread_mutex_lock(&mutex);
				int ch;
				int degree=0;
				int seq_n=0;
				for(ch=7; ch>=0; ch--)
				{
					if(mesg[ch]!='|')
					{
						seq_n=seq_n+(mesg[ch] - '0')*onkat(degree);
						degree++;
					}
				}

				for(ch=10; ch<n; ch++)
				{
					output[seq_n+ch-10]=mesg[ch];
				}

				pthread_mutex_unlock(&mutex);

				int l;
				for(l=0;l<8;l++)
				{
					mesg[l+HEADER_SIZE]=mesg[l];
				}

				uint16_t checker=gen_crc162(mesg,HEADER_SIZE+SEQ_NUM_LENGTH);
				copyChecksum(~checker,mesg);

				mesg[HEADER_SIZE+SEQ_NUM_LENGTH]=NULL;

				sendto(sockfd,mesg,HEADER_SIZE+SEQ_NUM_LENGTH+1,0,(struct sockaddr *)&cliaddr,sizeof(cliaddr));

			}
		}
	}
	pthread_exit(0);
}

int main(int argc, char**argv)
{
	int number_of_switch=atoi(argv[1]);

	int i=0;	
	
	ip remote[number_of_switch];
	pthread_t threads[number_of_switch];
	pthread_attr_t attr[number_of_switch];

	pthread_mutex_init(&mutex, NULL);

	for(i=0; i<number_of_switch; i++)
	{
		remote[i].port=32000+i;					//we create thread according to number of switches
		pthread_attr_init(&attr[i]);
		pthread_create(&threads[i], &attr[i], receiver, (void*) &remote[i]);
	}

	while(1)
	{
		if(finito)
		{
			FILE *outputFile = fopen("output.txt", "w");	//After threads are finished we write the buffer to the file then finishes the process 
			fprintf(outputFile, "%s", output);
			fclose(outputFile);

			int a;
			for(a=0;a<number_of_switch;a++)
			{
				pthread_kill(threads[a], SIGTERM);
			}
			break;
		}
	}
	return 0;
}
