#include <sys/socket.h>
#include <netinet/in.h>
#include <stdio.h>
#include <pthread.h>
#include <string.h>
#include <stdbool.h>
#include <stdlib.h>

#define CRC16 0x8005

#define PACKET_SIZE 999
#define HEADER_SIZE 10
#define THREE_DUPLICATE_ACK 3
#define PORT 32000
#define TIMEOUT_VAL 80000
#define SEQ_NUM_LENGTH 8


//this struct is used to give input to threads
typedef struct
{
	int portnum;	//port number
	int index;	//thread index up to switch count
	int sockfd;	//sockets are created before creating threads so sockfd is given to the thread
}receive;

struct timeval mytime; //time value for measuring time in order to detect timeout

long int timeOutMicro[3];  //this holds the time in microseconds
long int timeOutSec[3];    //this holds the time in seconds

int switchCnt;			//switch count for topologies

pthread_mutex_t mutex;		//mutexes for controlling critical sections
pthread_mutex_t timeMutex;

int SeqNum=0;			//sequence number

int seqNUMS[3];			//after sending packet we keep their sequence number in this array. we create at most 3 threads so for 
				//each thread an integer is kept in this array			
			
bool switcheck[3]={true,true,true};	//for each thread again switches are kept to detect for which thread packet is sent and for which thread
					//ACK is received. If switch is not true packets can not be sent

void setSeqNum(int input)
{
	pthread_mutex_lock(&mutex);

	if(input<=SeqNum)
	{
		SeqNum=input; // this function is only for setting sequence number. Sequence number is global for our code and we control everything by using only one
	}			//integer, so we have to protect this function with mutexes.

	pthread_mutex_unlock(&mutex);
}

bool checkChecksum(uint16_t checksum,char* packet)
{
	uint16_t incomingChecksum=0,first=0,second=0;

	first=packet[8];
	second=packet[9];				//this function checks the checksum for received ACKs
							//it takes two checksum characters from packet. Assigns them to two 16 bits integers. 
	first=first<<8;					//with shift and or operator it combines that two integers and produces checksum.
	first=first|second;				//After or operation it returns whether it is true or not

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

		first=data[i];second=data[i+1];		//this is checksum generator
							//like in the previous functions it takes two character from the data part of the packet
		first=first<<8;				//with shift and or operators characters are embedded to a 16bits integer
		first=first|second;			//after adding all 2 characters groups checksum is produced
		out=out+first;	
	}
    i=HEADER_SIZE;
    for(;i<=size;i+=2)
    {
    	uint16_t first=0,second=0;

		first=data[i];second=data[i+1];

    	first=first<<8;
    	first=first|second;
    	out=out+first;
    }
    return ~out;
}

bool isSeqEqual(char* packet,int index)
{
	int i=HEADER_SIZE;
	char seq[SEQ_NUM_LENGTH];
	for(;i<SEQ_NUM_LENGTH+HEADER_SIZE;i++)
	{
		if(packet[i]=='|')
		{
			seq[i-HEADER_SIZE]=NULL; 		//After taking ACK this function compares the sent and received sequence number
			break;					
		}
		seq[i-HEADER_SIZE]=packet[i];
	}
	if(seq[0]==NULL)
	{
		return true;
	}
	int temp=atoi(seq);
	if(temp==seqNUMS[index])
	{
		return true;
	}
	return false;
}

bool checkAck(char* packet,int index)
{
	uint16_t out=~gen_crc16(packet,SEQ_NUM_LENGTH+HEADER_SIZE);
	if(!checkChecksum(out,packet))
	{
		return false;
	}
	else if(!isSeqEqual(packet,index))			//this funcitons checks ACK
	{							//It both checks ACK's checksum and ACK's sequence number
		return false;
	}
	return true;
}

void isTimeOut()
{
	int tempSeq=SeqNum;
	bool ok=false;
	gettimeofday(&mytime, NULL);

	int i=0;
	for(;i<switchCnt;i++)
	{
		long int temp=timeOutSec[i]-mytime.tv_sec;
		if(temp == 0)
		{
			if(mytime.tv_usec-timeOutMicro[i] > TIMEOUT_VAL)	//this function checks timeout
			{							//For each thread we keep a time value which is the send time of packet.
				if(seqNUMS[i] < tempSeq)			//Then we compare each value with the current time
				{
					tempSeq=seqNUMS[i];
					ok=true;
				}
			}
		}

		else if(1000000-timeOutMicro[i]+mytime.tv_usec > TIMEOUT_VAL)
		{
			if(seqNUMS[i] < tempSeq)
			{
				tempSeq=seqNUMS[i];
				ok=true;
			}
		}
	}

	setSeqNum(tempSeq);

	if(ok)
	{
		i=0;
		for(;i<switchCnt;i++)
		{
			switcheck[i]=true;
		}
	}
}

void makeNotAvailable()
{
	switcheck[0]=false;			//we make all the switches false for stopping threads in order not to send packet
	switcheck[1]=false;
	switcheck[2]=false;
}

bool checkAvailable()
{
	int i=0;
	bool dummy=true;
	for(;i<switchCnt;i++)			//we check whether all the switches are ready to send
	{
		dummy=dummy&&switcheck[i];
	}
	return dummy;
}

copySeqNumDelimeter(char *packet)
{
	int i;
	for(i=0;i<8;i++)
	{
		packet[i]='|';		//this is for the last packet.In order to indicate that this is the last packet we set the sequence number to "||||||||"
	}
}

void copySeqNum(char* packet)
{
	char temp[8];
	int i;
	sprintf(temp, "%d", SeqNum);
	for(i=0;i<8;i++)
	{
		if(temp[i]==NULL)
		{
			for(;i<8;i++)  //this funtions is for taking sequence number to the packet
			{
				temp[i]='|';
			}
			break;
		}
	}

	for(i=0;i<8;i++)
	{
		packet[i]=temp[i];
	}
}

void copyChecksum(uint16_t checksum,char* packet)
{
	checksum&=0x0000FFFF;
	packet[8]=(char)((checksum>>8)&(0x000000FF));	//this functions copies the produced checksum to the packet with the help of
	packet[9]=(char)(checksum&(0x000000FF));	//Shift and And operators
}

void* receiver(void* p)
{
	receive* input=(receive*) p;		//this function is for receiving ACKs

	int index=input->index;			//we takes arguments
	int clientfd=input->sockfd,n;

	struct sockaddr_in cliaddr;
	char recvline[PACKET_SIZE];	

	while(1)
	{
		n = recvfrom(clientfd,recvline,999,0,NULL,NULL); 	//in this loop we receive ACKS and checks checksum and sequece numbers
									//according to sequence numbers we set the seqNum 
		if(!checkAck(recvline,index))
		{
			setSeqNum(seqNUMS[index]);
		}
		switcheck[index]=true;
	}	
}

int main(int argc, char**argv)// out.txt switchnum ip1 ip2 ip3
{
	switchCnt=atoi(argv[2]);

	FILE * inputfile;		//for reading input file
	int sockfd[switchCnt],n,len;

	pthread_mutex_init(&mutex, NULL);	//here we initialize mutexes
	pthread_mutex_init(&timeMutex, NULL);

	struct sockaddr_in servaddr[switchCnt]; //sockets
	
	char* remoteip[3];			//this is for holding remote ips of destination's interfaces

	int r=3;
	
	for(;r<3+switchCnt;r++)
	{
		remoteip[r-3]=malloc(sizeof(char)*16);		
		strcpy(remoteip[r-3],argv[r]);
	}

	inputfile=fopen(argv[1],"r");

	char header[HEADER_SIZE]="123456";	//header part of packet

	int k=0;

	for(;k<switchCnt;k++)
	{
		sockfd[k]=socket(AF_INET,SOCK_DGRAM,0);  //here we create UDP socket for sending message 
		bzero(&servaddr[k],sizeof(servaddr[k]));
		servaddr[k].sin_family = AF_INET;
		servaddr[k].sin_addr.s_addr=inet_addr(remoteip[k]);	//we give destination's interfaces ip adress
		servaddr[k].sin_port=htons(PORT+k);
	}

	int x=0;
	receive remote[switchCnt];
	pthread_t threads[switchCnt];
	pthread_attr_t attr[switchCnt];

	for(x=0; x<switchCnt;x++)
	{
		remote[x].portnum=32003+x;
		remote[x].index=x;
		remote[x].sockfd=sockfd[x];		//here we create the threads according to switch count

		pthread_attr_init(&attr[x]);
		pthread_create(&threads[x], &attr[x], receiver, (void*) &remote[x]);

	}

	while(1)
	{
		isTimeOut();					//this while loop is the main loop. In this loop we check timeout and send packets.

		if(checkAvailable())				//before sending messages we check the switch states
		{
			makeNotAvailable();			//after entering we make all the switches of in order to protect sending phase

			int sendnum=0;
			for(;sendnum<switchCnt;sendnum++)	//here we sent packet according to switch count
			{
				char packet[PACKET_SIZE];	//packet

				fseek(inputfile,SeqNum,SEEK_SET);	//we take the datas according to sequence number

				int i=HEADER_SIZE;
				char c;
				for(;i<PACKET_SIZE-1;i++)
				{
					if((c = getc(inputfile)) == EOF)
					{
						packet[i-1]=NULL;

						seqNUMS[sendnum]=SeqNum;
						copySeqNumDelimeter(packet);
						SeqNum+=i-HEADER_SIZE;

						uint16_t checksum=(gen_crc16(packet,(i-1)));  //these codes and the following codes are duplicates
											//we read until EOF character, create checksum, create sequence number and then copy them to the packet
											//When packet is ready we sent it to the destination
						copyChecksum(checksum,packet);

						sendto(sockfd[sendnum],packet,i, 0, (struct sockaddr *)&(servaddr[sendnum]),sizeof(servaddr[sendnum]));

						fclose(inputfile);
						return 0;
					}
					packet[i]=c;
				}

				packet[PACKET_SIZE-1]=NULL;

				seqNUMS[sendnum]=SeqNum;
				copySeqNum(packet);
				SeqNum+=PACKET_SIZE-HEADER_SIZE-1;

				uint16_t checksum=(gen_crc16(packet,(PACKET_SIZE-1)));

				copyChecksum(checksum,packet);

				sendto(sockfd[sendnum],packet,PACKET_SIZE, 0, (struct sockaddr *)&(servaddr[sendnum]),sizeof(servaddr[sendnum]));

				gettimeofday(&mytime, NULL);

				timeOutSec[sendnum]=mytime.tv_sec;
				timeOutMicro[sendnum]=mytime.tv_usec;
			}
		}
	}
	for (x = 0; x < 3; x++)
	{
		pthread_join(threads[x], NULL);			//this is for closing threads
	}
	return 0;
}
