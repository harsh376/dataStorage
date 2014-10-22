/**
 * @file
 * @brief This file implements a "very" simple sample client.
 * 
 * The client connects to the server, running at SERVERHOST:SERVERPORT
 * and performs a number of storage_* operations. If there are errors,
 * the client exists.
 */

#include <stdlib.h>
#include <unistd.h>
#include <sys/types.h>
#include <sys/socket.h>
#include <arpa/inet.h>
#include <netdb.h>
#include <string.h>
#include <assert.h>
#include <signal.h>


#include <errno.h>
#include <stdio.h>
#include <time.h>
#include <string.h>
#include <ctype.h>
#include "storage.h"

#define SERVERHOST "localhost"
#define SERVERPORT 1111
#define SERVERUSERNAME "admin"
#define SERVERPASSWORD "dog4sale"
#define TABLE "marks"
#define KEY "ece297"
#define LENGTH 64
#define LOGGING 0
#define LOAD_CENSUS 2

// LOGGING:
// 0: no logging
// 1: logging to stdout
// 2: logging to file

// LOAD_CENSUS: 
// 0: don't load census
// 1: load census on server side, 
// 2: load census on client side


// struct storage_record obj1[MAX_RECORDS_PER_TABLE];
// struct storage_record* objPtr[MAX_RECORDS_PER_TABLE];



// struct storage_record obj2[MAX_RECORDS_PER_TABLE];

extern FILE *log;
void *conn = NULL; // Needed to satisfy function calls below
struct storage_record r;
/**
 * @brief Stores the line entered by the user
 *
 * @param s Character array that stores the line provided by the user
 * @param arraySize The size of the character array "s"
 * 
 * Takes each character entered by the user from a line and stores them
 * in a character array. It stops storing characters immediately prior
 * to the new line ('\n') character in order to avoid storing it.
 */

void safegets (char s[], int arraySize)
{
    int i = 0, maxIndex = arraySize - 1;
    char c;
    while (i < maxIndex && (c = getchar()) != '\n')
    {
        s[i] = c;
        i = i + 1;
    }
    s[i] = '\0';
}

/**
 * @brief Automates the process of loading the Census Data into the
 *		  Database.
 *
 * @param table The table name
 * @param line The line of text from the input file
 * @param ct Temporary counter that points to the index in the object
 * 		  array that needs to be inserted into the database. The 
 * 		  input census data is stored in an array of objects, which
 *		  has the members key, record and next.
 *
 * Initially, it takes the line provided by the user from an external
 * source and breaks it up into two tokens: key and respective value.
 * It then inputs the key-value pair and the table name into storage_set,
 * which inserts the value in the database.
 */

int processCensus(char* table, char *line)
{

	// Extract key and value
    // char name[1024];
    // char val[1024];
    //int items = sscanf(line, "%s %s\n", name, val);

    char* pp = line;
    char* name = strtok(pp, " ");
    char* val = strtok(NULL, "\n");

    printf("Key: %s, Value: %sEND\n", name, val);
  
    char key[MAX_KEY_LEN];
    char value[MAX_VALUE_LEN];

    if(strlen(name)<=MAX_KEY_LEN && strlen(val)<=MAX_VALUE_LEN)
    {
    	//strncpy(key, name, MAX_KEY_LEN);
    	//strncpy(value, val, MAX_VALUE_LEN);

   		strncpy(r.value, val, MAX_VALUE_LEN);
   		printf("R value: %sEND\n", r.value);

   		// objPtr[ct] = &obj1[ct];
   		// printf("%s, %s\n", key, r.value);

   		// printf("set %s\n", obj1[ct].value);


   		int status = storage_set(table, name, &r, conn);

   		// printf("got %s\n", obj2[ct].value);
   		
    }

    // printf("%s, %s\n", name, value);
 

    // printf("%s, %s\n", name, obj[ct].value);
	
    return 0;
}

/**
 * @brief Reads the census from the database
 *
 * @param table The table name
 * @param line The line of text from the input file
 * @param ct Temporary counter that points to the index in the object
 * 		  array that needs to be inserted into the database. The 
 * 		  input census data is stored in an array of objects, which
 *		  has the members key, record and next.
 */

int readCensus(char* table, char *line)
{

	// Extract key and value
    char name[1024];
    // char val[1024];
    int items = sscanf(line, "%s\n", name);

    char key[MAX_KEY_LEN];

    if(strlen(name)<=MAX_KEY_LEN)
    {
    	strncpy(key, name, MAX_KEY_LEN);

   		int status = storage_get(table, key, &r, conn);
   		printf("%s, %s\n", key, r.value);
   		
    }

    return 0;
}

/**
 * @brief Provides an interface that allows for the interaction
 *		 between the user and the server.
 *
 * @param argc Number of arguments passed when the client shell
 * 		 is run
 * @param argv An array that contains the tokens from the command
 * 		 line 
 *
 * Initially and repeatedly prompts the user with multiple commands
 * until the user selects the disconnect command, which exits the 
 * the program. The interface also allows the user to connect to the
 * server and store, modify, delete and retrieve data.
 */

int main(int argc, char *argv[]) {

  // struct storage_record r;
  int i;
  int connected = -1;
  int authenticated = -1;


  if(LOGGING == 2) {
	  time_t rawtime;
	  struct tm * timeinfo;
	  time (&rawtime);
	  timeinfo = localtime (&rawtime);

	  int year = timeinfo->tm_year + 1900;
	  int month = timeinfo->tm_mon + 1;
	  int day = timeinfo->tm_mday;
	  int hour = timeinfo->tm_hour;
	  int min = timeinfo->tm_min;
	  int sec = timeinfo->tm_sec;
	  char filename[50];
	  sprintf(filename, "Client-%d-%02d-%02d-%02d-%02d-%02d.log", year, month, day, hour, min, sec);

	  log = fopen(filename, "w");
  } else if(LOGGING == 1)
	  log = stdout;

  int cont = 1; 

  do {
	  // Print out "menu" for the user
	  printf("--------------------\n");
	  printf("1) Connect\n");
	  printf("2) Authenticate\n");
	  printf("3) Get\n");
	  printf("4) Set\n");
	  printf("5) Disconnect\n");
	  printf("6) Exit\n");
	  printf("7) Query\n");
	  printf("8) Bulkload\n");
	  printf("9) Testing\n");
	  printf("10) Transaction Abortion\n");
	  printf("--------------------\n");

	  // Retrieve selection from user

 	  printf("Please enter your selection: ");
  	  
  	  char selection[100];

	  safegets(selection, 100);


	  // scanf("%d", &i);
	  // getchar();

	  if(strcmp(selection, "1") == 0) 
	  {

		  	char serverHost_[MAX_HOST_LEN];
			int serverPort_;
			char temp[MAX_PORT_LEN];
			
			printf("Please input the hostname: ");
			safegets(serverHost_, MAX_HOST_LEN);
			
			printf("Please input the port: ");
			safegets(temp, MAX_PORT_LEN);
			
			serverPort_ = atoi(temp);
			
			// Connect to server  
			conn = storage_connect(serverHost_, serverPort_);
			if(!conn)
			{
				printf("Cannot connect to server @ %s:%d. Error code: %d.\n",
				serverHost_, serverPort_, errno);
				//return -1;
			}
			else
			{
				connected = 0;
				printf("storage_connect: successful.\n");
			}

	  } 


	  else if(strcmp(selection, "2") == 0)
	  {
			char serverUsername_[64];
			char serverPassword_[64];
		
			printf("Please input username: ");
			safegets(serverUsername_, 64);

			printf("Please input password: ");
			safegets(serverPassword_, 64);

			// Authenticate the client.
			 int status = storage_auth(serverUsername_, serverPassword_, conn);
			 authenticated = status;

			if(status != 0) {
				printf("storage_auth failed with username '%s' and password '%s'. " \
					   "Error code: %d.\n", serverUsername_, serverPassword_, errno);
				if(conn == NULL)
					printf("No valid connection\n");
				continue;
				//else
					//storage_disconnect(conn);
			} 
			else
				printf("storage_auth: successful.\n");




	  }

	  else if(strcmp(selection, "3") == 0)
	  {
	  		// struct storage_record r;
		  	char table_[20];
		  	char key_[20];
			
			printf("Please input table: ");
			safegets(table_, 20);
			printf("Please input key: ");
			safegets(key_, 20);


			// Issue storage_get
			int status = storage_get(table_, key_, &r, conn);
			if(status != 0){
				printf("storage_get failed. Error code: %d.\n", errno);
				if(conn == NULL)
					printf("No valid connection\n");
				//else
					//storage_disconnect(conn);
			} 
			else
				printf("storage_get: the value returned for key '%s' is '%s'.\n", key_, r.value);



	  }

	  else if(strcmp(selection, "4") == 0)
	  {

	  		
		  	char value_[800];
			char table_[20];
			char key_[20];
			
			printf("Please input table: ");
			safegets(table_, 20);
			printf("Please input key: ");
			safegets(key_, 20);
			printf("Please input value: ");
			safegets(value_, 800);


			// Issue storage_set
			strncpy(r.value, value_, sizeof r.value);
			int status = storage_set(table_, key_, &r, conn);
			if(status != 0) {
				printf("storage_set failed. Error code: %d.\n", errno);
				if(conn == NULL)
					printf("No valid connection\n");
				//else
					//storage_disconnect(conn);
			} 

			else
				printf("storage_set: successful.\n");


	  }

	  else if(strcmp(selection, "5") == 0)
	  {
		  // Disconnect from server
		  int status = storage_disconnect(conn);
		  if(status != 0)
		  {
		  	printf("storage_disconnect failed. Error code: %d.\n", errno);
	  	  }
	  	  else
	  	  	printf("disconnected server.\n");

	  }


	else if(strcmp(selection, "6")==0)
	{
		printf("Exiting Server...\n");
		cont=0;
		// return 0;
	}

	else if(strcmp(selection, "7")==0)
	{

		char table_s[20] = "subwayLines";
		char value_1[800] = " name bloor danforth, stops 21, kilometres 76 ";
		char key_1[20] = "toronto";

		char value_2[800] = " name dundas, stops 12, kilometres 100 ";
		char key_2[20] = "waterloo";

		char value_3[800] = " name bay, stops 55, kilometres 7 ";
		char key_3[20] = "van";

		char value_4[800] = " name uni, stops 25, kilometres 98";
		char key_4[20] = "dc";

		// Issue storage_set
		strncpy(r.value, value_1, sizeof r.value);
		int status1 = storage_set(table_s, key_1, &r, conn);
		
		strncpy(r.value, value_2, sizeof r.value);
		status1 = storage_set(table_s, key_2, &r, conn);

		strncpy(r.value, value_3, sizeof r.value);
		status1 = storage_set(table_s, key_3, &r, conn);

		strncpy(r.value, value_4, sizeof r.value);
		status1 = storage_set(table_s, key_4, &r, conn);


		int max_keys = 5;

		char table_[MAX_TABLE_LEN];
		char predicates_[MAX_CONFIG_LINE_LEN];
		
		char *keys_[max_keys];


		printf("Please input table: ");
		safegets(table_, MAX_TABLE_LEN);
		
		printf("Please input the predicates: ");
		safegets(predicates_, MAX_CONFIG_LINE_LEN);
		

		// Issue storage_query
		int status = storage_query(table_, predicates_, keys_, max_keys, conn);
		if(status != 0)
		{
			printf("storage query failed. Error code: %d.\n", errno);
			if(conn == NULL)
				printf("No valid connection\n");
			//else
			//storage_disconnect(conn);
		} 

		else
		{
			printf("storage_query: successful.\n");
			// int i;
			// for(i=0; i<max_keys; i++)
			// 	printf("%s, ", keys_[i][0]);
		}

	}

	else if(strcmp(selection, "8")==0)
	{

	  	  if(LOAD_CENSUS != 2)
	  	  	continue;

	  	  if(connected == -1)
	  	  {
	  	  	printf("Cannot load census data: No connection\n");
	  	  	continue;
	  	  }

	  	  if(authenticated == -1)
	  	  {
	  	  	printf("Cannot load census data: No authentication\n");
	  	  	continue;
	  	  }


	  	  // can load data
	  	  printf("Loading and then reading census data from client side...\n");


     	  int error_occurred = 0;

		  char data[20] = "../data/input";

		  FILE *file = fopen(data, "r");
		  if (file == NULL)
		  	error_occurred = 1;

		  char table[MAX_TABLE_LEN] = "census";
/*
		  clock_t start, end;
		  double cpu_time_used;
		  start = clock();
		  struct timeval start_time, end_time;
*/


		  struct timeval start_time, end_time;
		  gettimeofday(&start_time, NULL);
		  
		  int n = 100000;
		  int n1 = 0;
		  int n2 = 0;

		  // Process the config file.
		  while (!error_occurred && !feof(file) && n1<=n) 
		  {
		   	// Read a line from the file.
		   	char line[1024];
		   	char *l = fgets(line, sizeof line, file);

	        // Process the line.
	        if (l == line)
	        {
	            processCensus(table, line);	        
	        }
	        else if (!feof(file))
	            error_occurred = 1;

	        n1++;

		  }

		  fclose(file);

		  // ..................................

		  int error_occurred1 = 0;
		  char data1[20] = "../data/input";
		  FILE *file1 = fopen(data1, "r");
		  if (file1 == NULL)
		  	error_occurred1 = 1;
		  char table1[MAX_TABLE_LEN] = "census";

		  // Process the config file.
		  while (!error_occurred1 && !feof(file1) && n2<=n) 
		  {
		   	// Read a line from the file.
		   	char line[1024];
		   	char *l = fgets(line, sizeof line, file);

	        // Process the line.
	        if (l == line)
	        {
	            readCensus(table, line);
	        }
	        else if (!feof(file1))
	            error_occurred = 1;

	        n2++;
		  }
		  
		  gettimeofday(&end_time, NULL);
		  unsigned int t1 = end_time.tv_sec-start_time.tv_sec;
		  unsigned int t2 = end_time.tv_usec-start_time.tv_usec;
		  
		  double t = (double)t1 + (double)(t2/1000000);
		  
		  printf("time taken (seconds) = %lf\n", (double)t1);
		  printf("time taken (mi-seconds) = %lf\n", (double)t2);
		  printf("time taken (total) = %lf\n", (double)t);
/*
		  gettimeofday(&end_time, NULL);
		  unsigned int t = end_time.tv_usec - start_time.tv_usec;
		  printf("end to end loading/reading time: %lf seconds\n", (double)t/1000000);
		  end = clock();
		  cpu_time_used = ((double) (end - start)) / CLOCKS_PER_SEC;
		  printf("%lf\n", cpu_time_used);
*/
		  fclose(file);

	  }

	  else if(strcmp(selection, "9") == 0)
	  {


			struct timeval start_time, end_time;
			gettimeofday(&start_time, NULL);
	

/*
	  		clock_t start, end;
		  	double cpu_time_used;
		  	start = clock();
		  	struct timeval start_time, end_time;
	
	*/
		  	int i =0;

		  	char table[MAX_TABLE_LEN] = "census";
		  	char key[MAX_KEY_LEN];

		  	while (i < 100000) {
		  		sprintf(key, "key%d", i);
		  		printf("%s\n", key);
		  		int status = storage_get(table, key, &r, conn);
		  		i++;
		  	}

			gettimeofday(&end_time, NULL);
			unsigned int t1 = end_time.tv_sec-start_time.tv_sec;
			unsigned int t2 = end_time.tv_usec-start_time.tv_usec;
			  
			double t = (double)t1 + (double)(t2/1000000);
			  
			printf("time taken (seconds) = %lf\n", (double)t1);
			  


/*
			gettimeofday(&end_time, NULL);
			unsigned int t = end_time.tv_usec - start_time.tv_usec;
			printf("end to end TESTING time: %lf seconds\n", (double)t/1000000);
			end = clock();
			cpu_time_used = ((double) (end - start)) / CLOCKS_PER_SEC;
			printf("%lf\n", cpu_time_used);

*/
	  }
	 	else if(strcmp(selection, "10")==0)
		{

		    if(connected == -1)
		    {
		     printf("Cannot load census data: No connection\n");
		     continue;
		    }

		    if(authenticated == -1)
		    {
		     printf("Cannot load census data: No authentication\n");
		     continue;
		    }


		    int error_occurred = 0;

		  char data[20] = "../data/input";

		  FILE *file = fopen(data, "r");
		  if (file == NULL)
		   error_occurred = 1;

		  char table[MAX_TABLE_LEN] = "census";



		  int n = 100000;
		  int n1 = 0;
		  int n2 = 0;
		  int abortionCounter = 0;
		  // Process the config file.
		  while (!error_occurred && !feof(file) && n1<=n) 
		  {
		      // Read a line from the file.
		      char line[1024];
		      char *key;
		      char *value;
		      char *l = fgets(line, sizeof line, file);

		         // Process the line.
		         if (l == line)
		         {
		          	// key = strtok(l, " ");
		          	// value = strtok(NULL, "\0\n ");

					// Extract key and value
					char name[1024];
					// char val[1024];
					int items = sscanf(line, "%s\n", name);


				    char key[MAX_KEY_LEN];

				    if(strlen(name)<=MAX_KEY_LEN)
				    {
				    	strncpy(key, name, MAX_KEY_LEN);
				    	struct storage_record r;
				   		int getStatus = storage_get(table, key, &r, conn);
				   		printf("%s, %s\n", key, r.value);
				   		
				   		sleep(0.001); //Delay for 1 ms

						char* pp = line;
						char* n1 = strtok(pp, " ");
						char* val = strtok(NULL, "\n");

						strncpy(r.value, val, MAX_VALUE_LEN);
						printf("R value: %sEND\n", r.value);

						int setStatus = storage_set(table, name, &r, conn);

						if (setStatus == -1 && errno == ERR_TRANSACTION_ABORT) 
						{
							abortionCounter++;
							// printf("Abortion happened - %d", abortionCounter);
						}


				    }



		      //        // struct storage_record r;
		    		//  storage_get(table, key, &r, conn);
		    		//  sleep(0.001); //Delay for 1 ms
		    		// // Update the population.  Do not change r.metadata.
		    		// strcpy(r.value, value);
		   		 // 	int status = storage_set(table, key, &r, conn);     
		    		// if (status == -1 && errno == ERR_TRANSACTION_ABORT) 
		    		// {
		     	// 		abortionCounter++;
		     	// 		printf("Abortion happened - %d", abortionCounter);
				    // }
		   		}
		   

		   		else if (!feof(file))
		    		error_occurred = 1;

		        n1++;

		    }

		    fclose(file);

		    printf("total abortions = %d\n", abortionCounter);

		}

  }while(cont == 1);



  return 0;

}

/*

Pseudocode for transaction abort rate:
	while (bulk loads data){
		//Initialize Table (census)
		//Initialize multiple key names
		//Get inputs from a file by using line-by-line loop (bulk load)
		//Keep records same as the records that are returned from the get
		//Force a delay immediately after get and before set in order to allow for abortion rate
	
	// Get the record.
	// Storage_get (table name, key, conn, record (&r))


	struct storage_record r;
	storage_get("census", "Toronto", &r, conn);

	// Update the population.  Do not change r.metadata.
	strcpy(r.value, "Province Ontario, Population 2000000, Change 9, Rank 1");
	int status = storage_set("census", "Toronto", &r, conn);
	if (status == -1 && errno == ERR_TRANSACTION_ABORT) {
    printf("Transaction aborted.\n");
}
*/
