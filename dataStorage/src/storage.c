/**
 * @file
 * @brief This file contains the implementation of the storage server
 * interface as specified in storage.h.
 */

#include <stdlib.h>
#include <stdio.h>
#include <unistd.h>
#include <string.h>
#include <ctype.h>
#include <sys/types.h>
#include <sys/socket.h>
#include <netdb.h>
#include <errno.h>
#include "storage.h"
#include "utils.h"

#define LOGGING 0

int connected = 0; // Used to check if the connection is valid
int authenticated = 0; // Used to check if the connection has been authenticated

/**
 * @brief This is the function used to create a connection.
 *
 * @param hostname A character array containing the user-entered hostname
 * @param port Is the user-entered port number
 * @return Return a pointer to the connection if sucessful, NULL if unsuccessful
 *
 * The function takes in a hostname and a port and uses it to create a socket, 
 * get information about the server, and then connect to the server.
 */
void* storage_connect(const char *hostname, const int port)
{

	if(hostname==NULL)
	{
		printf("invalid param in conn1\n");
		errno = ERR_INVALID_PARAM;	//1

		return NULL;
	}


	int isNum = 0;		
	char strhost[64];
	
	strcpy(strhost,hostname);

	int len = strlen(strhost);
	int i=0;
	while(i<len)
	{
		if(strhost[i]==' ')
			isNum++;
		i++;
	}

	if(isNum!=0 || port==0 || *hostname=='\0')
	{
		printf("invalid param in conn 2\n");
		errno = ERR_INVALID_PARAM;	//1
		return NULL;
	}
	

	// Create a socket.
	int sock = socket(PF_INET, SOCK_STREAM, 0);
	if (sock < 0) {
		logger("[LOG CLIENT] Unable to create socket\n", LOGGING);

		printf("conn fail in conn1\n");
		errno = ERR_CONNECTION_FAIL;	//2

		return NULL;
	}

	// Get info about the server.
	struct addrinfo serveraddr, *res;
	memset(&serveraddr, 0, sizeof serveraddr);
	serveraddr.ai_family = AF_UNSPEC;
	serveraddr.ai_socktype = SOCK_STREAM;
	char portstr[MAX_PORT_LEN];
	snprintf(portstr, sizeof portstr, "%d", port);
	int status = getaddrinfo(hostname, portstr, &serveraddr, &res);
	

	if (status != 0) {
		logger("[LOG CLIENT] Unable to get info about server\n", LOGGING);
		
		printf("conn fail in conn2\n");
		errno = ERR_CONNECTION_FAIL;	//2

		return NULL;
	}

	// Connect to the server.
	status = connect(sock, res->ai_addr, res->ai_addrlen);
	if (status != 0) {
		logger("[LOG CLIENT] Unable to connect to server\n", LOGGING);
		
		printf("conn fail in conn3\n");
		errno = ERR_CONNECTION_FAIL;	//2

		return NULL;
	}

	connected = 1;

	// Logging if connection is successful
	logger("[LOG CLIENT] Successful connection\n", LOGGING);
	return (void*) sock;
}


/**
 * @brief This is the function used to authenticate the user.  
 *
 * @param username Is the user-entered username
 * @param passwd Is the user-entered password
 * @param conn Acts as a file descriptor
 * @return Returns 0 if sucessful, -1 otherwise
 *
 * The function takes in a username, password, and a pointer to a valid connection
 * and uses it to authenticate the user by comparing the username and password to
 * those saved in the configuration file.
 */
int storage_auth(const char *username, const char *passwd, void *conn)
{

	if(username == NULL || passwd == NULL || conn==NULL)
	{		
		errno = ERR_INVALID_PARAM;	//1
		printf("invalid param in auth1\n");
		return -1;
	}



	// Validate the connection
    if(connected == 0)
	{
		printf("conn fail in auth\n");
		errno = ERR_CONNECTION_FAIL;		// 2
		logger("[LOG CLIENT] Authentication failed. No connection", LOGGING);
		return -1;
	}

 //    // Check to see if the connection has already been authenticated
	// if(authenticated == 1)
	// {
	// 	logger("[LOG CLIENT] Already authenticated.", LOGGING);
	// 	errno = ERR_AUTHENTICATION_FAILED;	// 4
	// 	return -1;
	// }

	if(*username=='\0' || *passwd=='\0')
	{
		printf("auth fail in auth1\n");
		errno = ERR_AUTHENTICATION_FAILED;	// 4
		return -1;	
	}

	// Validate the information being passed: checking for special characters
	int i = 0, c = 0;
		for(; i < strlen(username) && c == 0; i++) {
		    if (!isalnum(username[i])) 
		        c++;
		}

	if(c != 0)
	{
		printf("invalid param in auth2\n");
		logger("[LOG CLIENT] Invalid parameter.", LOGGING);
		errno = ERR_INVALID_PARAM;	// 1
		return -1;
	}

	// Connection is really just a socket file descriptor.
	int sock = (int)conn;

	// Send some data.
	char buf[MAX_CMD_LEN];
	memset(buf, 0, sizeof buf);
	char *encrypted_passwd = generate_encrypted_password(passwd, NULL);
	snprintf(buf, sizeof buf, "AUTH;%s;%s\n", username, encrypted_passwd);
	if (sendall(sock, buf, strlen(buf)) == 0 && recvline(sock, buf, sizeof buf) == 0) {

		char message[MAX_CMD_LEN];
		memset(message, 0, sizeof message);

        // Check to see if the username and password match
		if(strcmp(buf, "fail")==0)
		{
			printf("auth fail in auth2\n");
			logger("[LOG CLIENT] Unable to authenticate username", LOGGING);
			errno = ERR_AUTHENTICATION_FAILED;		// 4
			return -1;
		}

		else
		{
			authenticated = 1;
			snprintf(message, sizeof message, "[LOG CLIENT] Successful: AUTH %s %s\n", username, encrypted_passwd);
		}

		logger(message, LOGGING);
		return 0;
	}

	// Logging if authentication fails
	printf("auth fail in auth3\n");
	logger("[LOG CLIENT] Unable to authenticate username", LOGGING);
	errno = ERR_AUTHENTICATION_FAILED;	// 4
	return -1;
}

/**
 * @brief This is the function used to obtain information from the records requested 
 * by the user.  
 *
 * @param table The user-entered table name
 * @param key The user-entered key
 * @param record Pointer to the structure that stores the value at the given key
 * @param conn Acts as a file descriptor
 * @return Returns 0 if sucessful, -1 otherwise
 *
 * The functions takes in a table name, key, pointer to a structure which holds the 
 * record, and a valid connection and uses it retrieve the value of the key in the 
 * specified table IF it exists.
 */
int storage_get(const char *table, const char *key, struct storage_record *record, void *conn)
{
	

	if(table == NULL || key == NULL || conn==NULL)
	{		
		errno = ERR_INVALID_PARAM;	//1
		return -1;
	}


	// Check to see if the connection passed through is valid
	if(connected == 0)
	{
		errno = ERR_CONNECTION_FAIL;	// 2
		logger("[LOG CLIENT] Get failed. No connection", LOGGING);
		return -1;
	}

	// Check to see if the connection has been authenticated
	if(authenticated == 0)
	{
		errno = ERR_NOT_AUTHENTICATED; // 3
		logger("[LOG CLIENT] Get failed. No connection", LOGGING);
		return -1;
	}

	// Validate the information being passed in
	int i = 0, c = 0;
	for(; i < strlen(table) && c == 0; i++) {
	    if (!isalnum(table[i])) 
	        c++;
	}
	
	i = 0;
	for(; i < strlen(key) && c == 0; i++) {
	    if (!isalnum(key[i])) 
	        c++;
	}
	
	if(c != 0 || *table=='\0' || *key=='\0'){

		errno = ERR_INVALID_PARAM;	// 1
		return -1;
	}

	// Connection is really just a socket file descriptor.
	int sock = (int)conn;

	// Send some data.
	char buf[MAX_CMD_LEN];
	memset(buf, 0, sizeof buf);
	snprintf(buf, sizeof buf, "GET;%s;%s\n", table, key);
	if (sendall(sock, buf, strlen(buf)) == 0 && recvline(sock, buf, sizeof buf) == 0) {


		printf("buffer received = %s\n", buf);

		char message[MAX_CMD_LEN];
		memset(message, 0, sizeof message);

        // Check to see if the table passed through exists
		if(strcmp(buf, "tableNotFound")==0)
		{
			snprintf(message, sizeof message, "[LOG CLIENT] Unable to GET %s %s. Table not found.\n", table, key);
			logger(message, LOGGING);
			errno = ERR_TABLE_NOT_FOUND;		// 5

			return -1;
		}

        // Check to see if the record passed through exists
		else if(strcmp(buf, "recordNotFound")==0)
		{
			snprintf(message, sizeof message, "[LOG CLIENT] Unable to GET %s %s. Record not found.\n", table, key);
			logger(message, LOGGING);
			errno = ERR_KEY_NOT_FOUND;			// 6

			return -1;
		}


		else
		{
			char temp[MAX_CMD_LEN];
			strcpy(temp, buf);

			char* str = temp;
			char* tok = strtok(str, ";");

			char valBuf[MAX_CMD_LEN];
			strncpy(valBuf, tok, sizeof valBuf);

			// printf("valBuf = %s\n", valBuf);

			tok = strtok(NULL, ";");
			char v[10];
			strncpy(v, tok, sizeof v);

			printf("value = %s, version = %s\n", valBuf, v);
			int version = atoi(v);

			strncpy(record->value, valBuf, sizeof record->value);
			record->metadata[0] = version;
			snprintf(message, sizeof message, "[LOG CLIENT] Successful: GET %s %s\n", table, key);			
			logger(message, LOGGING);
		}


		return 0;
	}

	// Logging if GET fails
	char fail[MAX_CMD_LEN];
	memset(fail, 0, sizeof fail);
	snprintf(fail, sizeof fail, "[LOG CLIENT] Unable to GET %s %s\n", table, key);
	logger(fail, LOGGING);

	return -1;}


/**
 * @brief This is the function used to create, modify, or delete a record.
 * 
 * @param table The user-entered table name
 * @param key The user-entered key
 * @param record Pointer to the structure that stores the value at the given key
 * @param conn Acts as a file descriptor
 * @return Returns 0 if sucessful, -1 otherwise
 *
 * The function takes in a table name, key, pointer to a structure that holds the
 * record, and a valid connection and uses it to either create, modify, or delete 
 * a record depending on the value of the record.
 */
int storage_set(const char *table, const char *key, struct storage_record *record, void *conn)
{

// table, key, NULL, 


	if(table == NULL || key == NULL || conn==NULL)
	{		
		errno = ERR_INVALID_PARAM;	//1
		printf("invalid param1\n");
		return -1;
	}


	
	// Check to see if the connection passed through is valid
	if(connected == 0)
	{
		errno = ERR_CONNECTION_FAIL;	// 2
		logger("[LOG CLIENT] Set failed. No connection", LOGGING);
		printf("not connected\n");
		return -1;
	}

    // Check to see if the connection has been authenticated
	if(authenticated == 0)
	{
		errno = ERR_NOT_AUTHENTICATED; // 3
		printf("not authenticated\n");
		return -1;
	}

	char val123[1024];
    // Check to see if the value of the record is NULL, if it is NULL then delete
    // the record
	
	if(record==NULL)
	{
		record = malloc(sizeof(struct storage_record));
		strncpy(record->value, "deleteRecord", sizeof record->value);
	}

	if(*(record->value) == '\0')
	{
		strncpy(record->value, "deleteRecord", sizeof record->value);

	}

	// Validate the information being passed in
	int i = 0, c = 0;
	for(; i < strlen(table) && c == 0; i++) {
	    if (!isalnum(table[i])) 
	        c++;
	}
	
	i = 0;
	for(; i < strlen(key) && c == 0; i++) {
	    if (!isalnum(key[i])) 
	        c++;
	}
	
	if(c != 0 || *table=='\0' || *key=='\0') {

		errno = ERR_INVALID_PARAM;	// 1
		printf("invalid param2\n");
		return -1;
	}
	
	// printf("%s, %s\n", key, record->value);

	// Connection is really just a socket file descriptor.
	int sock = (int)conn;

	// Send some data.
	char buf[MAX_CMD_LEN];
	memset(buf, 0, sizeof buf);
	

	snprintf(buf, sizeof buf, "SET;%s;%s;%s;%d\n", table, key, record->value, record->metadata[0]);
	
	// printf("%s\n", buf);
	if (sendall(sock, buf, strlen(buf)) == 0 && recvline(sock, buf, sizeof buf) == 0) {



		// printf("buffer received = %s\n", buf);

		char message[MAX_CMD_LEN];
		memset(message, 0, sizeof message);
		
		// printf("%s\n", buf);

        // Check to see if the table passed through exists
		if(strcmp(buf, "tableNotFound")==0)
		{
			snprintf(message, sizeof message, "[LOG CLIENT] Unable to SET %s %s %s. Table not found.\n", table, key, record->value);
			logger(message, LOGGING);
			errno = ERR_TABLE_NOT_FOUND;		// 5

			// printf("%s\n", message);

			printf("table not found\n");
			return -1;
		}

        // Check to see if the record passed through exists
		else if(strcmp(buf, "recordNotFound")==0)
		{
			snprintf(message, sizeof message, "[LOG CLIENT] Unable to SET %s %s %s. Record not found.\n", table, key, record->value);
			logger(message, LOGGING);
			errno = ERR_KEY_NOT_FOUND;			// 6	
			// printf("%s\n", message);
			printf("record not found\n");

			return -1;
		}

        // Check to see if invalid parameter: col order, number doesn't match schema
		else if(strcmp(buf, "invalidParameter")==0)
		{
			snprintf(message, sizeof message, "[LOG CLIENT] Unable to SET %s %s %s. Invalid Parameter.\n", table, key, record->value);
			logger(message, LOGGING);
			errno = ERR_INVALID_PARAM;			// 1	
			// printf("%s\n", message);

			printf("invalid param\n");
			return -1;
		}

		else if(strcmp(buf, "transactionAborted")==0)
		{
			snprintf(message, sizeof message, "[LOG CLIENT] Unable to SET %s %s %s. Transaction aborted.\n", table, key, record->value);
			logger(message, LOGGING);
			errno = ERR_TRANSACTION_ABORT;			// 8	
			// printf("%s\n", message);

			printf("Transaction aborted\n");
			return -1;
		}

		else
		{
			snprintf(message, sizeof message, "[LOG CLIENT] Successful: SET %s %s %s\n", table, key, record->value);			
			logger(message, LOGGING);

			// printf("%s\n", message);
		}

		return 0;
	}

}


/**
 * @brief Query the table for records, and retrieve the matching keys.
 *
 * @param table A table in the database.
 * @param predicates A comma separated list of predicates.
 * @param keys An array of strings where the keys whose records match the 
 * specified predicates will be copied.  The array must have room for at 
 * least max_keys elements.  The caller must allocate memory for this array.
 * @param max_keys The size of the keys array.
 * @param conn A connection to the server.
 * @return Return the number of matching keys (which may be more than
 * max_keys) if successful, and -1 otherwise.
 *
 * On error, errno will be set to one of the following, as appropriate: 
 * ERR_INVALID_PARAM, ERR_CONNECTION_FAIL, ERR_TABLE_NOT_FOUND, 
 * ERR_KEY_NOT_FOUND, ERR_NOT_AUTHENTICATED, or ERR_UNKNOWN.
 *
 * Each predicate consists of a column name, an operator, and a value, each
 * separated by optional whitespace. The operator may be a "=" for string
 * types, or one of "<, >, =" for int and float types. An example of query
 * predicates is "name = bob, mark > 90".
 */
int storage_query(const char *table, const char *predicates, char **keys, 
	const int max_keys, void *conn)
{


	if(table == NULL || predicates == NULL || conn==NULL)
	{		
		errno = ERR_INVALID_PARAM;	//1
		return -1;
	}



	// Check to see if the connection passed through is valid
	if(connected == 0)
	{
		errno = ERR_CONNECTION_FAIL;	// 2
		logger("[LOG CLIENT] Set failed. No connection", LOGGING);
		return -1;
	}

    // Check to see if the connection has been authenticated
	if(authenticated == 0)
	{
		errno = ERR_NOT_AUTHENTICATED; // 3
		return -1;
	}


	// Validate the information being passed in
	int i = 0, c = 0;
	for(; i < strlen(table) && c == 0; i++) {
	    if (!isalnum(table[i])) 
	        c++;
	}
	

	if(c != 0 || *table=='\0' || *predicates=='\0') {

		errno = ERR_INVALID_PARAM;	// 1
		return -1;
	}

	// int len1 = strlen(table);
	// int len2 = strlen(predicates);

	// printf("table=%s, length=%d; predicates=%s, length=%d",table, len1, predicates, len2);

	// Connection is really just a socket file descriptor.
	int sock = (int)conn;

	// Send some data.
	char buf[MAX_CMD_LEN];
	memset(buf, 0, sizeof buf);

	snprintf(buf, sizeof buf, "QUERY;%s;%s\n", table, predicates);


	// printf("%s\n", buf);
	if (sendall(sock, buf, strlen(buf)) == 0 && recvline(sock, buf, sizeof buf) == 0)
	{

		// "bloor dundas vc sdf sdf"



		char message[MAX_CMD_LEN];
		memset(message, 0, sizeof message);
		
		
        // Check to see if the table passed through exists
		if(strcmp(buf, "tableNotFound")==0)
		{
			printf("%s\n", buf);

			
			snprintf(message, sizeof message, "[LOG CLIENT] Unable to QUERY %s %s %s. Table not found.\n", table, predicates);
			logger(message, LOGGING);
			errno = ERR_TABLE_NOT_FOUND;		// 5

			// printf("%s\n", message);

			return -1;
		}

        // Check to see if invalid parameter: col order, number doesn't match schema
		else if(strcmp(buf, "invalidParameter")==0)
		{
			snprintf(message, sizeof message, "[LOG CLIENT] Unable to QUERY %s %s. Invalid Parameter.\n", table, predicates);
			logger(message, LOGGING);
			errno = ERR_INVALID_PARAM;			// 1	
			// printf("%s\n", message);

			return -1;
		}

		else
		{

			printf("buf = %s\n", buf);

			char temp[MAX_CMD_LEN];
			strcpy(temp, buf);
			// printf("%s\n", temp);

			char *str = temp;
			char *tok = strtok(str, " ");
			int found = 0, i = 0;

			while(tok){

				keys[found] = tok;
				strcat(keys[found], "\0");
				// printf("In function: %s\n", keys[found]);		

				tok = strtok(NULL, " ");
				found++;
			}

			snprintf(message, sizeof message, "[LOG CLIENT] Successful: QUERY %s %s\n", table, predicates);			
			logger(message, LOGGING);

			// printf("%d\n", found);
			return found;

			// printf("%s\n", message);
		}

		return 0;
	}

}



/**
 * @brief This is the function used to disconnect from the server.
 * 
 * @param conn Acts as a file descriptor
 * @return Returns 0 if sucessful, -1 otherwise
 *
 * The function takes in a pointer to a valid connection and uses it to disconnect
 * from the server.
 */
int storage_disconnect(void *conn)
{
	// Cleanup
	int sock = (int)conn;
	
	if(conn!=NULL)
	{
		authenticated = 0;
		close(sock);

		// Logger
		logger("[LOG CLIENT] Successfully disconnected\n", LOGGING);
	}
	else
	{
		errno = ERR_INVALID_PARAM;
		return -1;
	}

	return 0;
}

