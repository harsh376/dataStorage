/**
 * @file
 * @brief This file implements various utility functions that are
 * can be used by the storage server and client library. 
 */

#define _XOPEN_SOURCE

 

#include <stdlib.h>
#include <stdio.h>
#include <string.h>
#include <stdbool.h>
#include <sys/types.h>
#include <sys/socket.h>
#include <unistd.h>
#include "utils.h"


FILE *log;

tableLengthExceeded = 0; // Used to check if the length of the table has been exceeded

/**
 * @brief This function is used to send information of the socket to 
 * the server or the client.
 *
 * @param sock Acts as a file descriptor
 * @param buf Buffer that stores the data that is to be sent on the other side of the
 * 		  network
 * @param len Length of the buffer
 * 
 * The function takes in a socket, a message as a string, and the
 * length of the string and uses it to send the information to either
 * the client or the server.
 */

char* substring1(const char* str, size_t begin, size_t len) 
{ 
  if (str == 0 || strlen(str) == 0 || strlen(str) < begin || strlen(str) < len) 
    return 0; 

  return strndup(str + begin, len-begin); 
} 


// tokenizer for schema string
char* getNextToken(char* str, int* i)
{
    int j = *i;
    while((str+j)!=NULL)
    {
        if(*(str+j) == ' ' || *(str+j) == '\0')
        {
            break;      
        }
        j++;
    }
    char* tok = substring1(str, *i, j);
    j++;
    *i = j;
    return tok;
}


char* substring(const char* str, size_t begin, size_t len) 
{ 
  if (str == 0 || strlen(str) == 0 || strlen(str) < begin || strlen(str) < (begin+len)) 
    return 0; 

  return strndup(str + begin, len); 
} 


char* trimStartAndEnd(char *str)
{
	

	char strCopy1[MAX_CONFIG_LINE_LEN];
	strcpy(strCopy1, str);

	char* strCopy = strCopy1;

	int len = strlen(strCopy);
	int i = 0;
	int j = len-1;

	// printf("%s(%d)\n", strCopy, len);

	while(strCopy[i]==' ')
		i++;

	while(strCopy[j]==' ')
		j--;

	strCopy[j+1]='\0';
	strCopy=strCopy+i;

	// printf("%s(%d)\n", strCopy, strlen(strCopy));
	return strCopy;


}


char* trim(char* str)
{
	int len = strlen(str);
	int i = 0;
	int j = len-1;

	while(str[i]==' ')
		i++;

	while(str[j]==' ')
		j--;

	str[j+1]='\0';
	str=str+i;

	char* s = str;
	
	// printf("%s(%d)\n", s, strlen(s));
	
	return s;
}

char* tokenize(char** str, char* tok)
{
	int i = 0;
	bool flag = true;

	*str = trim(*str);
	// printf("%s,", *str);
	strcpy(tok, *str);

	while(*(*str)!=' ' && *(*str)!='\0')
	{
		// printf("%c,", **str);
		(*str)++;
		i++;
	}

	*str = trim(*str);
	// *str = "31" which is the value of the col
	
	if(**str == '\0')
	{
		*str = NULL;
		// printf("No value\n");
	}

	// printf("before\n");
	strcpy(tok, substring(tok, 0, i));
	// tok = "kilometres" which is the name of col

	char* token = tok;

	return token;

}


int isNum(char* str_)
{
	char* str = str_;
	int len = strlen(str);
	int isNum = 0;

	if(*str!='-' && *str!='+' && !isdigit(*str))
		return 1;

	str++;

	while(*str)
	{
		if(!isdigit(*str))
			isNum++;
		str++;
	}

	if(isNum==0)
		return 0;
	return 1;
}

// schema
int getNumberOfColsSchema(char* str)
{
	int cols = 0;
	int i =0;
	char* temp;

	while(i <= strlen(str))
    {
        temp = getNextToken(str,&i);
        temp = getNextToken(str,&i);

        if(strcmp(temp, "char")==0)
        	temp = getNextToken(str,&i);

        cols++;
    }
    return cols;
}

int getNumberOfColsInput(char* str)
{
	int cols = 0;
	char* s = str;
	char* s1 = strtok(s,",");

	while(s1)
	{
		cols++;
		s1 = strtok(NULL, ",");
	}

	return cols;
}

int parser(char* schema, char* str)
{
	bool inputIsValid = true;


	int i = 0;
	char* schemaCopy = schema;

	char schemaTemp[MAX_CONFIG_LINE_LEN];
	strncpy(schemaTemp, schema, sizeof schemaTemp);
	int numOfColsSchema = getNumberOfColsSchema(schemaTemp);


	// printf("schema:%s\n", schema);	

	char inputTemp[MAX_CONFIG_LINE_LEN];
	strncpy(inputTemp, str, sizeof inputTemp);

	int numOfColsInput = getNumberOfColsInput(inputTemp);

	char* ss;
	char* s1;



	if(numOfColsInput!=numOfColsSchema)
	{
		printf("numOfColsSchema = %d\n", numOfColsSchema);
		printf("numOfColsInput = %d\n", numOfColsInput);
		printf("Number of cols don't match\n");
		inputIsValid = false;
	}

	else
	{
	    ss = str;		// pointer to the string for strtok
	    s1 = strtok(ss,",");		// getting first comma separated token, i.e "  name    Bloor   Danforth "
	}

    while(s1 && inputIsValid)
    {
    	char* schColName;
    	char* schColType;
    	char* schColLength;
    	int schColLength_int;
	    if(i <= strlen(schemaCopy))
	    {
	        schColName = getNextToken(schemaCopy,&i);
	        // printf("%s,", schColName);
	        schColType = getNextToken(schemaCopy,&i);
	        // printf("%s,", schColType);

	        if(strcmp(schColType,"char")==0)
	        {
	        	schColLength = getNextToken(schemaCopy,&i);
	        	schColLength_int = atoi(schColLength);
	        	// printf("%d\n", schColLength_int);
	        }
	        else
	        {
	        	// printf("\n");
	        }
	    }

    	char s4[MAX_CONFIG_LINE_LEN];
    	strcpy(s4, s1);
    	// s4 = "  kilometres 26   "

    	char* s3 = trim(s4);
    	// s3 = "kilometres 26"	

	    char token[MAX_CONFIG_LINE_LEN];

	    // token: column name, s3: column value
		strncpy(token, tokenize(&s3, token), sizeof token);
		// printf("%s,", token);
		
		if(s3==NULL)
		{
			printf("NULL value\n");
			inputIsValid = false;
			break;
		}
		else
		{
			// printf("%s\n", s3);
		}

		// validate input
		
		// matching the columns name
		if(strcmp(schColName, token)!=0)
		{
			inputIsValid = false;
			break;
		}
		// column name matches
		
		// check if value is a number
		if(strcmp(schColType, "int")==0)
		{
			int status = isNum(s3);
			if(status==1)
			{
				inputIsValid = false;
				break;
			}
		}

		// check if length of string fits the schema
		if(strcmp(schColType, "char")==0)
		{
			int len = strlen(s3);
			if(len > schColLength_int)
			{
				inputIsValid = false;
				break;
			}
			char tt[100];
			strcpy(tt, s3);
			char* ttptr = tt;
			bool isAlphaNum = true;
			while(*ttptr!='\0' && isAlphaNum)
			{
				if(!isalnum(*ttptr) && *ttptr!=' ')
				{
					isAlphaNum = false;
				}
				else
				{
					ttptr++;
				}
			}

			if(!isAlphaNum)
			{
				inputIsValid = false;
				break;
			}
		}


		// getting next colName,colValue
		s1 = strtok(NULL,",");		

	}


	if(inputIsValid)
	{
		// printf("Success\n");
		return 0;
	}
	else
	{
		// printf("Error: Invalid input\n");
		return 1;
	}



}



int sendall(const int sock, const char *buf, const size_t len)
{
	size_t tosend = len;
	while (tosend > 0) {
		ssize_t bytes = send(sock, buf, tosend, 0);
		if (bytes <= 0) 
			break; // send() was not successful, so stop.
		tosend -= (size_t) bytes;
		buf += bytes;
	};

	return tosend == 0 ? 0 : -1;
}

/**
 * @brief This function is used to recieve information of the socket
 * to the server or the client.
 * 
 * @param sock Acts as a file descriptor
 * @param buf Buffer that stores the data that is to be sent on the
 		  other side of the network
 * @param buflen Length of the buffer
 *
 * The function takes in a socket, a message as a string, and the
 * length of the string and uses it to recieve the information from
 * the client or the server. In order to avoid reading more than a 
 * line from the stream, this function only reads one byte at a time.
 */
int recvline(const int sock, char *buf, const size_t buflen)
{
	int status = 0; // Return status.
	size_t bufleft = buflen;

	while (bufleft > 1) {
		// Read one byte from scoket.
		ssize_t bytes = recv(sock, buf, 1, 0);
		if (bytes <= 0) {
			// recv() was not successful, so stop.
			status = -1;
			break;
		} else if (*buf == '\n') {
			// Found end of line, so stop.
			*buf = 0; // Replace end of line with a null terminator.
			status = 0;
			break;
		} else {
			// Keep going.
			bufleft -= 1;
			buf += 1;
		}
	}
	*buf = 0; // add null terminator in case it's not already there.

	return status;
}


/**
 * @brief This function is used to parse and process a line in the 
 * config file.
 *
 * @param line Character array given from the configuration input file
 * @param params Stores the configuration parameters such as username
 * 		  serverhost, serverport, password, and the table names from
 * 		  the configuration file
 * @param tl 
 *
 * The function takes in a line of the configuration file, a structure 
 * to hold the parameters, and an integer that is 1 if an error occured 
 * and 0 otherwise. It then uses this information to process the line 
 * and determine whether it is a value to be stored in the structure 
 * orinformation about a table.
 */
int process_config_line(char *line, struct config_params *params, int *tl)
{
	// Ignore comments.
	if (line[0] == CONFIG_COMMENT_CHAR)
		return 0;

	// Extract config parameter name and value.
	char name[MAX_CONFIG_LINE_LEN];
	char value[MAX_CONFIG_LINE_LEN];
	char extraArg[MAX_CONFIG_LINE_LEN];

	// In case of an extra argument, storing it in extraArg
	int items = sscanf(line, "%s %s %s\n", name, value, extraArg);

	// printf("items:%d\n", items);

	// Line wasn't as expected.
	if (items != 2)
		return -1;


	// Process this line.
	if(strcmp(name, "server_host") == 0) {
 
		// Check if the length of value > MAX_HOST_LEN
		int hostLength = strlen(value);
		// printf("%d\n", hostLength);
		if(hostLength > MAX_HOST_LEN)
		{
			printf("length exceeded MAX_HOST_LEN\n");
			return -1;
		}


		// The first server host
		if(params->numOfServerHosts == 0)
		{
			strncpy(params->server_host, value, sizeof params->server_host);
		}

		// Increment the number of server hosts in the conf file
		params->numOfServerHosts++;		
	} 

	else if(strcmp(name, "server_port") == 0) {
	

		int isNum = 0;		
		char* str = value;
		// printf("%s\n", t);		


		while(*str)
		{
			if(!isdigit(*str))
				isNum++;
			str++;
		}

		if(isNum!=0)
		{
			printf("port number is not a number\n");
			return -1;
		}


		// Check to see if the length of value > MAX_PORT_LEN

		int portLength = strlen(value);
		// printf("%d\n", portLength);
		if(portLength > MAX_PORT_LEN)
		{
			printf("length exceeded MAX_PORT_LEN\n");
			return -1;
		}

		// The first server port
		if(params->numOfServerPorts == 0)
		{
			params->server_port = atoi(value);

			// printf("port: %d\n", params->server_port);			
		}

		// Increment the number of server ports in the conf file
		params->numOfServerPorts++;

	}

	else if(strcmp(name, "username") == 0) {
	
		// Check if the length of value > MAX_USERNAME_LEN
		int usernameLength = strlen(value);
		// printf("%d\n", portLength);
		if(usernameLength > MAX_USERNAME_LEN)
		{
			printf("length exceeded MAX_USERNAME_LEN\n");
			return -1;
		}

		// The first username
		if(params->numOfUsernames == 0)
		{
			strncpy(params->username, value, sizeof params->username);
		}

		// Increment the number of usernames in the conf file
		params->numOfUsernames++;
	}


	else if(strcmp(name, "password") == 0) {
	
		// Check if the length of value > MAX_ENC_PASSWORD_LEN
		int encPasswordLength = strlen(value);
		// printf("%d\n", portLength);
		if(encPasswordLength > MAX_ENC_PASSWORD_LEN)
		{
			printf("length exceeded MAX_ENC_PASSWORD_LEN\n");
			return -1;
		}


		// The first password
		if(params->numOfPasswords == 0)
		{
			strncpy(params->password, value, sizeof params->password);
		}

		// Incrementing the number of passwords in the conf file
		params->numOfPasswords++;
	} 

	else if(strcmp(name, "table") == 0){

		
		// Validate the information being passed: checking for special characters
		int i = 0, c = 0;
			for(; i < strlen(value) && c == 0; i++) {
			    if (!isalnum(value[i])) 
			        c++;
			}

		if(c != 0)
		{
			*tl = 1;
			// printf("table name has special characters\n");
			return -1;
		}




		// Check if the length of value > MAX_TABLE_LEN

		int tableLength = strlen(value);
		// printf("%d\n", portLength);
		if(tableLength > MAX_TABLE_LEN)
		{
			tableLengthExceeded = 1;
			*tl = 1;
			printf("length exceeded MAX_TABLE_LEN\n");
			return -1;
		}

		if(tableLengthExceeded == 0)
		{
			// Getting the index where the new table needs to be
			int index = params->numOfTables;

			int i;
			int flag=0;
			for(i=0; i<index; i++)
			{
				if(strcmp(params->tableArray[i], value)==0)
				{
					flag++;
				}
			}

			if(flag != 0)
			{
				params->hasDuplicateTables++;
				// printf("duplicate table\n");
			}

			else
			{
				// Copying the name of the table name at that location
				strncpy( params->tableArray[index], value, sizeof params->tableArray[index] );

				// Incrementing the index of the tableArray
				params->numOfTables += 1;
			}
		}
	} 

	// else if (strcmp(name, "data_directory") == 0) {
	//	strncpy(params->data_directory, value, sizeof params->data_directory);
	//} 
	else {
		// Ignore unknown config parameters.
	}

	return 0;
}

extern FILE* yyin;
extern int yylex();
extern int yylineno;
extern char* yytext;

char *n1[] = {NULL, "table", "name", "colon", "char_type", "int_type",
"open_bracket", "close_bracket", "comma", "number", "server_host", "server_port",
"key_username", "key_password","password"};


int tokenizer(char* config_file, struct config_params *params)
{
	yyin = fopen(config_file, "r");

	char serverHost[MAX_HOST_LEN];
	char serverPort[MAX_PORT_LEN];
	char username[MAX_USERNAME_LEN];
	char password[MAX_ENC_PASSWORD_LEN];
	char concurrency[10];

	int serverHosts = 0;
	int serverPorts = 0;
	int usernames = 0;
	int passwords = 0;
	int concurrencyOccurrences = 0;

	int ntoken, vtoken, temp, temp1;

	int error = 0;

	char ntok_value[100];
	char vtok_value[100];


	char tableNames[MAX_TABLES][MAX_TABLE_LEN];		// 100 : MAX_TABLES, 20: MAX_TABLE_LEN
	char tableProps[MAX_TABLES][1024];

	int tableCount = 0;

	ntoken = yylex();

	char rowType[MAX_TABLES][1024];			// stores the first token of the line
	char rowValue[MAX_TABLES][1024];			// stores the rest of the line

	int row_token = 0;
	int lineNumber = 0;

	while(ntoken)	// while non-zero token
	{
		if(row_token == 0)
		{
			strncpy(rowType[lineNumber], yytext, sizeof rowType[lineNumber]);
			row_token++;
			lineNumber++;
		}

		strncpy(ntok_value, yytext, sizeof ntok_value);
		vtoken = yylex();
		strncpy(vtok_value, yytext, sizeof vtok_value);
		
		// printf("ntoken = %s, vtoken = %s\n", ntok_value, vtok_value);
		// printf("ntoken = %s, vtoken = %s\n", n1[ntoken], n1[vtoken]);
		
		int ntoken_equal_vtoken = 0;
		int ntoken_equal_temp = 0;

		switch(ntoken)
		{
			case KEY_SERVERHOST:
						// invalid type, missing
						if(vtoken!=NAME)
						{	
							error = -1;
							printf("Expected host name\n");
						}
						
						// valid type
						else
						{
							temp = yylex();
							// duplicate
							if(serverHosts != 0)
							{
								error = -1;
								printf("Multiple server hosts\n");
							}
							// invalid after localhost
							else if(temp!=TABLE && temp!=KEY_SERVERPORT && temp!=KEY_SERVERHOST
								&& temp!=KEY_USERNAME && temp!=KEY_PASSWORD && temp!=CONCURRENCY)
							{
								error = -1;
								printf("Invalid number of parameters.\n");
							}
							// valid serverHost line
							else
							{
								strncpy(serverHost, vtok_value, sizeof serverHost);
								serverHosts++;
								ntoken_equal_temp = 1;
								row_token = 0;
							}

						}
						break;

			case KEY_SERVERPORT:
						// invalid type, missing
						// printf("%s, %s\n", n1[ntoken], n1[vtoken]);
						if(vtoken!=NUMBER)
						{
							error = -1;
							printf("Invalid port\n");
						}
						else
						{
							temp = yylex();
							// printf("%d\n", temp1);
							// duplicate
							if(serverPorts != 0)
							{
								error = -1;
								printf("Multiple server ports\n");
							}
							// invalid after 1112
							else if(temp!=TABLE && temp!=KEY_SERVERPORT && temp!=KEY_SERVERHOST
								&& temp!=KEY_USERNAME && temp!=KEY_PASSWORD && temp!=CONCURRENCY)
							{
								error = -1;
								printf("Invalid number of parameters pp.\n");
							}
							// valid serverHost line
							else
							{
								strncpy(serverPort, vtok_value, sizeof serverPort);
								serverPorts++;
								ntoken_equal_temp = 1;
								row_token = 0;
							}								
						}
						break;

			case KEY_USERNAME:
						// invalid type, missing
						// printf("%s, %s\n", n1[ntoken], n1[vtoken]);
						if(vtoken!=NAME)
						{
							error = -1;
							printf("Invalid username\n");
						}
						else
						{
							temp = yylex();
							// printf("%d\n", temp1);
							// duplicate
							if(usernames != 0)
							{
								error = -1;
								printf("Multiple usernames\n");
							}
							// invalid after admin
							else if(temp!=TABLE && temp!=KEY_SERVERPORT && temp!=KEY_SERVERHOST
								&& temp!=KEY_USERNAME && temp!=KEY_PASSWORD && temp!=CONCURRENCY)
							{
								error = -1;
								printf("Invalid number of parameters pp.\n");
							}
							// valid username line
							else
							{
								strncpy(username, vtok_value, sizeof username);
								usernames++;
								ntoken_equal_temp = 1;
								row_token = 0;
							}								
						}
						break;

			case KEY_PASSWORD:
						// invalid type, missing
						// printf("%s, %s\n", n1[ntoken], n1[vtoken]);
						if(vtoken!=PASSWORD && vtoken!=NAME)
						{
							error = -1;
							printf("Invalid password\n");
						}
						else
						{
							temp = yylex();
							// printf("%d\n", temp1);
							// duplicate
							if(passwords != 0)
							{
								error = -1;
								printf("Multiple passwords\n");
							}
							// invalid after admin
							else if(temp!=TABLE && temp!=KEY_SERVERPORT && temp!=KEY_SERVERHOST
								&& temp!=KEY_USERNAME && temp!=KEY_PASSWORD && temp!=CONCURRENCY)
							{
								error = -1;
								printf("Invalid number of parameters pp.\n");
							}
							// valid password line
							else
							{
								strncpy(password, vtok_value, sizeof password);
								passwords++;
								ntoken_equal_temp = 1;
								row_token = 0;
							}								
						}
						break;	


			case CONCURRENCY:
						// invalid type, missing
						// printf("%s, %s\n", n1[ntoken], n1[vtoken]);
						if(vtoken!=NUMBER)
						{
							error = -1;
							printf("Invalid Concurrency\n");
						}
						else
						{
							temp = yylex();
							// printf("%d\n", temp1);
							// duplicate
							if(concurrencyOccurrences != 0)
							{
								error = -1;
								printf("Multiple concurrency definitions\n");
							}
							// invalid after '1' or '2'
							else if(temp!=TABLE && temp!=KEY_SERVERPORT && temp!=KEY_SERVERHOST
								&& temp!=KEY_USERNAME && temp!=KEY_PASSWORD && temp!=CONCURRENCY)
							{
								error = -1;
								printf("Invalid number of parameters.\n");
							}
							// valid concurrency line
							else
							{
								strncpy(concurrency, vtok_value, sizeof concurrency);
								concurrencyOccurrences++;
								ntoken_equal_temp = 1;
								row_token = 0;
							}								
						}
						break;



			case TABLE:
						if(vtoken!=NAME)
						{
							error = -1;
							printf("Expected table name!\n");
						}
						else
						{

							int flag = 0;
							int i;
							for(i = 0; i < tableCount; ++i)
							{
								if(strcmp(tableNames[i], vtok_value)==0)
								{
									flag++;
								}	
							}

							printf("%d\n", flag);

							if(flag != 0)
							{
								error = -1;
								printf("duplicate tables\n");
							}

							else
							{
								strncpy(tableNames[tableCount], vtok_value, sizeof tableNames[tableCount]);
								tableCount++;
							}
						}
						break;


			case NAME:
						if(vtoken!=COLON)
						{
							error = -1;
							printf("Expected ':' after column name\n");
						}
						else
						{
							char t[50];
							sprintf(t, "%s ",ntok_value);
							int index = tableCount-1;
							strcat(rowValue[index], t);

							ntoken_equal_vtoken = 1;
						}
						break;

			case PASSWORD:
						if(vtoken!=COLON)
						{
							error = -1;
							printf("Expected ':' after column name\n");
						}
						else
						{
							char t[50];
							sprintf(t, "%s ",ntok_value);
							int index = tableCount-1;
							strcat(rowValue[index], t);

							ntoken_equal_vtoken = 1;
						}
						break;						

			case COLON:
						if(vtoken!=CHARACTER && vtoken!=INTEGER)
						{
							error = -1;
							printf("Expected column type after ':'\n");
						}
						else
						{
							ntoken_equal_vtoken = 1;
						}
						break;


			case CHARACTER:
						if(vtoken!=OPEN_BRACK)
						{
							error = -1;
							printf("Expected open bracket\n");
						}
						else
						{
							char t[50];
							sprintf(t, "%s ",ntok_value);
							int index = tableCount-1;
							strcat(rowValue[index], t);

							ntoken_equal_vtoken = 1;
						}
						break;

			case OPEN_BRACK:
						if(vtoken!=NUMBER)
						{
							error = -1;
							printf("Expected the size of char[]\n");
						}
						else
						{
							ntoken_equal_vtoken = 1;
						}
						break;

			case NUMBER:
						if(vtoken!=CLOSE_BRACK)
						{
							error = -1;
							printf("Expected close bracket\n");
						}
						else
						{
							char t[50];
							sprintf(t, "%s ",ntok_value);
							int index = tableCount-1;
							strcat(rowValue[index], t);
							
							ntoken_equal_vtoken = 1;
						}
						break;

			case CLOSE_BRACK:
						if(vtoken==TABLE || vtoken==KEY_SERVERPORT || 
							vtoken==KEY_SERVERHOST || vtoken==KEY_USERNAME
							|| vtoken==KEY_PASSWORD || vtoken==CONCURRENCY || vtoken==0)
						{
							ntoken_equal_vtoken = 1;
							row_token = 0;
						}
						else if(vtoken==COMMA)
						{
							ntoken_equal_vtoken = 1;
						}

						else
						{
							error = -1;
							printf("Expected comma or end of line\n");
						}
						break;

			case COMMA:
						if(vtoken!=NAME && vtoken!=PASSWORD)
						{
							error = -1;
							printf("Expected next column\n");
						}
						else
						{
							ntoken_equal_vtoken = 1;
						}
						break;
			
			case INTEGER:
						if(vtoken==TABLE || vtoken==KEY_SERVERPORT || 
							vtoken==KEY_SERVERHOST || vtoken==KEY_USERNAME
							|| vtoken==KEY_PASSWORD || vtoken==CONCURRENCY || vtoken==0)
						{
							char t[50];
							sprintf(t, "%s ",ntok_value);
							int index = tableCount-1;
							strcat(rowValue[index], t);
							ntoken_equal_vtoken = 1;
							row_token = 0;
						}
						else if(vtoken==COMMA)
						{
							char t[50];
							sprintf(t, "%s ",ntok_value);
							int index = tableCount-1;
							strcat(rowValue[index], t);
							ntoken_equal_vtoken = 1;
						}

						else
						{
							error = -1;
							printf("Expected comma or end of line\n");
						}
						break;

			default:
						;
		}

		if(error==-1)
			return -1;

		if(ntoken_equal_vtoken == 1)
			ntoken = vtoken;
		else if(ntoken_equal_temp == 1)
			ntoken = temp;
		else
			ntoken = yylex();

		// memset(&ntok_copy, 0, sizeof ntok_copy);

	}

	int i;
	for(i=0;i<tableCount;i++)
	{
		strncpy(params->tableArray[i], tableNames[i], sizeof params->tableArray[i]);
		strncpy(params->tableSchemaArray[i], rowValue[i], sizeof params->tableSchemaArray[i]);
		// printf("%s : ", tableNames[i]);	// table name
		// printf("%s\n", rowValue[i]);	// columns and their data types, space separated
	}

	// for(i=0;i<lineNumber;i++)
	// 	printf("%s\n", rowType[i]);

	strncpy(params->server_host, serverHost, sizeof params->server_host);
	params->server_port = atoi(serverPort);
	strncpy(params->username, username, sizeof params->username);
	strncpy(params->password, password, sizeof params->password);
	params->concurrency = atoi(concurrency);
	params->numOfTables = tableCount;
	params->numOfServerHosts = serverHosts;
	params->numOfServerPorts = serverPorts;
	params->numOfUsernames = usernames;
	params->numOfPasswords = passwords;


	if(serverHosts==0 || serverPorts==0 || usernames==0 || passwords==0 || concurrencyOccurrences==0 || tableCount==0)
	{
		printf("Missing configuration paramters\n");
		return -1;
	}

	return 0;
}


/**
 * @brief This function is used to read and process the configuration
 * file.
 *
 * @param config_file
 * @param params Stores the configuration parameters such as username
 * 		  serverhost, serverport, password, and the table names from
 * 		  the configuration file
 * 
 * The function takes in the name of the configuration file and a 
 * structure that holds the variables listed in the configuration 
 * file. It uses this to read through the configuration file and 
 * store all the information in the structure that was passed through.
 */
int read_config(const char *config_file, struct config_params *params)
{

	int error_occurred = tokenizer(config_file, params);

	// printf("error_occurred = %d\n", error_occurred);

	return error_occurred;
}

/**
 * @brief This function is used to log various processes throughout 
 * both the server and client.
 *
 * @param message
 * @param logging
 *
 * The function takes in a message as a string and an integer which 
 * determines what type of logging needs to be done. If logging is 
 * set to 0, then no logging is performed. If logging is set to 1, 
 * then the messages are displayed to the command line. If logging 
 * is set to 2, then the messages are configured into two files (one
 * for the client and one for the server) that are created when the
 * server/client are started.
 */
void logger(char *message, int logging)
{
	if(logging > 0) {
		fprintf(log,"%s\n",message);
		fflush(log);
	} else {
		// No logging
	}
}

/**
 * @brief This function is used to generate an encrypted password.
 * 
 * @param passwd Is the user-entered password
 * @param salt
 * @return Returns _____ if salt is NULL, otherwise returns ______
 * 
 * The function takes in a password as a string and encrypts the 
 * password to protect the information of the user.
 */
char *generate_encrypted_password(const char *passwd, const char *salt)
{
	if(salt != NULL)
		return crypt(passwd, salt);
	else
		return crypt(passwd, DEFAULT_CRYPT_SALT);
}

