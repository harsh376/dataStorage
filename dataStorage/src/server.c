/**
 * @file
 * @brief This file implements the storage server.
 *
 * The storage server should be named "server" and should take a single
 * command line argument that refers to the configuration file.
 * 
 * The storage server should be able to communicate with the client
 * library functions declared in storage.h and implemented in storage.c.
 */

#include <stdio.h>
#include <stdlib.h>
#include <unistd.h>
#include <sys/types.h>
#include <sys/socket.h>
#include <arpa/inet.h>
#include <netdb.h>
#include <string.h>
#include <assert.h>
#include <signal.h>
#include <time.h>
#include <string.h>
#include <stdbool.h>
#include "utils.h"
#include "storage.h"

/* Must include this to have prototype for the thread functions */ 
#include <pthread.h>

#define MAX_LISTENQUEUELEN 20	///< The maximum number of queued connections.
#define LOGGING 0
#define LOAD_CENSUS 0


#define MAX_THREADS 10

typedef struct _key_Version_ {

    char tableName[MAX_TABLE_LEN];
    char key[MAX_KEY_LEN];
    int version;

} keyVersion;


struct _ThreadInfo { 
  struct sockaddr_in clientaddr;
  socklen_t clientaddrlen; 
  int clientsock; 
  pthread_t theThread;
  struct config_params* params;
 
}; 
typedef struct _ThreadInfo *ThreadInfo; 

/*  Thread buffer, and circular buffer fields */ 
ThreadInfo runtimeThreads[MAX_THREADS]; 
unsigned int botRT = 0, topRT = 0;


/* Mutex to guard print statements */ 
pthread_mutex_t  printMutex    = PTHREAD_MUTEX_INITIALIZER; 

/* Mutex to guard handle_command statements */ 
pthread_mutex_t  handleCommandMutex    = PTHREAD_MUTEX_INITIALIZER; 




/* Mutex to guard condition -- used in getting/returning from thread pool*/ 
pthread_mutex_t conditionMutex = PTHREAD_MUTEX_INITIALIZER;
/* Condition variable -- used to wait for avaiable threads, and signal when available */ 
pthread_cond_t  conditionCond  = PTHREAD_COND_INITIALIZER;

// LOGGING:
// 0: no logging
// 1: logging to stdout
// 2: logging to file

// LOAD_CENSUS: 
// 0: don't load census
// 1: load census on server side, 
// 2: load census on client side

extern FILE *log;

/**
* @brief Acts as the structure for the data
* 
* @param string A given key
* @param record The pointer to the structure that stores
* 		 the value of a given key
* @param next The pointer to the next node structure
*/
typedef struct _list_t_ {
    char *string;
    struct storage_record *record;
    struct _list_t_ *next;
} Node;


/**
 * @brief Acts as the structure for the hash table
 * 
 * @param name The table name
 * @param size The size of the table
 * @param table A pointer that points to the first pointer
 *		 in array of node pointers
 */
typedef struct _hash_table_t_ {
    char* name;
    char* schema;
    int size;
    Node **table;
} HashTable;


typedef struct predicates_ {
    char* name_;
    char operator_;
    char* value_;
} Predicates;


typedef struct keys_ {
	char key[10];
	int count;
} Keys;



Predicates pred[10];


// HashTable *my_hash_table;
struct storage_record obj[1000000];
HashTable *allTables[MAX_TABLES];
int numberOfTables;

struct storage_record rec1[1000000];

keyVersion arrKeyVersion[100000];
int count_arrKeyVersion = 0;

int recCt = 0;


ThreadInfo getThreadInfo(void) { 
  ThreadInfo currThreadInfo = NULL;

  /* Wait as long as there are no more threads in the buffer */ 
  pthread_mutex_lock( &conditionMutex ); 
  while (((botRT+1)%MAX_THREADS)==topRT)
    pthread_cond_wait(&conditionCond,&conditionMutex); 
  
  /* At this point, there is at least one thread available for us. We
     take it, and update the circular buffer  */ 
  currThreadInfo = runtimeThreads[botRT]; 
  botRT = (botRT + 1)%MAX_THREADS;

  /* Release the mutex, so other clients can add threads back */ 
  pthread_mutex_unlock( &conditionMutex ); 
  
  return currThreadInfo;
}


/* This function receives a string from clients and echoes it back --
   the thread is released when the thread is finished */
void * threadCallFunction(void *arg) 
{

    printf("opening new thread\n");

    ThreadInfo tiInfo = (ThreadInfo)arg; 

    int wait_for_commands = 1;
    do
    {
        // Read a line from the client


        char buffer[MAX_CMD_LEN]; 
        int  length; 

        int status = recvline( tiInfo->clientsock, buffer, MAX_CMD_LEN); 

        if(status != 0)
        {
            wait_for_commands = 0;
        }
        else
        {
            // perform operation

            pthread_mutex_lock( &handleCommandMutex ); 

            int commandStatus = handle_command(tiInfo->clientsock, buffer, tiInfo->params);     


            if(commandStatus != 0)
                wait_for_commands = 0;  // Oops. An error occured.

            // length = strlen(buffer);
            // buffer[length] = '\n'; 
            // buffer[length+1] = 0; 
            // sendall( tiInfo->clientsock, buffer, strlen(buffer) ); 

            pthread_mutex_unlock( &handleCommandMutex ); 
   

            // length = strlen(buffer);
            // buffer[length] = '\n'; 
            // buffer[length+1] = 0; 
            // sendall( tiInfo->clientsock, &buffer[0], length+1 ); 
        }

    }while(wait_for_commands);


    if (close(tiInfo->clientsock)<0) 
    { 
        pthread_mutex_lock( &printMutex ); 
        printf("ERROR in closing socket to %s:%d.\n", 
        inet_ntoa(tiInfo->clientaddr.sin_addr), tiInfo->clientaddr.sin_port);
        pthread_mutex_unlock( &printMutex ); 
    }

    // close thread

    releaseThread( tiInfo ); 

    return NULL; 
}





/* Function called when thread is about to finish -- unless it is
   called, the ThreadInfo assigned to it is lost */ 
void releaseThread(ThreadInfo me) {
  pthread_mutex_lock( &conditionMutex ); 
  assert( botRT!=topRT ); 
 
  runtimeThreads[topRT] = me; 
  topRT = (topRT + 1)%MAX_THREADS; 

  /* tell getThreadInfo a new thread is available */ 
  pthread_cond_signal( &conditionCond ); 

  /* Release the mutex, so other clients can get new threads */ 
  pthread_mutex_unlock( &conditionMutex ); 

  printf("closing thread\n");
}




char* substring2(const char* str, size_t begin, size_t len) 
{ 
  if (str == 0 || strlen(str) == 0 || strlen(str) < begin || strlen(str) < (begin+len)) 
    return 0; 

  return strndup(str + begin, len); 
} 

char* trim2(char* str)
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

char* tokenize2(char** str, char* tok)
{
	int i = 0;
	bool flag = true;

	*str = trim2(*str);
	// printf("%s,", *str);
	strcpy(tok, *str);

	while(*(*str)!=' ' && *(*str)!='\0')
	{
		// printf("%c,", **str);
		(*str)++;
		i++;
	}

	*str = trim2(*str);
	// *str = "31" which is the value of the col
	
	if(**str == '\0')
	{
		*str = NULL;
		// printf("No value\n");
	}

	// printf("before\n");
	strcpy(tok, substring2(tok, 0, i));
	// tok = "kilometres" which is the name of col

	char* token = tok;

	return token;

}

char* colValue(char* line, char* colName)
{
	char cpy[1024];

	strcpy(cpy, line);

	char* ch = strtok(cpy, ",");

	while(ch)
	{
		char s4[1024];
    	strcpy(s4, ch);
    	// s4 = "  kilometres 26   "

    	char* s3 = trim2(s4);
    	// s3 = "kilometres 26"	

	    char token[1024];

	    // token: column name, s3: column value
		strncpy(token, tokenize2(&s3, token), sizeof token);
		// printf("%s\n", ch);

		if(strcmp(token, colName)==0)
		{
			return s3;

		}

		ch = strtok(NULL, ",");
	}

	char fail[] = "not found";
	char* f = fail;

	return f;

}


int colInSchema(char* line, char* colName)
{
	char cpy[1024];

	strcpy(cpy, line);

	char* ch = strtok(cpy, " ");

	while(ch)
	{
		char s4[1024];
    	strcpy(s4, ch);

    	char temp[100];
    	strcpy(temp, colName);

    	// printf("s: %s\n", s4);
    	// printf("c: %s\n", temp);
    	// s4 = "  kilometres 26   "

    	if(strcmp(s4, temp)==0)
    	{
    		// printf("equal\n");
    		return 0;
    	}
    	// printf("not equal\n");
    	
		ch = strtok(NULL, " ");
	}

	return 1;

}


char* colType1(char* schema, char* colName)
{
	char cpy[1024];

	strcpy(cpy, schema);

	char* ch = strtok(cpy, " ");

	while(ch && strcmp(ch,colName)!=0)
	{
		// printf("%s\n", ch);
		ch = strtok(NULL, " ");
	}

	if(ch!=NULL)
	{
		ch = strtok(NULL, " ");
		// printf("%s\n", ch);
		return ch;
	}
	else
	{
		char fail[] = "not found";
		char* res = fail;
		return res;
	}

}


char* trimXX(char* str)
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



char* format(char* str)
{
	char copy[MAX_CONFIG_LINE_LEN];
	strncpy(copy, str, sizeof copy);
	char* p_copy = copy;

	char res[MAX_CONFIG_LINE_LEN]="";

	char* pp = strtok(p_copy, ",");

	int i = 0;

	while(pp)
	{
		char temp[MAX_CONFIG_LINE_LEN];
		strncpy(temp, pp, sizeof temp);
		
		// printf("%s(%d)\n", temp, strlen(temp));
		char* p_temp = trimXX(temp);

		char temp1[MAX_CONFIG_LINE_LEN];
		strncpy(temp1, p_temp, sizeof temp1);
 	
		// printf("%s(%d)\n", temp1, strlen(temp1));

		char r[MAX_CONFIG_LINE_LEN];
		sprintf(r, "%s,",temp1);

		strcat(res, r);
		
		pp = strtok(NULL, ",");
	}

	int len = strlen(res);
	res[len-1] = '\0';

	// printf("%s(%d)\n", res, strlen(res));

	char* p_res = res;
	
	return p_res;
}

char* ntt(char** str)
{

	*str = trimXX(*str);

	char copy[100];
	strncpy(copy, *str, sizeof copy);
	char* p_copy = copy;

	int i=0;
	int reachedEnd = 0;

	if(isalnum(**str) || **str=='-' || **str=='+')
	{
		i++;
		(*str)++;
		
		while(isalnum(**str) && **str!=' ')
		{
			i++;
			(*str)++;
		}
	}
	else if(**str=='\0')
	{
		reachedEnd = 1;
	}

	else
	{
		(*str)++;
		i=1;
	}


	char x[100];
	strcpy(x, substring(p_copy, 0, i));
	char* res = x;

	return res;
}


void tokens(char* string, char** strMod, char** name, char* operator, char** value) {
    int i = 0, j =0;
    while(string[i] != '\0') {
        if(string[i] == '>') {
            *operator = '>';
            j = i;
        } else if(string[i] == '=') {
            *operator = '=';
            j = i;
        } else if(string[i] == '<') {
            *operator = '<';
            j = i;
        } else {

        }
        i++;
    }

    *name = substring2(string, 0, j);
    *name = trimXX(*name);
    *value = substring2(string, j+1, strlen(string)-j-1);
    *value = trimXX(*value);

}


// >>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>
// >>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>

// HASH TABLE FUNCTIONS

/**
 * @brief Creates a hash table
 *
 * @param name The name of the hash table
 * @param size The size of the hash table
 * 
 * Allocates memory for a hash table, initalizes its elements,
 * and sets the table's size. It also returns the new hash table.
 */
HashTable *create_hash_table(char* name_, char* schema_, int size)
{
    HashTable *new_table;
    
    if (size<1) return NULL; /* invalid size for table */

    /* Attempt to allocate memory for the table structure */
    // if ((new_table = malloc(sizeof(hash_value_t))) == NULL) {
	if ((new_table = malloc(sizeof(HashTable))) == NULL) {
    	return NULL;
    }
    
    /* Attempt to allocate memory for the table itself */
    if ((new_table->table = malloc(sizeof(Node *) * size)) == NULL) {
        return NULL;
    }

    new_table->name = name_;
    new_table->schema = schema_;

    /* Initialize the elements of the table */
    int i;
    for(i=0; i<size; i++) new_table->table[i] = NULL;

    /* Set the table's size */
    new_table->size = size;

    return new_table;
}


/**
* @brief Attains the index at which the given string must be inserted
* 
* @param hashtable Is the pointer to the HashTable structure
* @param str The key provided by the user
*/
unsigned int hash(HashTable *hashtable, char *str)
{
    unsigned int hashval;
    
    /* we start our hash out at 0 */
    hashval = 0;

    /* for each character, we multiply the old hash by 31 and add the current
     * character.  Remember that shifting a number left is equivalent to 
     * multiplying it by 2 raised to the number of places shifted.  So we 
     * are in effect multiplying hashval by 32 and then subtracting hashval.  
     * Why do we do this?  Because shifting and subtraction are much more 
     * efficient operations than multiplication.
     */
    for(; *str != '\0'; str++) 
    	hashval = *str + (hashval << 5) - hashval;

    /* we then return the hash value mod the hashtable size so that it will
     * fit into the necessary range
     */
    return hashval % hashtable->size;
}

/**
 * @brief Finds and returns the list a provided key is in
 * 
 * @param hashtable The pointer to the HashTable structure
 * @param str The key provided by the user
 * @return Returns the list in which the provided key is in, otherwise it
 * 		   returns NULL
 */
Node *lookup_string(HashTable *hashtable, char *str)
{
    Node *list;
    unsigned int hashval = hash(hashtable, str);

    /* Go to the correct list based on the hash value and see if str is
     * in the list.  If it is, return return a pointer to the list element.
     * If it isn't, the item isn't in the table, so return NULL.
     */
    // for(list = hashtable->table[hashval]; list != NULL; list = list->next) {
    //     if (strcmp(str, list->string) == 0) return list;
    // }


    list = hashtable->table[hashval];

    while(list!=NULL)
    {
        if (strcmp(str, list->string) == 0) 
            return list;

        list = list->next;
    }

    return NULL;
}

/**
 * @brief Inserts a string into the hash table
 * 
 * @return Returns 0 for success, 2 if it has deleted
 * 		  the record, 3 if it must be modified, and
 * 		  1 otherwise or if it failed to allocate memory
 */
int add_string(HashTable *hashtable, char *str, struct storage_record* record_)
{
    printf("add string call\n");
    Node *new_list;
    Node *current_list;
    Node* temp;
    unsigned int hashval = hash(hashtable, str);


    // printf("asdsadlsadlsd\n");


    /* Attempt to allocate memory for list */
    if ((new_list = malloc(sizeof(Node))) == NULL)
        return 1;

    /* Does item already exist? */
    current_list = lookup_string(hashtable, str);
    
        /* item already exists, don't insert it again. */
    if (current_list != NULL)
    {
        // delete
        if(record_ == NULL)
        {
            // printf("have to delete.\n");
            // two options:
            // 1. first record, 
            // 2. not the first record in list
            
            //    current_list = current_list->next->next;

            Node* head = hashtable->table[hashval];
            Node* prev = NULL;
            Node* curr = head;
            

            // no nodes at the hashVal
            while(curr!=NULL && strcmp(curr->string, str)!=0)
            {
                prev = curr;
                curr = curr->next;
            }

            // not the first element in the list
            if(prev!=NULL)
                prev->next = curr->next;
            

            // first element in the list
            else
                hashtable->table[hashval] = curr->next;


            bool key_and_table_match = false;
            int i;
            int pos;
            for(i=0; i<count_arrKeyVersion && !key_and_table_match; i++)
            {
                if(strcmp(arrKeyVersion[i].tableName, hashtable->name)==0 && 
                    strcmp(arrKeyVersion[i].key, str)==0)
                {
                    key_and_table_match = true;
                    
                    // copy the table name, key, next version in array
                    strcpy(arrKeyVersion[i].tableName, hashtable->name);

                    strcpy(arrKeyVersion[i].key, str);
                    arrKeyVersion[i].version = (curr->record->metadata[0])+1;

                    pos = i;
                }
            }

            if(!key_and_table_match)
            {
                // copy the table name, key, next version in array
                strcpy(arrKeyVersion[count_arrKeyVersion].tableName, hashtable->name);

                strcpy(arrKeyVersion[count_arrKeyVersion].key, str);
                arrKeyVersion[count_arrKeyVersion].version = (curr->record->metadata[0])+1;

                pos = count_arrKeyVersion;
                count_arrKeyVersion++;
            }
            printf("array:\ntable:%s\nkey:%s\nversion:%d\n",
                arrKeyVersion[pos].tableName, 
                arrKeyVersion[pos].key,
                arrKeyVersion[pos].version);


            free(curr);

            return 2;
        }

        // modify
        else
        {
            // printf("have to modify\n");

            if((record_->metadata[0] == current_list->record->metadata[0]) || record_->metadata[0]==0)
            {
                record_->metadata[0] = (current_list->record->metadata[0]) + 1;
                current_list->record = record_;

                return 3;
            }
            else
            {
                printf("versions don't match. transaction abort.\n");

                return 4;
            }
        }
    }

    /* Insert into list */
    if(record_ != NULL)
    {

        int version = 1;
        bool found = false;
        int i;
        for(i=0; i<count_arrKeyVersion && !found; i++)
        {
            if(strcmp(arrKeyVersion[i].tableName, hashtable->name)==0 && 
            strcmp(arrKeyVersion[i].key, str)==0)
            {
                version = arrKeyVersion[i].version;
                found = true;
            }
        }

        record_->metadata[0] = version;

        new_list->string = strdup(str);
        new_list->record = record_;
        new_list->next = hashtable->table[hashval];
        hashtable->table[hashval] = new_list;
	}

	// the record to be deleted is not in the table
	else
	{
		return 1;		
	}


    return 0;	// record inserted
}



/**
* @brief Deletes the hash table
*
* @param hashtable Is the pointer to the HashTable structure
*/
void free_table(HashTable *hashtable)
{
    int i;
    Node *list, *temp;

    if (hashtable==NULL) return;

    /* Free the memory for every item in the table, including the 
     * strings themselves.
     */
    for(i=0; i<hashtable->size; i++) {
        list = hashtable->table[i];
        while(list!=NULL) {
            temp = list;
            list = list->next;
            free(temp->string);
            // free(temp->record);
            free(temp);
        }
    }

    /* Free the table itself */
    free(hashtable->table);
    free(hashtable);
}


/**
 * @brief Helper function that prints the list that the index points to
 *
 * @param head Pointer that points to top of the Node list
 */
void printList(Node* head)
{
    Node* curr = head;
    int i=0;
    while(curr!=NULL)
    {
        printf("%s,%s-->", curr->string, curr->record->value);
        curr = curr->next;
    }
    printf("\n");
}


/**
 * @brief prints all records in the hashtable
 * 
 * @param hashTable_ Is a pointer to the Hashtable structure
 */
void printAllRecords(HashTable* hashTable_)
{
    int len = hashTable_->size;
    // printf("%d\n", len);
    int i;
    
    Node* head;

    for(i=0; i<len; i++)
    {
        head = hashTable_->table[i];
        // printf("%d: ", i);
        if(head!=NULL)
        	printList(head);
    }
}


/**
 * @brief Helper function that prints the list that the index points to
 *
 * @param head Pointer that points to top of the Node list
 */
int searchRecord(Node* head, char* predicate, char** keys, char* schema)
{

	char temp[1024];
	strcpy(temp, predicate);
	char* t1 = temp;

    printf("predicate in searchRecord: %s(%d)\n", temp, strlen(temp));

	char* first = ntt(&t1);
	char colName[100];
	strcpy(colName, first);
	
	// printf("\n");

	// printf("pred colName: %s\n", colName);
	
	char* second = ntt(&t1);
	char operator[100];
	strcpy(operator, second);
	char op = operator[0];
	
	// printf("\n");

	// printf("pred operator: %c\n", op);
    
    // after the second call to ntt, t1 has the remaining string after
    // the operator
    // trimming t1
    t1 = trimXX(t1);

    // printf("string : %s(%d)\n", t1, strlen(t1));

    // storing the trimmed version of t1 into colVal
	char colVal[100];
	strcpy(colVal, t1);
	
	// printf("\n");

	printf("pred value: %s\n", colVal);

	char* colType = colType1(schema, colName);
	// printf("colType: %s\n", colType);

	int isChar = 1;
	if(strcmp(colType,"char")==0)
		isChar = 0;


	int isNumber = isNum(colVal);
	int pred_v;

	if(isNumber==0)
		pred_v = atoi(colVal);

// "name bloor danforth,stops 21,kilometres 76"

	if(strcmp(colType,"int")==0 && isNumber!=0)
		return -1;

	char k[1024]="";

    Node* curr = head;
    while(curr!=NULL)
    {
    	char* val = colValue(curr->record->value, colName);

    	int v;
    	if(isChar==1)		// integer
    	{
    		v = atoi(val);

    		if(op=='=')
    		{
    			if(v == pred_v)
    			{
    				char k1[100];
    				sprintf(k1, "%s ", curr->string);
    				strcat(k, k1);
    				// printf("value: \"%s\",",curr->record->value);
    			}
    		}
    		else if(op=='<')
    		{
				if(v < pred_v)
				{
					char k1[100];
    				sprintf(k1, "%s ", curr->string);
    				strcat(k, k1);

    				// printf("value: \"%s\",",curr->record->value);
    			}

    		}
    		else if(op=='>')
    		{
    			if(v > pred_v)
    			{
    				char k1[100];
    				sprintf(k1, "%s ", curr->string);
    				strcat(k, k1);

    				// printf("value: \"%s\",",curr->record->value);
    			}
    		}

    		else
    		{
    			return -1;
    		}

    		// printf("record val: %d\n", v);
    		// printf("pred val : %d\n", pred_v);
    	}

    	else		// string
    	{
    		if(op!='=')
    		{
    			return -1;
    		}
    		else
    		{
    			if(strcmp(val, colVal)==0)
    			{
    				char k1[100];
    				sprintf(k1, "%s ", curr->string);
    				strcat(k, k1);

    				// printf("value: \"%s\",",curr->record->value);
    			}
    		}


    	}

    	// printf("val = %s\n", val);



    	// printf("value: \"%s\",",curr->record->value);
        
        curr = curr->next;
    }

    // char* pk = k;

    // char kk[1024];
    // strcpy(kk, *keys);
    strcat(*keys, k);

    // printf("keys: %s\n", k);

    printf("\n");


    return 0;
}


/**
 * @brief prints all records in the hashtable
 * 
 * @param hashTable_ Is a pointer to the Hashtable structure
 */
void searchAllRecords(HashTable* hashTable_, char* predicate, char** keys, 
	char* schema)
{
    int len = hashTable_->size;
    // printf("%d\n", len);
    int i;
    
    Node* head;

    char keys1[1024];
    char* p_keys1 = keys1;

    for(i=0; i<len; i++)
    {
        head = hashTable_->table[i];
        // printf("%d: ", i);
        if(head!=NULL)
        {
        	int res = searchRecord(head, predicate, &p_keys1, schema);
        	if(res!=0)
        	{
        		printf("error\n");
        		return;
        	}
        }
    }
    // printf("%s\n", p_keys1);
    char tt[1024];
    strcpy(tt, p_keys1);

    // printf("%s\n", tt);


    strcpy(*keys, tt);

    // printf("%s\n", *keys);
  	
}




char* substringNextTokSchema(const char* str, size_t begin, size_t len) 
{ 
  if (str == 0 || strlen(str) == 0 || strlen(str) < begin || strlen(str) < len) 
    return 0; 

  return strndup(str + begin, len-begin); 
} 


// tokenizer for schema string
// doesn't modify any parameters
char* nextTokSchema(char* str, int* i)
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
    char* tok = substringNextTokSchema(str, *i, j);
    j++;
    *i = j;
    return tok;
}



int parser_query(char* schema, char* str)
{
	bool inputIsValid = true;


	int i = 0;
	char* schemaCopy = schema;

	char schemaTemp[MAX_CONFIG_LINE_LEN];
	strncpy(schemaTemp, schema, sizeof schemaTemp);
	// int numOfColsSchema = getNumberOfColsSchema(schemaTemp);


	// printf("schema:%s\n", schema);	

	char inputTemp[MAX_CONFIG_LINE_LEN];
	strncpy(inputTemp, str, sizeof inputTemp);

	// int numOfColsInput = getNumberOfColsInput(inputTemp);

	char* ss;
	char* s1;

    ss = str;		// pointer to the string for strtok
    s1 = strtok(ss,",");		// getting first comma separated token, i.e "  name    Bloor   Danforth "


    while(s1 && inputIsValid)
    {
    	char* schColName;
    	char* schColType;
    	char* schColLength;
    	int schColLength_int;
	    if(i <= strlen(schemaCopy))
	    {
	        schColName = nextTokSchema(schemaCopy,&i);
	        // printf("%s,", schColName);
	        schColType = nextTokSchema(schemaCopy,&i);
	        // printf("%s,", schColType);

	        if(strcmp(schColType,"char")==0)
	        {
	        	schColLength = nextTokSchema(schemaCopy,&i);
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
    	// s4 = "  stops > 23   "

    	// char* s3 = trim(s4);
    	// s3 = "stops > 23"	

    	// char* first = nextToken(&s4);
    	// char* second = nextToken(&s4);
    	// char* third = nextToken(&s4);


/*
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
		}

*/
		// getting next colName,colValue
		s1 = strtok(NULL,",");		

	}

	return 0;
}

/**
 * @brief Process a command from the client.
 *
 * @param sock The socket connected to the client.
 * @param cmd The command received from the client.
 * @param params Stores the configuration parameters such as username
 * 		  serverhost, serverport, password, and the table names from
 * 		  the configuration file
 * @return Returns 0 on success, -1 otherwise.
 */

int handle_command(int sock, char *cmd,  struct config_params *params_)
{
	
	// printf("testing: %s\n", cmd);
	
	// strtok modifies the string, and so we need to create a copy 
	char cmdCopy[MAX_CMD_LEN];
	strncpy(cmdCopy, cmd, sizeof cmdCopy);
	char *cmd1;
	// storing the command(i.e AUTH/GET/SET) in cmd1
	cmd1 = strtok(cmdCopy,";");
	printf("%s\n", cmd1);

	char username_[MAX_USERNAME_LEN];
	char password_[MAX_ENC_PASSWORD_LEN];
	char table_[MAX_TABLE_LEN];
	char key_[MAX_KEY_LEN];
	char value_[MAX_VALUE_LEN];

	char authenticateFail[MAX_CMD_LEN] = "fail\n";
	char tableNotFound[MAX_CMD_LEN] = "tableNotFound\n";
	char recordNotFound[MAX_CMD_LEN] = "recordNotFound\n";

	char recordInserted[MAX_CMD_LEN] = "recordInserted\n";
	char recordDeleted[MAX_CMD_LEN] = "recordDeleted\n";
	char recordModified[MAX_CMD_LEN] = "recordModified\n";
	char invalidParameter[MAX_CMD_LEN] = "invalidParameter\n";
    char transactionAborted[MAX_CMD_LEN] = "transactionAborted\n";


	char recordDetails[MAX_CMD_LEN];

	
	if(strcmp(cmd1, "AUTH") == 0)
	{
		strcpy(username_, strtok(NULL, ";"));
		strcpy(password_, strtok(NULL, ";"));
		// printf("username: %s, password: %s\n", username_, password_);

		char out[50];

		if(strcmp(params_->username, username_)!=0 || strcmp(params_->password, password_)!=0)
		{

			// if the control enters this block
			// it means, the the username and password didn't match
			// as in the config file

			sendall(sock, authenticateFail, strlen(authenticateFail));
			
			sprintf(out, "[LOG SERVER] Authentication failed\n");
			logger(out, LOGGING);
			memset(&out[0], 0, sizeof out);

		}

		else
		{

			sprintf(out, "[LOG SERVER] Authentication successful\n");
			logger(out, LOGGING);
			memset(&out[0], 0, sizeof out);


			sendall(sock, cmd, strlen(cmd));
			sendall(sock, "\n", 1);
		
		}	

	}

	else if(strcmp(cmd1, "GET") == 0)
	{
		strcpy(table_, strtok(NULL, ";"));
		strcpy(key_, strtok(NULL, ";"));
		// printf("table: %s, key: %s\n", table_, key_);


		HashTable* my_hash_table = NULL;

		int i;
		for(i=0; i<numberOfTables; i++)
		{
			if(strcmp(allTables[i]->name, table_)==0)
			{
				// table found
				my_hash_table = allTables[i];
			}
		}

		if(my_hash_table == NULL)
		{
			// printf("table not found.\n");
			sendall(sock, tableNotFound, strlen(tableNotFound));
		}

		else
		{

			Node* l = lookup_string(my_hash_table, key_);


			// printAllRecords(my_hash_table);


			if(l == NULL)
			{
        		// printf("Record not Found.\n");
        		sendall(sock, recordNotFound, strlen(recordNotFound));
        	}
		    else
		    {
		        // sprintf(recordDetails, "%s\n",l->record->value);
		        sprintf(recordDetails, "%s;%d\n",l->record->value, l->record->metadata[0]);
                
		        // printf("%s\n", recordDetails);

        		sendall(sock, recordDetails, strlen(recordDetails));

		    }

		}

	}

	else if(strcmp(cmd1, "SET") == 0)
    {
        char value1[MAX_VALUE_LEN];
        char clientVersion[10];

        strcpy(table_, strtok(NULL, ";"));
        strcpy(key_, strtok(NULL, ";"));
        strcpy(value1, strtok(NULL, ";"));
        strcpy(clientVersion, strtok(NULL, ";"));
        int clientVersion_int = atoi(clientVersion);
        
        printf("table: %s, key: %s, value: %s, c_version: %d\n", table_, key_, value1, clientVersion_int);
        // printf("value: %s\n", value_);

        HashTable* my_hash_table = NULL;

        int i;
        // printf("%d\n", numberOfTables);
        for(i=0; i<numberOfTables; i++)
        {
            // printf("%s, %s\n", allTables[i], table_);
            if(strcmp(allTables[i]->name, table_)==0)
            {
                // table found
                my_hash_table = allTables[i];
            }
        }

        if(my_hash_table == NULL)
        {
            // printf("table not found.\n");
            sendall(sock, tableNotFound, strlen(tableNotFound));
        }

        else
        {
            // printf("table found.\n");
            char *res[2] = {"successful", "unsuccessful"};

            char schema[MAX_CONFIG_LINE_LEN];
            strncpy(schema, my_hash_table->schema, sizeof schema);

            // printf("%s,%d\n", schema, strlen(schema));
            // char schema[MAX_CONFIG_LINE_LEN] = "name char 30 stops int kilometres int ";
            // printf("%s,%d\n", schema, strlen(schema));

            int len = strlen(schema);
            schema[len-1]='\0';

            // printf("%s,%d\n", schema, strlen(schema));

            // printf("%s,%d\n", value_, strlen(value_));

            // char val[MAX_CONFIG_LINE_LEN] = "  name Bloor Danforth,stops 31, kilometres 26 ";

            // printf("%s\n", schema);

            char* x = format(value1);
            strcpy(value_, x);

            // printf("value_ = %s\n", value_);

            int result = parser(schema, value1);

            // printf("result = %d\n", result);

            // printf("%s\n", res[result]);



            struct storage_record* record_p;

            int retVal_addString;


            // record has to be NULL, so delete record
            if(strcmp(value_, "deleteRecord")==0)
            {
                // printf("have to delete record\n" );
                record_p = NULL;
            }

            else
            {
                // printf("insert or modify record\n");
                // int insert = 0;      // 0-modify, 1-insert

                // Node* l = lookup_string(my_hash_table, key_);
                // if(l==NULL)
                //  insert=1;       // insert
                // else
                //  insert=0;       // modify
            

                // check if 
                // 1. columns are in order, not missing
                // 2. data type matches with schema
                // 3. if char[], length doesn't exceed the length defined in the schema
                if(result == 1)
                {
                    sendall(sock, invalidParameter, strlen(invalidParameter));
                }


                strncpy(rec1[recCt].value, value_, sizeof rec1[recCt].value);
                record_p = &(rec1[recCt]);
                record_p->metadata[0] = clientVersion_int;


                recCt++;
            }

            // if(my_hash_table == NULL)
            //  printf("empty hashTable_\n");
            // else
            //  printf("not empty\n");

            retVal_addString = add_string(my_hash_table, key_, record_p);

            // printf("ret val = %d\n", retVal_addString);


            // Node* l = lookup_string(my_hash_table, key_);
            // if(l==NULL)
            //  printf("record not found.\n");
            // else
            //  printf("record found. %s\n", l->record->value);


            // printAllRecords(my_hash_table);

            // record not found
            if(retVal_addString == 1)
            {
                sendall(sock, recordNotFound, strlen(recordNotFound));
            }

            // record inserted
            else if(retVal_addString == 0)
            {
                sendall(sock, recordInserted, strlen(recordInserted));
            }

            // record modified
            else if(retVal_addString == 3)
            {
                sendall(sock, recordModified, strlen(recordModified));
            }

            // record deleted
            else if(retVal_addString == 2)
            {
                sendall(sock, recordDeleted, strlen(recordDeleted));            
            }

            // transcation abort
            else if(retVal_addString == 4)
            {
                sendall(sock, transactionAborted, strlen(transactionAborted));
            }


            // not any of the above
            else
            {
                sendall(sock, cmd, strlen(cmd));
                sendall(sock, "\n", 1);
            }


        }

        // if(record_p == NULL)
        //  printf("null record\n");
        // else
        //  printf("not null record\n");

    }


	else if(strcmp(cmd1, "QUERY") == 0)
	{

		char predicates[MAX_CONFIG_LINE_LEN];

		strcpy(table_, strtok(NULL, ";"));
		strcpy(predicates, strtok(NULL, ";"));
		
		HashTable* my_hash_table = NULL;

		int i;
		// printf("%d\n", numberOfTables);
		for(i=0; i<numberOfTables; i++)
		{
			// printf("%s, %s\n", allTables[i], table_);
			if(strcmp(allTables[i]->name, table_)==0)
			{
				// table found
				my_hash_table = allTables[i];
			}
		}

		if(my_hash_table == NULL)
		{
			// printf("table not found.\n");
			// printf("%d\n", strlen(tableNotFound));
			sendall(sock, tableNotFound, strlen(tableNotFound));
		}

		else
		{

			char schema[MAX_CONFIG_LINE_LEN];
			strncpy(schema, my_hash_table->schema, sizeof schema);

			// cshar pred1[MAX_CONFIG_LINE_LEN] = "stops > 24";

			// char keys[MAX_CONFIG_LINE_LEN];

			// char * keys1 = keys;

			// searchAllRecords(my_hash_table, pred1, &keys1, schema);

			// printf("keys: %s\n", keys1);


			char predicates_copy[50];

    		// predicates = " name = bloor danforth, stops > 12"

		    strncpy(predicates_copy, predicates, sizeof predicates_copy);

		    // printf("%s(%d)\n", predicates_copy, strlen(predicates_copy));
		    char* pp = predicates_copy;

		    char* pp_tok = strtok(pp, ",");
		    //pp = strtok(NULL, '\0');

		    int x = 0;

		    while(pp_tok)
		    {

		        // printf("%s\n", pp_tok);
		        char trimmed_pp_tok[50];
		        strcpy(trimmed_pp_tok, pp_tok);

		        char* p_trimmed_pp_tok = trimmed_pp_tok;
		        p_trimmed_pp_tok = trimXX(trimmed_pp_tok);
		        // trimmed_pp_tok is not modified
		        char temp[50];
		        strcpy(temp, p_trimmed_pp_tok);

		        // printf("%s(%d)\n", p_trimmed_pp_tok, strlen(p_trimmed_pp_tok));

		        char* value;
		        char* name;
		        char operator;

		        tokens(trimmed_pp_tok, &p_trimmed_pp_tok, &name, &operator, &value);

		        // printf("Name: %s\n", name);
		        // printf("Operator: %c\n", operator);
		        // printf("Value: %s\n", value);

		        pred[x].name_ = name;
		        pred[x].operator_ = operator;
		        pred[x].value_ = value;

		        pp_tok = strtok(NULL, ",");
		        x++;
		    }

		    int p;
		    int invalid = 0;
		    for(p=0;p<x && invalid==0;p++)
		    {
		    	char n1[100];
		        strcpy(n1, pred[p].name_);
		        printf("n1: %s\n", n1);

		       	char n2;
		        n2 = pred[p].operator_;
		        printf("n2: %c\n", n2);

		       	char n3[100];
		        strcpy(n3, pred[p].value_);
		        printf("n3: %s\n", n3);


		        char sch1[100];
		        strcpy(sch1, schema);
		        // printf("sch1: %s\n", sch1);

		        // checking if column is in schema
		        int rr = colInSchema(sch1, n1);

		       	if(rr==1)
		       	{
		       		invalid = 1;
		       		continue;
		       	}

		       	char* ss = colType1(schema, n1);

				int isChar1 = 1;
				if(strcmp(ss,"char")==0)
					isChar1 = 0;	// is string


				int isNumber1 = isNum(n3);	//0: int, 1: char*

				if(isChar1==0) //0: string, 1: int
				{
					if(n2!='=')
						invalid = 1;
				}
				else		// column is int
				{
					if(n2!='=' && n2!='<' && n2!='>')
					{
						invalid = 1;
					} 

					else if(isNumber1==1)
					{
						invalid = 1;
					}
				}


		    }


		    if(invalid==1)
		    {
		    	sendall(sock, invalidParameter, strlen(invalidParameter));
		    }

			else
			{
			    Keys arrKeys[10];

			    int a;
	            char pred_str[1024];

	            char keys[MAX_CONFIG_LINE_LEN];
	            char * keys1 = keys;

	            char allKeys[x][1024];
	            char empty[10] ="empty";

	            int i;
	            for(i=0; i<10; i++)
	            {
	                strcpy(arrKeys[i].key, empty);
	                arrKeys[i].count = 0;

	                // printf("%s\n", arrKeys[i].key);
	            }



			    for(a = 0; a < x; a++) 
	            {
			        // printf("Struct name: %s\n", pred[a].name_);
			        // printf("Struct operator: %c\n", pred[a].operator_);
			        // printf("Struct value: %s\n", pred[a].value_);
			        
			        sprintf(pred_str, "%s %c %s",pred[a].name_, pred[a].operator_, pred[a].value_);
			        // printf("%s\n", pred_str);

			        searchAllRecords(my_hash_table, pred_str, &keys1, schema);

	                keys1 = keys1;

			        // printf("keys: %s\n", keys1);

	                strncpy(allKeys[a], keys1, sizeof allKeys[a]);

	                // printf("allkeys[%d]: %s\n", a, allKeys[a]);

			    }

	            int ct = 0;

	            char copy[1024];
	            char* ch;

	            for (i = 0; i < 10; i++)
	            {

	                strncpy(copy, allKeys[i], sizeof copy);

	                // printf("copy=%s\n", copy);

	                ch = strtok(copy, " ");

	                // printf("ch=%s\n", ch);

	                // ch = strtok(NULL, " ");
	                // printf("ch=%s\n", ch);
	                while(ch)
	                {
	                    // printf("ch=%s, ", ch);


	                    char temp[10];
	                    strcpy(temp, ch);
	                    // printf("temp=%s\n", temp);


	                    int inserted = 0;
	                    int j;
	                    for(j=0; j<10 && inserted==0; j++)
	                    {
	                        // printf("temp=%s\n", temp);

	                        if(strcmp(arrKeys[j].key, "empty")==0)
	                        {
	                            strcpy(arrKeys[j].key, temp);
	                            arrKeys[j].count = 1;
	                            inserted = 1;

	                            // printf("%s\n", arrKeys[j].key);
	                        }
	                        else
	                        {

	                            // printf("not empty\n");
	                            if(strcmp(arrKeys[j].key, temp)==0)
	                            {
	                                // printf("matches\n");
	                                arrKeys[j].count = arrKeys[j].count + 1;
	                                inserted = 1;

	                                // printf("%s\n", arrKeys[j].key);
	                            }


	                        }

	                    }

	                    ch = strtok(NULL, " ");
	                }
	                printf("\n");

	                // printf("%s\n", copy);

	                // printf("%s\n", allKeys[i]);
	   

	            }

	            char arr[100]="";
	            char t1[10];

	            for(i=0; i<10; i++)
	            {
	                // arrKeys[i].key = "empty";
	                // arrKeys[i].count = 0;
	                if(strcmp(arrKeys[i].key, "empty")!=0 && arrKeys[i].count>x)
	                {
	                    // printf("key: %s, count: %d\n", arrKeys[i].key, arrKeys[i].count);
	                    sprintf(t1, "%s ",arrKeys[i].key);
	                    strcat(arr,t1);
	                }
	                else if(strcmp(arrKeys[i].key, "empty")!=0 && x==1)
	                {

	                	// printf("key: %s, count: %d\n", arrKeys[i].key, arrKeys[i].count);
	                    sprintf(t1, "%s ",arrKeys[i].key);
	                    strcat(arr,t1);
	                }
	            }  
	   			
	   			char arr1[100];

	   			int lk = 0;

	            if(x==1)
	            {

		            strcpy(arr1, arr);
		            int len = strlen(arr1);
		            
		            int i=0;

		            while(arr1[i]!='\0')
		            {
		            	if(arr1[i]==' ')
		            		lk = i;

		            	i++;
		            }


		            // printf("lk=%d\n", lk);
					arr1[lk-1]='\0';
					arr[lk-1]='\0';
					
				}



				// arr1[10] = '\0';

				// printf("arr1: %s\n", arr1);


	            printf("final array : %s\n", arr);

		        sendall(sock, arr, strlen(arr));
				sendall(sock, "\n", 1);
			}
		    // printf("number of predicates = %d\n", x);

		}


	}

	char out[50];
	sprintf(out, "[LOG SERVER] Processing command '%s'\n", cmd);
	logger(out, LOGGING);

	// For now, just send back the command to the client.
	
	// sendall(sock, cmd, strlen(cmd));
	// sendall(sock, "\n", 1);

	return 0;
}

/**
 * @brief Loads the key and value pair from the input file into
 * 		  the database
 *
 * @param table_census Hash table to hold census data.
 * @param line Individual line from census data.
 * @param ct Temporary counter that points to the index in the object
 * 		  array that needs to be inserted into the database. The 
 * 		  input census data is stored in an array of objects, which
 *		  has the members key, record and next.
 */
int processCensus(HashTable* table_census, char *line, int ct)
{

	// Extract key and value
    char name[MAX_CMD_LEN];
    char val[MAX_CMD_LEN];
    int items = sscanf(line, "%s %s\n", name, val);

    char key[MAX_KEY_LEN];
    char value[MAX_VALUE_LEN];

    if(strlen(name)<=MAX_KEY_LEN && strlen(val)<=MAX_VALUE_LEN)
    {
    	strncpy(key, name, MAX_KEY_LEN);
    	strncpy(value, val, MAX_VALUE_LEN);

   		strncpy(obj[ct].value, value, MAX_VALUE_LEN);

   		int retVal_addString = add_string(table_census, name, &(obj[ct]));
    }

    // printf("%s, %s\n", name, value);
 

    // printf("%s, %s\n", name, obj[ct].value);



    return 0;
}

/**
 * @brief Start the storage server.
 *
 * @param argc Number of arguments passed when the server is started
 * @param argv An array that contains the tokens from the command
 * 		  line 
 * 
 * This is the main entry point for the storage server.  It reads the
 * configuration file, starts listening on a port, and proccesses
 * commands from clients.
 */
int main(int argc, char *argv[])
{

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
        sprintf(filename, "Server-%d-%02d-%02d-%02d-%02d-%02d.log", year, month, day, hour, min, sec);

        log = fopen(filename, "w");
    } else if(LOGGING == 1)
        log = stdout;

    // Process command line arguments.
    // This program expects exactly one argument: the config file name.
    assert(argc > 0);
    if (argc != 2) {
        printf("Usage %s <config_file>\n", argv[0]);
        exit(EXIT_FAILURE);
    }
    char *config_file = argv[1];

    // Read the config file.
    struct config_params params;

    // initializing the number of tables to zero
    params.numOfTables = 0;

    params.numOfServerHosts = 0;
    params.numOfServerPorts = 0;
    params.numOfUsernames = 0;
    params.numOfPasswords = 0;

    int status = read_config(config_file, &params);
    int concurrencyVal;

    if(status==0)
    {
        printf("serverHost : %s\nserverPort : %d\nusername : %s\npassword : %s\nconcurrency : %d\nnumberOfTables : %d\nnumberOfHosts : %d\nnumberOfPorts : %d\nnumberOfUsernames : %d\nnumberOfPasswords : %d\n",
            params.server_host, params.server_port, params.username, params.password, 
            params.concurrency, params.numOfTables, params.numOfServerHosts, params.numOfServerPorts, params.numOfUsernames, params.numOfPasswords);

        concurrencyVal = params.concurrency;


        if(concurrencyVal!=0 && concurrencyVal!=1)
        {
            printf("concurrency method not implemented\n");
            exit(EXIT_FAILURE);
        }


        int i;
        for(i=0;i<params.numOfTables;i++)
        {
            printf("%s : %s\n", params.tableArray[i], params.tableSchemaArray[i]);
        }

    }
    else
    {
        printf("Error parsing configuration file!\n");
        exit(EXIT_FAILURE);
    }

    numberOfTables = params.numOfTables;




    int j;
    for(j=0; j<numberOfTables; j++)
    {
        // storing the name of the table in newTableName
        char* newTableName = params.tableArray[j];
        char* schema = params.tableSchemaArray[j];

        // create a new table with name = newTableName
        allTables[j] = create_hash_table(newTableName, schema, MAX_RECORDS_PER_TABLE);

        if(allTables[j] == NULL)
            printf("table %s was not allocated\n", allTables[j]->name);
        else
        {
            printf("table : %s, schema : %s\n", allTables[j]->name, allTables[j]->schema);          
        }

    }

    // LOG(("Server on %s:%d\n", params.server_host, params.server_port));
    char out[50];
    sprintf(out, "[LOG SERVER] Server on %s:%d\n", params.server_host, params.server_port);
    logger(out, LOGGING);

    // Create a socket.
    int listensock = socket(PF_INET, SOCK_STREAM, 0);
    if (listensock < 0) {
        printf("Error creating socket.\n");
        exit(EXIT_FAILURE);
    }

    // Allow listening port to be reused if defunct.
    int yes = 1;
    status = setsockopt(listensock, SOL_SOCKET, SO_REUSEADDR, &yes, sizeof yes);
    if (status != 0) {
        printf("Error configuring socket.\n");
        exit(EXIT_FAILURE);
    }

    // Bind it to the listening port.
    struct sockaddr_in listenaddr;
    memset(&listenaddr, 0, sizeof listenaddr);
    listenaddr.sin_family = AF_INET;
    listenaddr.sin_port = htons(params.server_port);
    inet_pton(AF_INET, params.server_host, &(listenaddr.sin_addr)); // bind to local IP address
    status = bind(listensock, (struct sockaddr*) &listenaddr, sizeof listenaddr);
    if (status != 0) {
        printf("Error binding socket.\n");
        exit(EXIT_FAILURE);
    }

    // Listen for connections.
    status = listen(listensock, MAX_LISTENQUEUELEN);
    if (status != 0) {
        printf("Error listening on socket.\n");
        exit(EXIT_FAILURE);
    }



    if(concurrencyVal==0)
    {
                // Listen loop.
        int wait_for_connections = 1;
        while (wait_for_connections) 
        {
            // Wait for a connection.
            struct sockaddr_in clientaddr;
            socklen_t clientaddrlen = sizeof clientaddr;
            int clientsock = accept(listensock, (struct sockaddr*)&clientaddr, &clientaddrlen);
            if (clientsock < 0) 
            {
                printf("Error accepting a connection.\n");
                exit(EXIT_FAILURE);
            }

            // LOG(("Got a connection from %s:%d.\n", inet_ntoa(clientaddr.sin_addr), clientaddr.sin_port));
            char out2[50];
            sprintf(out2, "[LOG SERVER] Got a connection from %s:%d.\n", inet_ntoa(clientaddr.sin_addr), clientaddr.sin_port);
            logger(out2, LOGGING);


            // Get commands from client.
            int wait_for_commands = 1;
            do {
                // Read a line from the client.
                char cmd[MAX_CMD_LEN];
                int status = recvline(clientsock, cmd, MAX_CMD_LEN);

                if (status != 0) 
                {
                    // Either an error occurred or the client closed the connection.
                    wait_for_commands = 0;
                } 
                else 
                {
                    // Handle the command from the client.
                    int status = handle_command(clientsock, cmd, &params);
                    if (status != 0)
                        wait_for_commands = 0; // Oops.  An error occured.
                }

            } while (wait_for_commands);
            

            // Close the connection with the client.
            close(clientsock);

            // Logging for closing the connection
            char out3[50];
            sprintf(out3, "[LOG SERVER] Closed connection from %s:%d.\n", inet_ntoa(clientaddr.sin_addr), clientaddr.sin_port);
            logger(out3, LOGGING);
        }

    // Stop listening for connections.
    close(listensock);
    }


    else if(concurrencyVal==1)
    {

        /* First, we allocate the thread pool */ 
        int i;
        for (i = 0; i!=MAX_THREADS; ++i)
            runtimeThreads[i] = malloc( sizeof( struct _ThreadInfo ) ); 


        while (1) 
        { 
            ThreadInfo tiInfo = getThreadInfo(); 
            tiInfo->clientaddrlen = sizeof(struct sockaddr_in); 
            tiInfo->clientsock = accept(listensock, (struct sockaddr*)&tiInfo->clientaddr, &tiInfo->clientaddrlen);
            tiInfo->params = &params;        
 
            if (tiInfo->clientsock <0)
            {
                pthread_mutex_lock( &printMutex ); 
                printf("ERROR in connecting to %s:%d.\n", 
                inet_ntoa(tiInfo->clientaddr.sin_addr), tiInfo->clientaddr.sin_port);
                pthread_mutex_unlock( &printMutex ); 
            } 
            else 
            {
                pthread_create( &tiInfo->theThread, NULL, threadCallFunction, tiInfo ); 
            }
        }


        /* At the end, wait until all connections close */ 
        for (i=topRT; i!=botRT; i = (i+1)%MAX_THREADS)
            pthread_join(runtimeThreads[i]->theThread, 0 ); 


        /* Deallocate all the resources */ 
        for (i=0; i!=MAX_THREADS; i++)
            free( runtimeThreads[i] ); 


    }





        // Logging for closing the connection
    // char out3[50];
    // sprintf(out3, "[LOG SERVER] Closed connection from %s:%d.\n", inet_ntoa(tiInfo.clientaddr.sin_addr), clientaddr.sin_port);
 //    logger(out3, LOGGING);

    int i;
    for(i=0; i<numberOfTables; i++)
    {   
        // printAllRecords(allTables[i]);
        free_table(allTables[i]);
    }

    return EXIT_SUCCESS;
}

