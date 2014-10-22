#include <stdlib.h>
#include <unistd.h>
#include <stdio.h>
#include <string.h>
#include <fcntl.h>
#include <check.h>
#include <sys/stat.h>
#include <sys/types.h>
#include <sys/wait.h>
#include <errno.h>
#include <math.h>
#include "storage.h"

#define TESTTIMEOUT	10		// How long to wait for each test to run.
#define SERVEREXEC	"./server"	// Server executable file.
#define SERVEROUT	"default.serverout"	// File where the server's output is stored.
#define SERVEROUT_MODE	0666		// Permissions of the server ouptut file.
#define SIMPLETABLES_CONF		"conf-simpletables.conf"	// Server configuration file with simple tables.
#define BADTYPE_CONF		"conf-badtype.conf"	// Server configuration file with bad type tables.
#define NEGSIZE_CONF         "conf-negsize.conf" // Server configuration file with negative char size.
#define NOCOMMA_CONF         "conf-nocomma.conf" // Server configuration file with no comma between columns.
#define DUPLICATE_COLUMN_TYPES_CONF     "conf-duplicatetablecoltype.conf"        // Server configuration file with duplicate column types.
#define BADTABLE	"bad table"	// A bad table name.
#define BADKEY		"bad key"	// A bad key name.
#define KEY		"somekey"	// A key used in the test cases.
#define KEY1		"somekey1"	// A key used in the test cases.
#define KEY2		"somekey2"	// A key used in the test cases.
#define KEY3		"somekey3"	// A key used in the test cases.
#define KEY4		"somekey4"	// A key used in the test cases.
#define VALUESPC	"someval 4"	// A value used in the test cases.
#define INTCOL		"col"		// An integer column
#define INTVALUE	"22"		// An integer value
#define INTCOLVAL	"col 22"	// An integer column name and value

// These settings should correspond to what's in the config file.
#define SERVERHOST	"localhost"	// The hostname where the server is running.
#define SERVERPORT	4848		// The port where the server is running.
#define SERVERUSERNAME	"admin"		// The server username
#define SERVERPASSWORD	"dog4sale"	// The server password
//#define SERVERPUBLICKEY	"keys/public.pem"	// The server public key
// #define DATADIR		"./mydata/"	// The data directory.
#define TABLE		"inttbl"	// The table to use.
#define INTTABLE	"inttbl"	// The first simple table.
//#define FLOATTABLE	"floattbl"	// The second simple table.
#define STRTABLE	"strtbl"	// The third simple table.
#define MISSINGTABLE	"missingtable"	// A non-existing table.
#define MISSINGKEY	"missingkey"	// A non-existing key.

#define FLOATTOLERANCE  0.0001		// How much a float value can be off by (due to type conversions).

/* Server port used by test */
int server_port;

/**
 * @brief Compare whether two floating point numbers are almost the same.
 * @return 0 if the numbers are almost the same, -1 otherwise.
 */
int floatcmp(float a, float b)
{
	if (fabs(a - b) < FLOATTOLERANCE)
		return 0;
	else
		return -1;
}

/**
 * @brief Remove trailing spaces from a string.
 * @param str The string to trim.
 * @return The modified string.
 */
char* trimtrailingspc(char *str)
{
	// Make sure string isn't null.
	if (str == NULL)
		return NULL;

	// Trim spaces from the end.
	int i = 0;
	for (i = strlen(str) - 1; i >= 0; i--) {
		if (str[i] == ' ')
			str[i] = '\0';
		else
			break; // stop if it's not a space.
	}
	return str;
}

/**
 * @brief Start the storage server.
 *
 * @param config_file The configuration file the server should use.
 * @param status Status info about the server (from waitpid).
 * @param serverout_file File where server output is stored.
 * @return Return server process id on success, or -1 otherwise.
 */
int start_server(char *config_file, int *status, const char *serverout_file)
{
	sleep(1);       // Give the OS enough time to kill previous process

	pid_t childpid = fork();
	if (childpid < 0) {
		// Failed to create child.
		return -1;
	} else if (childpid == 0) {
		// The child.

		// Redirect stdout and stderr to a file.
		const char *outfile = serverout_file == NULL ? SERVEROUT : serverout_file;
		//int outfd = creat(outfile, SERVEROUT_MODE);
		int outfd = open(outfile, O_CREAT|O_WRONLY, SERVEROUT_MODE);
		close(STDOUT_FILENO);
		close(STDERR_FILENO);
		if (dup2(outfd, STDOUT_FILENO) < 0 || dup2(outfd, STDERR_FILENO) < 0) {
			perror("dup2 error");
			return -1;
		}

		// Start the server
		execl(SERVEREXEC, SERVEREXEC, config_file, NULL);

		// Should never get here.
		perror("Couldn't start server");
		exit(EXIT_FAILURE);
	} else {
		// The parent.

		// If the child terminates quickly, then there was probably a
		// problem running the server (e.g., config file not found).
		sleep(1);
		int pid = waitpid(childpid, status, WNOHANG);
		//printf("Parent returned %d with child status %d\n", pid, WEXITSTATUS(*status));
		if (pid == childpid)
			return -1; // Probably a problem starting the server.
		else
			return childpid; // Probably ok.
	}
}

/**
 * @brief Start the server, and connect to it.
 * @return A connection to the server if successful.
 */
void* start_connect(char *config_file, char *serverout_file, int *serverpid)
{
	// Start the server.
	int pid = start_server(config_file, NULL, serverout_file);
	fail_unless(pid > 0, "Server didn't run properly.");
	if (serverpid != NULL)
		*serverpid = pid;

	// Connect to the server.
	void *conn = storage_connect(SERVERHOST, server_port);
	fail_unless(conn != NULL, "Couldn't connect to server.");

	// Authenticate with the server.
	int status = storage_auth(SERVERUSERNAME,
				  SERVERPASSWORD,
				  //SERVERPUBLICKEY,
				      conn);
	fail_unless(status == 0, "Authentication failed.");

	return conn;
}

/**
 * @brief Delete the data directory, start the server, and connect to it.
 * @return A connection to the server if successful.
 */
void* clean_start_connect(char *config_file, char *serverout_file, int *serverpid)
{
	// Delete the data directory.
//	system("rm -rf " DATADIR);

	return start_connect(config_file, serverout_file, serverpid);
}

/**
 * @brief Create an empty data directory, start the server, and connect to it.
 * @return A connection to the server if successful.
 */
void* init_start_connect(char *config_file, char *serverout_file, int *serverpid)
{
	// Delete the data directory.
//	system("rm -rf " DATADIR);
	
	// Create the data directory.
//	mkdir(DATADIR, 0777);

	return start_connect(config_file, serverout_file, serverpid);
}

/**
 * @brief Kill the server with given pid.
 * @return 0 on success, -1 on error.
 */
int kill_server(int pid)
{
	int status = kill(pid, SIGKILL);
	fail_unless(status == 0, "Couldn't kill server.");
	return status;
}


/// Connection used by test fixture.
void *test_conn = NULL;


// Keys array used by test fixture.
char* test_keys[MAX_RECORDS_PER_TABLE];


/**
 * @brief Text fixture teardown.  Disconnect from the server.
 */
void test_teardown()
{
	// Disconnect from the server.
	storage_disconnect(test_conn);
	//fail_unless(status == 0, "Error disconnecting from the server.");
}


/**
 * This test makes sure that the storage.h file has not been modified.
 */
START_TEST (test_sanity_filemod)
{
	// Compare with the original version of storage.h.
	int status = system("md5sum --status -c md5sum.check &> /dev/null");
	fail_if(status == -1, "Error computing md5sum of storage.h.");
	int matches = WIFEXITED(status) && WEXITSTATUS(status) == 0;

	// Fail if it doesn't match original version.
	fail_if(!matches, "storage.h has been modified.");
}
END_TEST


/*
 * Test how the server processes the configuration file.
 *
 * Config file error tests:
 * 	start server with bad table specs (fail)
 *  start server with no comma between columns
 *  start server with negative value for char size
 *  start server with duplicate tables
 */

START_TEST (test_configerror_nocomma)
{
	int serverpid = start_server(NOCOMMA_CONF, NULL, "test_configerror_nocomma.serverout");
	fail_unless(serverpid == -1, "Server should exit due to error in config file.");
}
END_TEST

START_TEST (test_configerror_negsize)
{
	int serverpid = start_server(NEGSIZE_CONF, NULL, "test_configerror_negsize.serverout");
	fail_unless(serverpid == -1, "Server should exit due to error in config file.");
}
END_TEST

START_TEST (test_configerror_duplicate)
{
	int serverpid = start_server(DUPLICATE_COLUMN_TYPES_CONF, NULL, "test_configerror_duplicate.serverout");
	fail_unless(serverpid == -1, "Server should exit due to error in config file.");
}
END_TEST

START_TEST (test_configerror_badtype)
{
	int serverpid = start_server(BADTYPE_CONF, NULL, "test_configerror_badtype.serverout");
	fail_unless(serverpid == -1, "Server should exit due to error in config file.");
}
END_TEST

/*
 * Test how the server processes the configuration file.
 *
 * Config file (server start) tests:
 * 	start server with two simple table (pass)
 */

START_TEST (test_config_simpletables)
{
	int serverpid = start_server(SIMPLETABLES_CONF, NULL, "test_config_simpletables.serverout");
	fail_unless(serverpid > 0, "Server didn't run properly.");
}
END_TEST


/*
 * Connection tests:
 * 	connect to and disconnect from server (pass)
 * 	connect to server without server running (fail)
 * 	connect to server with invalid hostname (fail)
 * 	disconnect from server with invalid params (fail)
 */

START_TEST (test_conn_basic)
{
	int serverpid = start_server(SIMPLETABLES_CONF, NULL, "test_conn_basic.serverout");
	fail_unless(serverpid > 0, "Server didn't run properly.");

	void *conn = storage_connect(SERVERHOST, server_port);
	fail_unless(conn != NULL, "Couldn't connect to server.");

	int status = storage_disconnect(conn);
	fail_unless(status == 0, "Error disconnecting from the server.");
}
END_TEST


START_TEST (test_conninvalid_connectinvalidparam)
{
	void *conn = storage_connect(NULL, server_port);
	fail_unless(conn == NULL, "storage_connect with invalid param should fail.");
	fail_unless(errno == ERR_INVALID_PARAM, "storage_connect with invalid param not setting errno properly.");
}
END_TEST


/**
 * @brief This runs the marking tests for Assignment 3.
 */
int main(int argc, char *argv[])
{
	if(argc == 2)
		server_port = atoi(argv[1]);
	else
		server_port = SERVERPORT;
	printf("Using server port: %d.\n", server_port);
	Suite *s = suite_create("config");
	TCase *tc;

	// Sanity tests
	tc = tcase_create("sanity");
	tcase_set_timeout(tc, TESTTIMEOUT);
	tcase_add_test(tc, test_sanity_filemod);
	suite_add_tcase(s, tc);

	// Config file test (no comma)
	tc = tcase_create("confignocomma");
	tcase_set_timeout(tc, TESTTIMEOUT);
	tcase_add_test(tc, test_configerror_nocomma);
	suite_add_tcase(s, tc);

	// Config file test (neg size)
	tc = tcase_create("confignegsize");
	tcase_set_timeout(tc, TESTTIMEOUT);
	tcase_add_test(tc, test_configerror_negsize);
	suite_add_tcase(s, tc);
    
    // Config file test (duplicate tables)
	tc = tcase_create("configduplicate");
	tcase_set_timeout(tc, TESTTIMEOUT);
	tcase_add_test(tc, test_configerror_duplicate);
	suite_add_tcase(s, tc);
    
    // Config file test (bad type)
    tc = tcase_create("configbadtype");
	tcase_set_timeout(tc, TESTTIMEOUT);
	tcase_add_test(tc, test_configerror_badtype);
	suite_add_tcase(s, tc);
    
    // Config file test (simple tables)
	tc = tcase_create("configsimple");
	tcase_set_timeout(tc, TESTTIMEOUT);
	tcase_add_test(tc, test_config_simpletables);
	suite_add_tcase(s, tc);
    
	// Connection tests
	tc = tcase_create("conn");
	tcase_set_timeout(tc, TESTTIMEOUT);
	tcase_add_test(tc, test_conn_basic);
	suite_add_tcase(s, tc);

	// Connection tests with invalid parameters
	tc = tcase_create("conninvalid");
	tcase_set_timeout(tc, TESTTIMEOUT);
	tcase_add_test(tc, test_conninvalid_connectinvalidparam);
	suite_add_tcase(s, tc);

	SRunner *sr = srunner_create(s);
	srunner_set_log(sr, "results.log");
	srunner_run_all(sr, CK_ENV);
	srunner_ntests_failed(sr);
	srunner_free(sr);

	return EXIT_SUCCESS;
}

