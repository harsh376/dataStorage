%{

#include "utils.h"

%}

%%

"server_host"		return KEY_SERVERHOST;
"server_port"		return KEY_SERVERPORT;
"username"			return KEY_USERNAME;
"password"			return KEY_PASSWORD;
"table"				return TABLE;
"char"				return CHARACTER;
"int"				return INTEGER;
"concurrency"		return CONCURRENCY;
:					return COLON;
,					return COMMA;
[[]					return OPEN_BRACK;
[]]					return CLOSE_BRACK;
[a-zA-Z][a-zA-Z]*	return NAME;
[-]					return NEGATIVE;
[0-9][0-9]*			return NUMBER;
[./0-9A-Za-z][./0-9A-Za-z]*	return PASSWORD;

[ \t\n]				;
.					printf("Unexpected character\n");


%%

int yywrap(void)
{
	return 1;
}
