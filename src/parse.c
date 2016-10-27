#define _BSD_SOURCE
#include <string.h>
#include <stdbool.h>
#include <stddef.h>
#include <stdlib.h>
#include <ctype.h>
#include "cs165_api.h"
#include "parse.h"
#include "utils.h"
#include "client_context.h"
#include "message.h"

/**
 * Takes a pointer to a string.
 * This method returns the original string truncated to where its first comma lies.
 * In addition, the original string now points to the first character after that comma.
 * This method destroys its input.
 **/

char* next_token(char** tokenizer, message_status* status) {
    char* token = strsep(tokenizer, ",");
    if (token == NULL) {
        *status= INCORRECT_FORMAT;
    }
    return token;
}

/**
 * This method takes in a string representing the arguments to create a column.
 * It parses those arguments, checks that they are valid, and creates the coulmns.
 **/

message_status parse_create_col(char* create_arguments) 
{
    printf("Entering  parse_create_col fn \n");
    message_status status = OK_DONE;
    char** create_arguments_index = &create_arguments;
    char* column_name = next_token(create_arguments_index, &status);
    char* db_name = strsep(create_arguments_index,".");

    column_name = trim_quotes(column_name);

//Debug line
printf("create args is *********%s\n", create_arguments);
printf("db name is *******%s\n",db_name);
printf("column name is ***********%s\n", column_name);

    if (strcmp(current_db->name, db_name) != 0) 
    {
        cs165_log(stdout, "query unsupported. Bad db name");
        return QUERY_UNSUPPORTED;
    }

    if (db_name == NULL)
    {
        return INCORRECT_FORMAT;
    }
    
    int last_char = strlen(create_arguments) - 1;

//Debug line
printf("create args after len  is *********%s\n", create_arguments);
printf("len of agrs-1 is ********%d\n",last_char);
printf("the last char of create args is******%c\n", create_arguments[last_char]);

    if (create_arguments[last_char] != ')') 
    {
        return INCORRECT_FORMAT;
    }
    
    create_arguments[last_char] = '\0';
    char* table_name = create_arguments;

//Debug line
printf("create args after last char made null is**** %s\n",create_arguments);
printf("table name after last char made null is**** %s\n",table_name);
printf("db name is *******%s\n",db_name);
printf("column name is ***********%s\n", column_name);
printf("the value of crrent_db and db_name are %s and %s********",current_db->name,db_name);
    if (strcmp(current_db->name, db_name) != 0) 
    {
        cs165_log(stdout, "query unsupported. Bad db name");
        return QUERY_UNSUPPORTED;
    }

    Status create_status;
//Debug
printf("table  name before calling create column fn is ***********%s\n", table_name);
    Column* c_ptr = create_column(column_name, table_name, false, &create_status);

     if (create_status.code != OK) 
     {
        cs165_log(stdout, "adding a table failed.");
        return EXECUTION_ERROR;
     }

//Debug line
//
Table* tbl_ptr;
Column* col_ptr;
tbl_ptr = current_db->tables;
col_ptr = tbl_ptr->columns;

if(tbl_ptr->columns_size == tbl_ptr->col_count)
{
printf("comparing c_ptr with col_ptr\n");
printf("c_ptr is %u\n",c_ptr);
printf("col_ptr is %u\n",col_ptr);

printf("Here start the gist of the db created\n");
printf("The DB is %s\n",current_db->name);
printf("The Db->table_size is %d\n",current_db->tables_size);
printf("The Db->table_capacity is %d\n",current_db->tables_capacity);
printf("*************************\n");
//Table* tbl_ptr;
//Column* col_ptr;

//tbl_ptr = current_db->tables;

for(int i = 0; i < current_db->tables_size; i++)
{
    printf("tables are %s\n",tbl_ptr->name);
    printf("table->col_count is %d\n",tbl_ptr->col_count);
    printf("table->table_length is %d\n",tbl_ptr->table_length);
    printf("table->columns_size is %d\n",tbl_ptr->columns_size);
    printf("*************************\n");
    col_ptr = tbl_ptr->columns;

    for(int j=0; j < tbl_ptr->columns_size; j++)
    {
        printf("Columns are %s\n",col_ptr->name);
        col_ptr++;
    }
    printf("*************************\n");
    tbl_ptr++;
}
}
    return status;
}

/**
 * This method takes in a string representing the arguments to create a table.
 * It parses those arguments, checks that they are valid, and creates a table.
 **/

message_status parse_create_tbl(char* create_arguments) {
//Debug line
printf("Entering  parse_create_tbl fn \n");
    message_status status = OK_DONE;
    char** create_arguments_index = &create_arguments;
    char* table_name = next_token(create_arguments_index, &status);
    char* db_name = next_token(create_arguments_index, &status);
    char* col_cnt = next_token(create_arguments_index, &status);

//Debug line
printf("message status is %d \n", status);
printf("table name inside parse create tbl fn is is %s \n", table_name);
printf("db name inside parse create tbl fn is is %s \n", db_name);
printf("column cnt inside parse create tbl fn is is %s \n", col_cnt);

    table_name = trim_quotes(table_name);
    // not enough arguments
    if (status == INCORRECT_FORMAT) {
        return status;
    }

    // read and chop off last char
    int last_char = strlen(col_cnt) - 1;
    if (col_cnt[last_char] != ')') {
        return INCORRECT_FORMAT;
    }
    col_cnt[last_char] = '\0';

    // check that the database argument is the current active database
//Debug line
printf("Current Database name is %s \n", current_db->name);
    
    if (strcmp(current_db->name, db_name) != 0) {
        cs165_log(stdout, "query unsupported. Bad db name");
        return QUERY_UNSUPPORTED;
    }


    int column_cnt = atoi(col_cnt);
//Debug line
printf("Column Cnt is %d \n", column_cnt);
    if (column_cnt < 1) {
        return INCORRECT_FORMAT;
    }
    Status create_status;
//Debug line
printf("Going to call create table now");
    create_table(current_db, table_name, column_cnt, &create_status);
//Debug line
printf("message status code after callinf create table is  %d \n", create_status.code);
    if (create_status.code != OK) {
        cs165_log(stdout, "adding a table failed.");
        return EXECUTION_ERROR;
    }

    return status;
}

/**
 * This method takes in a string representing the arguments to create a database.
 * It parses those arguments, checks that they are valid, and creates a database.
 **/

message_status parse_create_db(char* create_arguments) {
    char *token;
    token = strsep(&create_arguments, ",");
    if (token == NULL) {
        return INCORRECT_FORMAT;                    
    } else {
        // create the database with given name
        char* db_name = token;
//Debug line
printf("db name before trimming is %s \n", db_name);
        db_name = trim_quotes(db_name);
//Debug line
printf("db name after trimming is %s \n", db_name);
        int last_char = strlen(db_name) - 1;
        if (last_char < 0 || db_name[last_char] != ')') {
            return INCORRECT_FORMAT;
        }
        db_name[last_char] = '\0';
        token = strsep(&create_arguments, ",");
        if (token != NULL) {
            return INCORRECT_FORMAT;
        }
//Debug line
printf("db name inside parse_create_db before calling add_db fn is %s \n", db_name);
        if (add_db(db_name, true).code == OK) {
            return OK_DONE;
        } else {
            return EXECUTION_ERROR;
        }
    }
}

/**
 * parse_create parses a create statement and then passes the necessary arguments off to the next function
 **/
message_status parse_create(char* create_arguments) {
//Debug Line
printf("Entering parse_create fn \n");

    message_status mes_status;
    char *tokenizer_copy, *to_free;
    tokenizer_copy = to_free = malloc((strlen(create_arguments)+1) * sizeof(char));
    char *token;
    strcpy(tokenizer_copy, create_arguments);
    if (strncmp(tokenizer_copy, "(", 1) == 0) {
        tokenizer_copy++;
        // token stores first argument. Tokenizer copy now points to just past first ,
        token = strsep(&tokenizer_copy, ",");
        if (token == NULL) {
            return INCORRECT_FORMAT;
        } else {
            if (strcmp(token, "db") == 0) {
//Debug line
printf("inside parse_create fn where token = db \n");
                mes_status = parse_create_db(tokenizer_copy);
            } else if (strcmp(token, "tbl") == 0) {
//Debug line
printf("inside parse_create fn where token = tbl \n");
printf("the value of tokenizer copy is %s",tokenizer_copy);
                mes_status = parse_create_tbl(tokenizer_copy);
            } 
             else if (strcmp(token, "col") == 0) {
                mes_status = parse_create_col(tokenizer_copy);
            }
             else {
                mes_status = UNKNOWN_COMMAND;
            }
        }
    } else {
        mes_status = UNKNOWN_COMMAND;
    }
    free(to_free);
    return mes_status;
}

/**
 * parse_insert reads in the arguments for a create statement and 
 * then passes these arguments to a database function to insert a row.
 **/

DbOperator* parse_insert(char* query_command, message* send_message) 
{
    unsigned int columns_inserted = 0;
    char* token = NULL;
    if (strncmp(query_command, "(", 1) == 0) 
    {
        query_command++;
        char** command_index = &query_command;
        char* table_name = next_token(command_index, &send_message->status);
        char** table_name_index = &table_name;
        char* db_name  = strsep(table_name_index, ".");

        table_name = trim_quotes(table_name);
//Debug line
printf("%s\n",table_name);

        //char* db_name = strsep(create_arguments_index,".");

        if (send_message->status == INCORRECT_FORMAT) 
        {
            return NULL;
        }
        // lookup the table and make sure it exists. 
        Table* insert_table = lookup(table_name);
        if (insert_table == NULL) 
        {
            send_message->status = OBJECT_NOT_FOUND;
            return NULL;
        }
        Column* col = insert_table->columns;
        DbOperator* dbo = malloc(sizeof(DbOperator));
        dbo->type = INSERT;
        dbo->operator_fields.insert_operator.table = insert_table;
        dbo->operator_fields.insert_operator.values = malloc(sizeof(int) * insert_table->col_count);
        
        while ((token = strsep(command_index, ",")) != NULL) 
        {
            // NOT ERROR CHECKED. COULD WRITE YOUR OWN ATOI. (ATOI RETURNS 0 ON NON-INTEGER STRING)
            int insert_val = atoi(token);
            dbo->operator_fields.insert_operator.values[columns_inserted] = insert_val;
            columns_inserted++;
        }
        // check that we received the correct number of input values
        if (columns_inserted != insert_table->col_count) 
        {
            send_message->status = INCORRECT_FORMAT;
            free (dbo);
            return NULL;
        } 
        return dbo;
    } 
    else 
    {
        send_message->status = UNKNOWN_COMMAND;
        return NULL;
    }
}
/**
 * parse_load takes as input the send_message from the client and then
 * parses it into the appropriate query. Stores into send_message the
 * status to send back.
 * Returns a db_operator.
 **/
DbOperator* parse_load(char* query_command, message* send_message)
{
    if(strncmp(query_command, "(", 1) == 0)
    {   
        query_command++;
    }
    
    query_command = trim_newline(query_command);
    query_command = trim_whitespace(query_command);

    int last_char = strlen(query_command) - 1;
   
    printf("Last char is %c \n", query_command[last_char]);

    if (query_command[last_char] != ')')
    {
        printf(")INCORRECT_FORMAT");
    }
    
    query_command[last_char] = '\0';
//char* load_file_copy = strsep(table_name_index, "/");
//Debug 
printf("%s\n", query_command);

    query_command = trim_quotes(query_command);
//debug
printf("%s\n", query_command);
    
    
    DbOperator* dbo = malloc(sizeof(DbOperator));
    dbo->type = LOAD;
    
    //load_File(query_command);
    //Will code later

    return dbo;
}


/**
 * parse_select takes as input the send_message from the client and then
 * parses it into the appropriate query. Stores into send_message the
 * status to send back.
 * Returns a db_operator.
 **/

DbOperator* parse_select(char* query_command, char* handle, int client_socket, message* send_message)
{

    if(strncmp(query_command, "(", 1) == 0)
    {
        query_command++;
    }

    query_command = trim_newline(query_command);
    query_command = trim_whitespace(query_command);

    int last_char = strlen(query_command) - 1;

//debug
printf("Last char is %c \n", query_command[last_char]);

    if (query_command[last_char] != ')')
    {
        printf("INCORRECT_FORMAT");
    }
    
    query_command[last_char] = '\0';
//char* load_file_copy = strsep(table_name_index, "/");
//Debug 
printf("%s\n", query_command);

    char** command_index = &query_command;
    char* db_object = next_token(command_index, &send_message->status);
    
    //Range extraction
    command_index = &query_command;
    char* lower_bound = (next_token(command_index, &send_message->status));
    char* upper_bound = (query_command);

//debug
printf("the lower bd is %s \n", lower_bound);
printf("the upper bd is %s \n", upper_bound);
printf("the db obj is %s \n", db_object);

    char** db_object_index = &db_object;
    char* db_name = strsep(db_object_index, ".");

    db_object_index = &db_object;
    
    char* table_name = strsep(db_object_index, ".");
    char* col_name = db_object;

//debug
printf("db name is %s \n", db_name);
printf("the table name is %s \n", table_name);
printf("the col_name is %s \n", col_name);


    DbOperator* dbo = malloc(sizeof(DbOperator));
    dbo->type = SELECT;
    dbo->context = malloc(sizeof(ClientContext));
    (dbo->context)->chandles_in_use = 0;
    (dbo->context)->chandle_slots = 10;

//debug
Column* col_ptr = col_lookup(col_name);

    Result* res = select_results(db_name, table_name, col_name, lower_bound, upper_bound);

return dbo;

}


/**
 * parse_command takes as input the send_message from the client and then
 * parses it into the appropriate query. Stores into send_message the
 * status to send back.
 * Returns a db_operator.
 **/

DbOperator* parse_command(char* query_command, message* send_message, int client_socket, ClientContext* context) 
{
    DbOperator *dbo = NULL; // = malloc(sizeof(DbOperator)); // calloc?

    if (strncmp(query_command, "--", 2) == 0) 
    {
        send_message->status = OK_DONE;
        // COMMENT LINE! 
        return NULL;
    }

    if (strncmp(query_command, "load", 4) == 0)
    {
        send_message->status = OK_DONE;
        query_command += 4;
        //printf("%s\n", query_command);
        dbo = parse_load(query_command, send_message);
    }

    char *equals_pointer = strchr(query_command, '=');
    char *handle = query_command;
  
    if (equals_pointer != NULL) 
    {
        // handle file table
        *equals_pointer = '\0';
        cs165_log(stdout, "FILE HANDLE: %s\n", handle);

        query_command = ++equals_pointer;
        //query_command = trim_newline(query_command);
    }
    if(strncmp(query_command, "select", 6) == 0)
    {
        query_command += 6;
        dbo = parse_select(query_command, handle, client_socket, send_message);
//debug
printf("the variable is %s \n", handle);
//printf("the query is  %s \n", query_command);
    } 
    
    else 
    {
        handle = NULL;
    }

    cs165_log(stdout, "QUERY: %s\n", query_command);

    send_message->status = OK_WAIT_FOR_RESPONSE;
    query_command = trim_whitespace(query_command);

    if (strncmp(query_command, "create", 6) == 0) 
    {
        query_command += 6;

//Debug line
printf("inside parse_command fn when create is issued \n");

        send_message->status = parse_create(query_command);
        dbo = malloc(sizeof(DbOperator));
        dbo->type = CREATE;
    } 
    
    else if (strncmp(query_command, "relational_insert", 17) == 0) 
    {
        query_command += 17;
        dbo = parse_insert(query_command, send_message);
    }
    
    if (dbo == NULL) 
    {
        return dbo;
    }
    
    dbo->client_fd = client_socket;
    dbo->context = context;
    return dbo;
}
