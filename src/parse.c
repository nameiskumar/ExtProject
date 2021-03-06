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

#define MAX_VAR_COUNT_SIZE 16

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

char* next_dot_token(char** tokenizer, message_status* status)
{
    char* token = strsep(tokenizer, ".");
    if (token == NULL)
    {
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
    //printf("Entering  parse_create_col fn \n");
    message_status status = OK_DONE;
    char** create_arguments_index = &create_arguments;
    char* column_name = next_token(create_arguments_index, &status);
    char* db_name = strsep(create_arguments_index,".");

    column_name = trim_quotes(column_name);

//Debug line
//printf("create args is *********%s\n", create_arguments);
//printf("db name is *******%s\n",db_name);
//printf("column name is ***********%s\n", column_name);

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
//printf("create args after len  is *********%s\n", create_arguments);
//printf("len of agrs-1 is ********%d\n",last_char);
//printf("the last char of create args is******%c\n", create_arguments[last_char]);

    if (create_arguments[last_char] != ')')
    {
        return INCORRECT_FORMAT;
    }
    create_arguments[last_char] = '\0';
    char* table_name = create_arguments;

//Debug line
//printf("create args after last char made null is**** %s\n",create_arguments);
//printf("table name after last char made null is**** %s\n",table_name);
//printf("db name is *******%s\n",db_name);
//printf("column name is ***********%s\n", column_name);
//printf("the value of crrent_db and db_name are %s and %s********",current_db->name,db_name);
    if (strcmp(current_db->name, db_name) != 0)
    {
        cs165_log(stdout, "query unsupported. Bad db name");
        return QUERY_UNSUPPORTED;
    }

    Status create_status;
//Debug
//printf("table  name before calling create column fn is ***********%s\n", table_name);
    Column* c_ptr = create_column(column_name, table_name, false, &create_status);

     if (create_status.code != OK) 
     {
        cs165_log(stdout, "adding a table failed.");
        return EXECUTION_ERROR;
     }

    return status;
}

/**
 * This method takes in a string representing the arguments to create a table.
 * It parses those arguments, checks that they are valid, and creates a table.
 **/

message_status parse_create_tbl(char* create_arguments) {
//Debug line
//printf("Entering  parse_create_tbl fn \n");
    message_status status = OK_DONE;
    char** create_arguments_index = &create_arguments;
    char* table_name = next_token(create_arguments_index, &status);
    char* db_name = next_token(create_arguments_index, &status);
    char* col_cnt = next_token(create_arguments_index, &status);

//Debug line
//printf("message status is %d \n", status);
//printf("table name inside parse create tbl fn is is %s \n", table_name);
//printf("db name inside parse create tbl fn is is %s \n", db_name);
//printf("column cnt inside parse create tbl fn is is %s \n", col_cnt);

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
//printf("Current Database name is %s \n", current_db->name);
    if (strcmp(current_db->name, db_name) != 0) {
        cs165_log(stdout, "query unsupported. Bad db name");
        return QUERY_UNSUPPORTED;
    }


    int column_cnt = atoi(col_cnt);
//Debug line
//printf("Column Cnt is %d \n", column_cnt);
    if (column_cnt < 1) {
        return INCORRECT_FORMAT;
    }
    Status create_status;
//Debug line
//printf("Going to call create table now");
    create_table(current_db, table_name, column_cnt, &create_status);
//Debug line
//printf("message status code after callinf create table is  %d \n", create_status.code);
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
        db_name = trim_quotes(db_name);
        int last_char = strlen(db_name) - 1;
        if (last_char < 0 || db_name[last_char] != ')') {
            return INCORRECT_FORMAT;
        }
        db_name[last_char] = '\0';
        token = strsep(&create_arguments, ",");
        if (token != NULL) {
            return INCORRECT_FORMAT;
        }
        if (add_db(db_name, true).code == OK) {
            return OK_DONE;
        } else {
            return EXECUTION_ERROR;
        }
    }
}

message_status parse_create_index(char* create_arguments)
{
    char *token;
    token = strsep(&create_arguments, ",");
    if (token == NULL)
    {
        return INCORRECT_FORMAT;
    }
    else
    {
        char* db_object = token;
        char* db_name = strsep(&db_object, ".");
        char* tbl_name = strsep(&db_object, ".");
        char* col_name = db_object;

        Table* tbl_ptr = lookup(tbl_name);
        Column* col_ptr = tbl_ptr->columns;
        while((strcmp(col_ptr->name, col_name) != 0))
            col_ptr++;

        tbl_ptr->index_priority = (Column** )malloc(sizeof(Column*) * tbl_ptr->col_count);
        col_ptr->index = (ColumnIndex* )malloc(sizeof(ColumnIndex));

        char* index_param1 = strsep(&create_arguments, ",");
        char* index_param2 = create_arguments;
        int last_char = strlen(index_param2) - 1;
        index_param2[last_char] = '\0';

        if (strcmp(index_param1, "btree") == 0 && strcmp(index_param2, "clustered") == 0)
            col_ptr->index->type = BTREE_CLUSTERED;
        else if (strcmp(index_param1, "btree") == 0 && strcmp(index_param2, "unclustered") == 0)
            col_ptr->index->type = BTREE_UNCLUSTERED;
        else if (strcmp(index_param1, "sorted") == 0 && strcmp(index_param2, "clustered") == 0)
            col_ptr->index->type = SORTED_CLUSTERED;
        else if (strcmp(index_param1, "sorted") == 0 && strcmp(index_param2, "unclustered") == 0)
            col_ptr->index->type = SORTED_UNCLUSTERED;

        tbl_ptr->index_priority[tbl_ptr->index_count] = col_ptr;
        tbl_ptr->index_count++;
    }
    return OK_DONE;
}

/**
 * parse_create parses a create statement and then passes the necessary arguments off to the next function
 **/
message_status parse_create(char* create_arguments) {
//Debug Line
//printf("Entering parse_create fn \n");

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
                mes_status = parse_create_db(tokenizer_copy);
            } else if (strcmp(token, "tbl") == 0) {
                mes_status = parse_create_tbl(tokenizer_copy);
            }
             else if (strcmp(token, "col") == 0) {
                mes_status = parse_create_col(tokenizer_copy);
            }
             else if (strcmp(token, "idx") == 0) {
                mes_status = parse_create_index(tokenizer_copy);
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
//printf("%s\n",table_name);

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
        //Column* col = insert_table->columns;
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

    //printf("Last char is %c \n", query_command[last_char]);

    if (query_command[last_char] != ')')
    {
        printf(")INCORRECT_FORMAT");
    }

    query_command[last_char] = '\0';
//char* load_file_copy = strsep(table_name_index, "/");
//Debug 
//printf("%s\n", query_command);

    query_command = trim_quotes(query_command);
//debug
//printf("%s\n", query_command);

    DbOperator* dbo = malloc(sizeof(DbOperator));
    dbo->type = LOAD;
    //load_File(query_command);
    //Will code later

    return dbo;
}


/**
 * parse_print takes as input the send_message from the client and then
 * parses it into the appropriate query. Stores into send_message the
 * status to send back.
 * Returns db operator
**/

DbOperator* parse_print(char* query_command, char* handle, int client_socket, ClientContext* context, message* send_message)
{
    if(strncmp(query_command, "(", 1) == 0)
    {
        query_command++;
    }

    query_command = trim_newline(query_command);
    query_command = trim_whitespace(query_command);

    int last_char = strlen(query_command) - 1;

    if (query_command[last_char] != ')')
    {
        printf("INCORRECT_FORMAT");
        send_message->status = UNKNOWN_COMMAND;
        return NULL;
    }

    query_command[last_char] = '\0';

    DbOperator* dbo = malloc(sizeof(DbOperator));
    dbo->type = PRINT;
    dbo->operator_fields.print_operator.var_count = 0;
    VariablePool* var_pool_ptr = malloc(sizeof(VariablePool) * MAX_VAR_COUNT_SIZE);
    dbo->operator_fields.print_operator.var_pool = var_pool_ptr;

    char** query_command_index = &query_command;
    //message_status status = OK_DONE;

    char* var_name = next_token(query_command_index, &(send_message->status));
    while(var_name != NULL)
    {
        strcpy(var_pool_ptr->name, var_name);
        dbo->operator_fields.print_operator.var_count++;
        var_pool_ptr++;

        var_name = next_token(query_command_index, &(send_message->status));
    }

//    DbOperator* dbo = malloc(sizeof(DbOperator));
    send_message->status = OK_WAIT_FOR_RESPONSE;
    dbo->context = context;

return dbo;
}

DbOperator* parse_addsub(char* query_command, char* handle, int client_socket, ClientContext* context, message* send_message)
{
    send_message->status = OK_DONE;
    DbOperator* dbo = malloc(sizeof(DbOperator));

    if(strncmp(query_command, "add(", 4) == 0)
    {
        dbo->operator_fields.addsub_operator.type = ADD;
    }
    else if(strncmp(query_command, "sub(", 4) == 0)
    {
        dbo->operator_fields.addsub_operator.type = SUB;
    }
    query_command += 4;
    query_command = trim_newline(query_command);
    query_command = trim_whitespace(query_command);

    int last_char = strlen(query_command) - 1;
    if (query_command[last_char] != ')')
    {
        printf("INCORRECT_FORMAT");
        send_message->status = UNKNOWN_COMMAND;
        return NULL;
    }
    query_command[last_char] = '\0';
    char** command_index = &query_command;

    char* math_operand1 = next_token(command_index, &send_message->status);
    char* math_operand2 = next_token(command_index, &send_message->status);
    strcpy(dbo->operator_fields.addsub_operator.name, handle);

    ClientContext* client_context = context;
    GeneralizedColumnHandle* chandle_table_ptr = client_context->chandle_table;
    for(int i = 0; i < client_context->chandles_in_use; i++)
    {
        if(strcmp(math_operand1, chandle_table_ptr->name) == 0)
        {
            dbo->operator_fields.addsub_operator.operand1 = chandle_table_ptr->generalized_column.column_pointer.result;
            dbo->operator_fields.addsub_operator.num_tuples = (chandle_table_ptr->generalized_column.column_pointer.result)->num_tuples;
        }
        if(strcmp(math_operand2, chandle_table_ptr->name) == 0)
        {
            dbo->operator_fields.addsub_operator.operand2 = chandle_table_ptr->generalized_column.column_pointer.result;
        }
        chandle_table_ptr++;
    }
    strcpy((chandle_table_ptr)->name, handle);
    chandle_table_ptr->generalized_column.column_type = RESULT;

    client_context->chandles_in_use++;
    dbo->context = client_context;
    dbo->type = ADDSUB;
return dbo;
}


/**
 * parse_math takes as input the send_message from the client and then
 * parses it into the appropriate query. Stores into send_message the
 * status to send back.
 * Returns db operator
 **/

DbOperator* parse_math(char* query_command, char* handle, int client_socket, ClientContext* context, message* send_message)
{
    send_message->status = OK_DONE;
    DbOperator* dbo = malloc(sizeof(DbOperator));

    if(strncmp(query_command, "min(", 4) == 0)
    {
        dbo->operator_fields.math_operator.type = MIN;
    }
    else if(strncmp(query_command, "max(", 4) == 0)
    {
        dbo->operator_fields.math_operator.type = MAX;
    }
    else if(strncmp(query_command, "avg(", 4) == 0)
    {
        dbo->operator_fields.math_operator.type = AVG;
    }
    else if(strncmp(query_command, "sum(", 4) == 0)
    {
        dbo->operator_fields.math_operator.type = SUM;
    }

    query_command += 4;
    query_command = trim_newline(query_command);
    query_command = trim_whitespace(query_command);

    int last_char = strlen(query_command) - 1;
    if (query_command[last_char] != ')')
    {
        printf("INCORRECT_FORMAT");
        send_message->status = UNKNOWN_COMMAND;
        return NULL;
    }
    query_command[last_char] = '\0';
    char** command_index = &query_command;
    char* math_operand = next_dot_token(command_index, &send_message->status);
    strcpy(dbo->operator_fields.math_operator.name, handle);

    ClientContext* client_context = context;
    GeneralizedColumnHandle* chandle_table_ptr = client_context->chandle_table;
    if(*command_index != NULL)
    {
        GeneralizedColumnHandle* chandle_table_last = &chandle_table_ptr[client_context->chandles_in_use];
        Table* tbl_ptr = lookup(next_dot_token(command_index, &send_message->status));
        char* col_name = next_dot_token(command_index, &send_message->status);
        Column* col_ptr = tbl_ptr->columns;
        while((strcmp(col_ptr->name, col_name) != 0))
            col_ptr++;

        dbo->operator_fields.math_operator.num_tuples = tbl_ptr->data_pos;
        dbo->operator_fields.math_operator.col_operand = col_ptr;
        chandle_table_last->generalized_column.column_type = COLUMN;
        strcpy(chandle_table_last->name, handle);
    }
    else
    {
        int i = 0;
        while(i != client_context->chandles_in_use)
        {
            if(strcmp(math_operand, chandle_table_ptr->name) == 0)
            {
                dbo->operator_fields.math_operator.res_operand = chandle_table_ptr->generalized_column.column_pointer.result;
            }
            chandle_table_ptr++;
            i++;
        }
        strcpy((chandle_table_ptr)->name, handle);
        chandle_table_ptr->generalized_column.column_type = RESULT;
    }
        client_context->chandles_in_use++;
        dbo->context = client_context;
        dbo->type = MATH;
return dbo;
}

/**
 * parse_fetch takes as input the send_message from the client and then
 * parses it into the appropriate query. Stores into send_message the
 * status to send back.
 * Returns db operator
 **/

DbOperator* parse_fetch(char* query_command, char* handle, int client_socket, ClientContext* context, message* send_message)
{
    send_message->status = OK_DONE;

    if(strncmp(query_command, "(", 1) == 0)
    {
        query_command++;
    }

    query_command = trim_newline(query_command);
    query_command = trim_whitespace(query_command);

    int last_char = strlen(query_command) - 1;

    if (query_command[last_char] != ')')
    {
        printf("INCORRECT_FORMAT");
        send_message->status = UNKNOWN_COMMAND;
        return NULL;
    }
    query_command[last_char] = '\0';

    char** command_index = &query_command;
    char* db_object = next_token(command_index, &send_message->status);

    char* select_var = query_command;

    command_index = &db_object;
    char* db_name = strsep(command_index, ".");

    command_index = &db_object;
    char* tbl_name = strsep(command_index, ".");
    char* col_name = db_object;

    Table* tbl_ptr = lookup(tbl_name);
    Column* col_ptr = tbl_ptr->columns;
    while((strcmp(col_ptr->name, col_name) != 0))
        col_ptr++;
    //Column* col_ptr = col_lookup(col_name);

    DbOperator* dbo = malloc(sizeof(DbOperator));
    dbo->type = FETCH;

    dbo->operator_fields.fetch_operator.table = tbl_ptr;
    dbo->operator_fields.fetch_operator.column = col_ptr;

    //setting the context here for fetch  query
    ClientContext* client_context = context;
    //client_context->chandle_table = (GeneralizedColumnHandle* )malloc(sizeof(GeneralizedColumnHandle));
    GeneralizedColumnHandle* chandle_table_ptr = client_context->chandle_table;

    int i = 0;
    while(i != client_context->chandles_in_use)
    {
        chandle_table_ptr++;
        i++;
    }

    strcpy((chandle_table_ptr)->name, handle);
    client_context->chandles_in_use++;

    strcpy(dbo->operator_fields.fetch_operator.select_handle_name, select_var);

    dbo->context = client_context;

return dbo;

}


DbOperator* parse_join(char* query_command, char* handle, int client_socket, ClientContext* context, message* send_message)
{
    send_message->status = OK_DONE;

    if(strncmp(query_command, "(", 1) == 0)
    {
        query_command++;
    }

    query_command = trim_newline(query_command);
    query_command = trim_whitespace(query_command);
    handle = trim_whitespace(handle);
    char** handle_index = &handle;
    char* handle_left = next_token(handle_index, &send_message->status);
    char* handle_right = handle;

    int last_char = strlen(query_command) - 1;

    if (query_command[last_char] != ')')
    {
        printf("INCORRECT_FORMAT");
        send_message->status = UNKNOWN_COMMAND;
        return NULL;
    }
    query_command[last_char] = '\0';

    char** command_index = &query_command;
    char* fetch_obj1 = next_token(command_index, &send_message->status);
    char* select_obj1 = next_token(command_index, &send_message->status);
    char* fetch_obj2 = next_token(command_index, &send_message->status);
    char* select_obj2 = next_token(command_index, &send_message->status);
    char* join_type = query_command;

    DbOperator* dbo = malloc(sizeof(DbOperator));
    dbo->type = JOIN;

    strcpy(dbo->operator_fields.join_operator.name_left, handle_left);
    strcpy(dbo->operator_fields.join_operator.name_right, handle_right);
    strcpy(dbo->operator_fields.join_operator.select_handle1, select_obj1);
    strcpy(dbo->operator_fields.join_operator.select_handle2, select_obj2);
    strcpy(dbo->operator_fields.join_operator.fetch_handle1, fetch_obj1);
    strcpy(dbo->operator_fields.join_operator.fetch_handle2, fetch_obj2);

    if(strcmp(join_type, "hash") == 0)
        dbo->operator_fields.join_operator.type = HASH;
    else
        dbo->operator_fields.join_operator.type = NESTED_LOOP;

    dbo->operator_fields.join_operator.num_tuples = 0;

    ClientContext* client_context = context;
    GeneralizedColumnHandle* chandle_table_ptr = client_context->chandle_table;

    int i = 0;
    while(i != client_context->chandles_in_use)
    {
        chandle_table_ptr++;
        i++;
    }

    strcpy((chandle_table_ptr)->name, handle_left);
    chandle_table_ptr++;
    strcpy((chandle_table_ptr)->name, handle_right);
    client_context->chandles_in_use += 2;

    dbo->context = client_context;

return dbo;
}

DbOperator* parse_select2(char* query_command, char* handle, int client_socket, ClientContext* context, message* send_message)
{
    send_message->status = OK_DONE;
    if(strncmp(query_command, "(", 1) == 0)
    {
        query_command++;
    }
    query_command = trim_newline(query_command);
    query_command = trim_whitespace(query_command);
    int last_char = strlen(query_command) - 1;
    if (query_command[last_char] != ')')
    {
        printf("INCORRECT_FORMAT");
        send_message->status = UNKNOWN_COMMAND;
    }
    query_command[last_char] = '\0';
    char** command_index = &query_command;

    char* select_var = next_token(command_index, &send_message->status);
    char* fetch_var = next_token(command_index, &send_message->status);
    char* lower_bound = next_token(command_index, &send_message->status);
    char* upper_bound = query_command;

    Comparator* comp = (Comparator* )malloc(sizeof(Comparator));
    if(strcmp(lower_bound, "null") == 0)
    {
        comp->p_low = -2147483648;
        comp->p_high = atoi(upper_bound);
        comp->type1 = LESS_THAN;
    }
    else if(strcmp(upper_bound, "null") == 0)
    {
        comp->p_high = 2147483647;
        comp->p_low = atoi(lower_bound);
        comp->type1 = GREATER_THAN_OR_EQUAL;
    }
    else
    {
        comp->p_low = atoi(lower_bound);
        comp->p_high = atoi(upper_bound);
    }
    DbOperator* dbo = malloc(sizeof(DbOperator));
    dbo->type = SELECT2;
    dbo->operator_fields.select_operator.comparator = comp;

    comp->gen_col = (GeneralizedColumn* )malloc(sizeof(GeneralizedColumn));
    (comp->gen_col)->column_type = RESULT;
    ClientContext* client_context = context;
    GeneralizedColumnHandle* chandle_table_ptr = client_context->chandle_table;
    for(int i = 0; i < client_context->chandles_in_use; i++)
    {
        if(strcmp(fetch_var, chandle_table_ptr->name) == 0)
        {
            (comp->gen_col)->column_pointer.result = chandle_table_ptr->generalized_column.column_pointer.result;
        }
        chandle_table_ptr++;
    }
    strcpy((chandle_table_ptr)->name, handle);
    chandle_table_ptr->generalized_column.column_type = RESULT;

    client_context->chandles_in_use++;
    dbo->context = client_context;
return dbo;
}

/**
 * parse_select takes as input the send_message from the client and then
 * parses it into the appropriate query. Stores into send_message the
 * status to send back.
 * Returns a db_operator.
 **/

DbOperator* parse_select(char* query_command, char* handle, int client_socket, ClientContext* context, message* send_message)
{

    send_message->status = OK_DONE;

    if(strncmp(query_command, "(", 1) == 0)
    {
        query_command++;
    }

    query_command = trim_newline(query_command);
    query_command = trim_whitespace(query_command);

    int last_char = strlen(query_command) - 1;
    if (query_command[last_char] != ')')
    {
        printf("INCORRECT_FORMAT");
        send_message->status = UNKNOWN_COMMAND;
    }
    query_command[last_char] = '\0';

     char** command_index = &query_command;
     char* table_name = next_dot_token(command_index, &send_message->status);
     char* col_name = next_token(command_index, &send_message->status);
     char* lower_bound = next_token(command_index, &send_message->status);
     char* upper_bound = query_command;

    Comparator* comp = (Comparator* )malloc(sizeof(Comparator));
    if(strcmp(lower_bound, "null") == 0)
    {
        comp->p_low = -2147483648;
        comp->p_high = atoi(upper_bound);
        comp->type1 = LESS_THAN;
    }
    else if(strcmp(upper_bound, "null") == 0)
    {
        comp->p_high = 2147483647;
        comp->p_low = atoi(lower_bound);
        comp->type1 = GREATER_THAN_OR_EQUAL;
    }
    else
    {
        comp->p_low = atoi(lower_bound);
        comp->p_high = atoi(upper_bound);
    }
    Table* tbl_ptr = lookup(table_name);
    Column* col_ptr = tbl_ptr->columns;
    size_t j = 0;
    while((strcmp(col_ptr->name, col_name) != 0))
    {
        j++;
        if(j == tbl_ptr->col_count)
            break;
        col_ptr++;
    }
    DbOperator* dbo = malloc(sizeof(DbOperator));
    dbo->type = SELECT;

    dbo->operator_fields.select_operator.comparator = comp;
    dbo->operator_fields.select_operator.table = tbl_ptr;

    comp->gen_col = (GeneralizedColumn* )malloc(sizeof(GeneralizedColumn));
    (comp->gen_col)->column_type = COLUMN;
    (comp->gen_col)->column_pointer.column = col_ptr;
    comp->handle = (char* )malloc(sizeof(char));
    strcpy(comp->handle, handle);

    ClientContext* client_context = context;

    GeneralizedColumnHandle* chandle_table_ptr = client_context->chandle_table;
    int i = 0;
    while(i != client_context->chandles_in_use)
    {
        chandle_table_ptr++;
        i++;
    }

    strcpy((chandle_table_ptr)->name, handle);
    client_context->chandles_in_use++;

    dbo->context = client_context;

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
    send_message->status = OK_DONE;
    query_command = trim_whitespace(query_command);
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
        char** command_index = &query_command;
        char* db_name = next_dot_token(command_index, &send_message->status);
        if(*command_index == NULL)
        {
            query_command = db_name;
            dbo = parse_select2(db_name, handle, client_socket, context, send_message);
        }
        else
        {
            dbo = parse_select(query_command, handle, client_socket, context, send_message);
        }
    }
    else if(strncmp(query_command, "fetch", 5) == 0)
    {
        query_command += 5;
        dbo = parse_fetch(query_command, handle, client_socket, context, send_message);
    }
    else if((strncmp(query_command, "min(", 4) == 0) || (strncmp(query_command, "max(", 4) == 0) || (strncmp(query_command, "avg(", 4) == 0) || (strncmp(query_command, "sum(", 4) == 0))
    {
        dbo = parse_math(query_command, handle, client_socket, context, send_message);
    }
    else if((strncmp(query_command, "add(", 4) == 0) || (strncmp(query_command, "sub(", 4) == 0))
    {
        dbo = parse_addsub(query_command, handle, client_socket, context, send_message);
    }
    else
    {
        handle = NULL;
    }

    cs165_log(stdout, "QUERY: %s\n", query_command);

    send_message->status = OK_DONE;

    if (strncmp(query_command, "create", 6) == 0)
    {
        query_command += 6;
        send_message->status = parse_create(query_command);
        dbo = malloc(sizeof(DbOperator));
        dbo->type = CREATE;
    }
    else if (strncmp(query_command, "relational_insert", 17) == 0)
    {
        query_command += 17;
        dbo = parse_insert(query_command, send_message);
    }
    else if(strncmp(query_command, "shutdown", 8) == 0)
    {
        dbo = malloc(sizeof(DbOperator));
        dbo->type = SHUTDOWN;
    }
    else if(strncmp(query_command, "print", 5) == 0)
    {
        query_command += 5;
        dbo = parse_print(query_command, handle, client_socket, context, send_message);
        send_message->status = OK_WAIT_FOR_RESPONSE;
    }
    if (dbo == NULL)
    {
        return dbo;
    }
    dbo->client_fd = client_socket;
    return dbo;
}
