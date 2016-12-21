#include "cs165_api.h"
#include "utils.h"
#include "message.h"
#include <string.h>

#define _BSD_SOURCE
#define MAX_TABLE_CAP 10
#define DEFAULT_QUERY_BUFFER_SIZE 1024
#define MAX_STRING_SIZE 1024

//#define MAX_TABLE_LENGTH 2
// In this class, there will always be only one active database at a time
Db *current_db;
char* next_dot_token(char** tokenizer, message_status* status)
{
    char* token = strsep(tokenizer, ".");
    if (token == NULL)
    {
        *status= INCORRECT_FORMAT;
    }
    return token;
}

Table* lookup(const char* tbl_name)
{
//Debug
//printf("********in lookup fn\n");
//printf("the value of table in lookup fn is ***%s\n", tbl_name);

    Table* table_ptr = current_db->tables;
    size_t count = 0;
//Debug
//printf("inside lookup the value of table_ptr->name and tbl_name is %s%s****** \n", table_ptr->name,tbl_name);
    while(strcmp(table_ptr->name,tbl_name) != 0)
    {
        count++;

        if(count == current_db->tables_size)
            break;

        table_ptr++;
    }
    if (strcmp(table_ptr->name, tbl_name) != 0)
    {
        cs165_log(stdout, "Table name not found");
        return NULL;
    }
    return table_ptr;
}

Column* col_lookup(const char* col_name)
{
    Column* col_ptr = (current_db->tables)->columns;
    size_t count = 0;

    while(strcmp(col_ptr->name, col_name) != 0)
    {
        count++;

        if(count == (current_db->tables)->table_length)
            break;

        col_ptr++;
    }

    if(strcmp(col_ptr->name, col_name) !=0)
    {
        cs165_log(stdout, "Table name not found");
        return NULL;
    }

    return col_ptr;

}

//#define DEFAULT_QUERY_BUFFER_SIZE 1024

/** execute_DbOperator takes as input the DbOperator and executes the query.
 * This should be replaced in your implementation (and its implementation possibly moved to a different file).
 * It is currently here so that you can verify that your server and client can send messages.
 **/

char* execute_DbOperator(DbOperator* query)
{
    //printf("%d",query->client_fd);
//debug line
    if(query == NULL)
    {
        return "This is either a comment or an unsupported query";
    }

//printf("The value in the query is %d\n", query);
//printf("inside DBOperator-start\n");
//printf("The query type is %d\n", query->type);
//printf("inside DBOperator-end\n");

    Table* table_ptr;
    Column* col_ptr;
    message_status status;
    Comparator* comp;

    VariablePool* var_pool_ptr;
    char* str_res = malloc(sizeof(char) * MAX_STRING_SIZE);

    Result* res = (Result* )malloc(sizeof(Result));
    char* select_var = (char* )malloc(sizeof(char) * MAX_STRING_SIZE);

    GeneralizedColumnHandle* colhandle_table_ptr;
    GeneralizedColumnHandle gc_handle;

    Result* min_res;

    Status synch_status;

    switch(query->type)
    {
        case 0 : //CREATE Queries
            return "This is a Create query! Cant return results on this";
        case 1 : //INSERT QUERIES
//Debug
for (size_t i = 0; i < (current_db->tables)->col_count; i++)
//printf("The values to be inserted is %d\n", query->operator_fields.insert_operator.values[i]);
//printf("%d\n", query->operator_fields.insert_operator.values[1]);           
            col_ptr = (query->operator_fields.insert_operator.table)->columns;
            table_ptr = query->operator_fields.insert_operator.table;

            if(table_ptr->data_pos == 0)
            {
                for(size_t i = 0; i < table_ptr->col_count; i++)
                {
                    col_ptr->data = (int* )malloc(sizeof(int)*table_ptr->table_length);
                    col_ptr++;
                }
            }

            if(table_ptr->data_pos >= table_ptr->table_length)
            {
                table_ptr->table_length = table_ptr->table_length + MAX_TABLE_LENGTH;

                for (size_t i = 0; i < table_ptr->col_count; i++)
                {
                    status = TABLE_FULL;
                    void *temp = (int* )realloc((col_ptr->data),sizeof(int)*MAX_TABLE_LENGTH);
                    if(temp != NULL)
                    {
                        (col_ptr->data) = temp;
                    }
                    col_ptr++;
                }
            }

            col_ptr = table_ptr->columns;
            for(size_t j = 0; j < table_ptr->col_count; j++)
            {
                (col_ptr)->data[table_ptr->data_pos] = query->operator_fields.insert_operator.values[j];
                col_ptr++;

//Debug
//printf("data position value %d \n", (table_ptr->data_pos));
//printf("value inserted at this point is %d \n", (col_ptr)->data[table_ptr->data_pos]);
            }

            (table_ptr->data_pos)++;
Column *column_ptr = (current_db->tables)->columns;

            return "Data inserted successfully";

        case 4 : //SELECTS
            comp = query->operator_fields.select_operator.comparator;
            table_ptr = query->operator_fields.select_operator.table;

            //comp->gen_col = (GeneralizedColumn* )malloc(sizeof(GeneralizedColumn));
            GeneralizedColumnHandle* chandle_table_ptr = (query->context)->chandle_table;
            col_ptr = (comp->gen_col)->column_pointer.column;

            Result* res_ptr = (Result* )malloc(sizeof(Result));
            res_ptr->data_type = INT;
            res_ptr->num_tuples = 0;

            res_ptr->payload = (int* )malloc(sizeof(int) * table_ptr->table_length);

            for (size_t i = 0; i < (table_ptr->data_pos); i++)
            {
                if(col_ptr->data[i] < comp->p_high && col_ptr->data[i] >= comp->p_low)
                {
                    res_ptr->payload[res_ptr->num_tuples] = i;
                    res_ptr->num_tuples++;
                }
            }

            for(int i = 0; i <  ((query->context)->chandles_in_use - 1); i++)
            {
                chandle_table_ptr++;
            }

            chandle_table_ptr->generalized_column.column_pointer.result = res_ptr;

            //printf("This is select query and results will be returned \n");
            return "Get the results from Select Result Pointer";

        case 3 : //LOADS
            //printf("This is a load  query and results will NOT  be returned \n");
            //free(query);

        case 5 : //fetch
            col_ptr = query->operator_fields.fetch_operator.column;
            GeneralizedColumnHandle* chandle_table_ptr_fetch = (query->context)->chandle_table;
            strcpy(select_var, query->operator_fields.fetch_operator.select_handle_name);

            for(int i = 0; i < (query->context)->chandles_in_use; i++)
            {
                if(strcmp(select_var, chandle_table_ptr_fetch->name) == 0)
                {
                    res = chandle_table_ptr_fetch->generalized_column.column_pointer.result;
                    break;
                }

                else
                {
                    chandle_table_ptr_fetch++;
                }
            }

            Result* fetch_res = malloc(sizeof(Result));
            fetch_res->payload = (int *) malloc(sizeof(int) * res->num_tuples);
            fetch_res->num_tuples = res->num_tuples;

            for(size_t i = 0; i < res->num_tuples; i++)
            {
                fetch_res->payload[i] = col_ptr->data[res->payload[i]];
            }

            chandle_table_ptr_fetch = (query->context)->chandle_table;

            for(int i = 0; i <  ((query->context)->chandles_in_use - 1); i++)
            {
                chandle_table_ptr_fetch++;
            }

            chandle_table_ptr_fetch->generalized_column.column_pointer.result = fetch_res;
            chandle_table_ptr_fetch->generalized_column.column_pointer.result->data_type = INT;
            return "Get the results from Fetch Result Pointer";

        case 6 : //print
            var_pool_ptr = query->operator_fields.print_operator.var_pool;
            for(int i = 0; i < query->operator_fields.print_operator.var_count; i++)
            {
                char* print_var = malloc(sizeof(char)*(strlen(var_pool_ptr->name)+1));
                strcpy(print_var, var_pool_ptr->name);
                ClientContext* cc_ptr = query->context;

                for(int j = ((query->context)->chandles_in_use) - 1; j >= 0; j--)
                {
                    gc_handle = cc_ptr->chandle_table[j];
                    if(strcmp(print_var, gc_handle.name) == 0)
                    {
                        var_pool_ptr->data_type = (gc_handle.generalized_column.column_pointer.result)->data_type;
                        if(var_pool_ptr->data_type == 0)
                        {
                            var_pool_ptr->payload = (int* )calloc((gc_handle.generalized_column.column_pointer.result->num_tuples), sizeof(int));
                        }
                        else
                        {
                            var_pool_ptr->payload = (float* )calloc((gc_handle.generalized_column.column_pointer.result->num_tuples), sizeof(float));
                        }
                        var_pool_ptr->payload = (gc_handle.generalized_column.column_pointer.result)->payload;
                        var_pool_ptr->num_tuples = (gc_handle.generalized_column.column_pointer.result)->num_tuples;
                        break;
                    }
                }
                var_pool_ptr++;
            }

            //Output formatting
            GeneralizedColumnHandle* colhandle_table_ptr = (query->context)->chandle_table;
            Result* result_ptr = chandle_table_ptr->generalized_column.column_pointer.result;
            int count = query->operator_fields.print_operator.var_pool->num_tuples;

                for(int i = 0; i < count; i++)
                {
                    var_pool_ptr = query->operator_fields.print_operator.var_pool;
                    for(int j = 0; j < query->operator_fields.print_operator.var_count; j++)
                    {
                        if(var_pool_ptr->data_type == 0)
                        {
                            int* payload_ptr = var_pool_ptr->payload;
                            char* str_payload = (char* )malloc(sizeof(int) + 1);
                            sprintf(str_payload, "%d", payload_ptr[i]);
                            strcat(str_res, str_payload);
                        }
                        else
                        {
                            float* payload_ptr = var_pool_ptr->payload;
                            char* str_payload = (char* )malloc(sizeof(float) + 1);
                            sprintf(str_payload, "%f", payload_ptr[i]);
                            strcat(str_res, str_payload);
                        }

                        var_pool_ptr++;
                        if(j < query->operator_fields.print_operator.var_count - 1)
                        {
                            strcat(str_res, ",");
                            var_pool_ptr++;
                        }
                    }
                    if(i < count - 1)
                        strcat(str_res, "\n");

                    if(strlen(str_res) == MAX_STRING_SIZE)
                        str_res = realloc(str_res, MAX_STRING_SIZE*(i+1));
                }
            //str_res = "1\n2\n3";
            return str_res;

        case 7 :
            //printf("shutdown");
            synch_status = sync_db(current_db);
            return "shutdown";

        case 8 ://min operations
            min_res = malloc(sizeof(Result));
            min_res->num_tuples = 1;
            min_res->payload = (int *) malloc(sizeof(int) * min_res->num_tuples);
            GeneralizedColumnHandle* chandle_table_ptr_min = (query->context)->chandle_table;
            Result* operand_arr = query->operator_fields.math_operator.res_operand;
            int min = operand_arr->payload[0];
            int min_index = 0;
            for(int i = 0; i < operand_arr->num_tuples; i++)
            {
                int flag = operand_arr->payload[i] < min;
                min_index = i*flag + (1-flag)*min_index;
                min = operand_arr->payload[min_index];
            }
            min_res->payload[0] = min;
            for(int i = 0; i <  ((query->context)->chandles_in_use - 1); i++)
            {
                chandle_table_ptr_min++;
            }
            chandle_table_ptr_min->generalized_column.column_pointer.result = min_res;
            chandle_table_ptr_min->generalized_column.column_pointer.result->data_type = INT;

            return "Get the min value after issuing print command";

        case 9 ://max operations
            max_res = malloc(sizeof(Result));
            max_res->num_tuples = 1;
            max_res->payload = (int *) malloc(sizeof(int) * max_res->num_tuples);
            GeneralizedColumnHandle* chandle_table_ptr_max = (query->context)->chandle_table;
            Result* operand_arr = query->operator_fields.math_operator.res_operand;
            int max = operand_arr->payload[0];
            int max_index = 0;
            for(int i = 0; i < operand_arr->num_tuples; i++)
            {
                int flag = operand_arr->payload[i] > max;
                max_index = i*flag + (1-flag)*max_index;
                max = operand_arr->payload[max_index];
            }
            max_res->payload[0] = max;
            for(int i = 0; i <  ((query->context)->chandles_in_use - 1); i++)
            {
                chandle_table_ptr_max++;
            }
            chandle_table_ptr_max->generalized_column.column_pointer.result = max_res;
            chandle_table_ptr_max->generalized_column.column_pointer.result->data_type = INT;

            return "Get the min value after issuing print command";




        default : //EVERYTHING ELSE
            return "Hello 165";
    }
}



//end of execute DB

Column* create_column(const char* column_name, char* table_name, bool sorted, Status *ret_status)
{
//Debug
//printf("*********in create column fn******\n");
//printf("column name inside create col fn is ***********%s\n", column_name);
//printf("table  name inside create column fn is ***********%s\n", table_name);

    Table* table_ptr = lookup(table_name);
    table_ptr->columns_size++;

    if(table_ptr->columns_size > table_ptr->col_count)
    {
        ret_status->code = ERROR;
        return NULL;
    }
    Column* col_ptr;

    //table_ptr->columns_size++;

    if(table_ptr->columns == NULL)
    {
        table_ptr->columns = (Column* )(malloc(sizeof(Column) * 1));
        col_ptr = table_ptr->columns;
    }
    else
    {
        if(table_ptr->columns_size <= table_ptr->col_count)
        {
            table_ptr->columns = realloc(table_ptr->columns,(sizeof(Column)*table_ptr->columns_size));
            col_ptr = table_ptr->columns + (table_ptr->columns_size - 1);
        }
        else
        {
            ret_status->code = ERROR;
            return NULL;
        }
    }

    strcpy(col_ptr->name,column_name);
    col_ptr->data = NULL;
    
    ret_status->code = OK;

    //printf("$$$$$$$$$$$$$$$$$$$printing col_ptr before returning$$$$$$$$$$\n");
    //printf("col_ptr is %u\n",col_ptr);
    //printf("$$$$$$$$$$$$$$$$$$$printing col_ptr->name$$$$$$$$$$$$$$$$%s\n",col_ptr->name);
    //printf("$$$$$$$$$$$$$$$$$$$printing col_name$$$$$$$$$$$$$$$$%s\n",column_name);

    return col_ptr;
}


Table* create_table(Db* db, const char* name, size_t num_columns, Status *ret_status) 
{
//Debug line
//printf("Entering create table fn \n");
//printf("the table capacity is %zu", db->tables_capacity);

    Table *table_ptr;

    if(db->tables_capacity == 0)
    {
        ret_status->code = ERROR;
        return NULL;
    }

//Debug line
//printf("Inside create table fn beyong NULL \n");
    db->tables_size++;

    if(db->tables == NULL)
    {
        db->tables = (Table *)malloc(sizeof(Table)*1);
        table_ptr = db->tables;
    }

    else
    {
        db->tables = (Table *)realloc(db->tables,sizeof(Table)*db->tables_size);
        table_ptr = db->tables+(db->tables_size - 1);
    }

    strcpy(table_ptr->name,name);
    table_ptr->col_count = num_columns;
    table_ptr->table_length = MAX_TABLE_LENGTH;
    table_ptr->columns_size = 0;
    table_ptr->data_pos = 0;
    table_ptr->columns = NULL;

    db->tables_capacity--;

    ret_status->code=OK;

//Debug line
//printf("Table name is %s \n",table_ptr->name);
//printf("Table columns are %zu \n",table_ptr->col_count);

	//return db->tables;
    return table_ptr;
}

Status add_db(const char* db_name, bool new) 
{
	struct Status ret_status;
//Debug line
//printf("Name of the DB is %s\n", db_name);

    current_db = (Db *)malloc(sizeof(Db)*1);
    //current_db->name = db_name;
    strcpy(current_db->name,db_name);
    current_db->tables_capacity = MAX_TABLE_CAP;
    current_db->tables_size = 0;
    current_db->tables = NULL;
//Debug line
//printf("Name of the DB in current_db pointer is  %s\n", current_db->name);

	ret_status.code = OK;
	return ret_status;
}

Status db_startup()
{
    Status startup_status;
    startup_status.code = OK;

    struct stat st = {0};
    
    if (stat("data", &st) == -1)
    {
        return startup_status;
       
    }

    FILE *fp_obj_size, *fp_obj_name, *fp_obj_file_name;

    fp_obj_size = fopen("data/DBCatalog_Object_Size.txt", "r");
    fp_obj_name = fopen("data/DBCatalog_Object_Name.txt", "r");
    fp_obj_file_name = fopen("data/DBCatalog_File_Name.txt", "r");

    char size_buffer[MAX_SIZE_NAME];
    
//    int table_count = atoi(fgets(size_buffer, sizeof buffer, fb_obj_size));

//The first value in the object size catalog file is total column count value
 
/*    int obj_size = table_count + column_count + 1;
    int obj_arr[obj_size];

    while (fgets(size_buffer, sizeof size_buffer, fb_obj_size) != NULL)
    {
        obj_arr[obj_index] = atoi(size_buffer);
        obj_index++;
    }
*/

    int column_count = atoi(fgets(size_buffer, sizeof size_buffer, fp_obj_size));

    char* table_count_str = fgets(size_buffer, sizeof size_buffer, fp_obj_size);
    int table_count = atoi(table_count_str);

    int obj_size = table_count + column_count + 1;

    //fseek(fp_obj_size, sizeof(int)*1, SEEK_SET);
    
    fseek(fp_obj_size, -strlen(table_count_str), SEEK_CUR);

    DbCatalog* dbc = malloc(sizeof(DbCatalog)*obj_size);
    DbCatalog* dbc_copy = dbc;
    
    for (int i = 0; i < obj_size; i++)
    {
        strcpy(dbc_copy->obj_name,  fgets(size_buffer, sizeof(size_buffer), fp_obj_name));
        dbc_copy->obj_size =  atoi(fgets(size_buffer, sizeof(size_buffer), fp_obj_size));
        strcpy(dbc_copy->obj_file_name, fgets(size_buffer, sizeof(size_buffer), fp_obj_file_name));

        dbc_copy++;
    }
    
    dbc_copy = dbc;

    char* db_name = dbc_copy->obj_name;
    db_name = trim_newline(db_name);

    // dbc_copy++;

    startup_status = add_db(db_name, false);

    Db* db_ptr = current_db;
    char* tbl_name = NULL;
    int col_data;
    table_count = (dbc_copy)->obj_size;

    for (int i = 0; i < table_count; i++)
    {
        dbc_copy++;
        tbl_name = dbc_copy->obj_name;
        tbl_name = trim_newline(tbl_name);

        size_t no_of_cols = dbc_copy->obj_size;
        Table* tbl_ptr = create_table(current_db, tbl_name, no_of_cols, &startup_status);

        for (size_t j = 0; j < no_of_cols; j++)
        {
            dbc_copy++;
            char* col_name = dbc_copy->obj_name;
            col_name = trim_newline(col_name);

            Column* col_ptr = create_column(col_name, tbl_name, false, &startup_status);
            col_ptr->data = (int* )malloc(sizeof(int)*tbl_ptr->table_length);

            char* file_name_buf = dbc_copy->obj_file_name;
            file_name_buf = trim_newline(file_name_buf);

            char file_name[MAX_SIZE_NAME];
            strcpy(file_name, "data/");
            strcat(file_name, file_name_buf);
            FILE* fp_col_data=fopen(file_name, "rb");

            int data_pos = dbc_copy->obj_size;

            for(int k = 0; k < data_pos; k++)
            {
                fread(&col_data, sizeof(int), 1, fp_col_data);
                col_ptr->data[k] = col_data;
            }

        }

        tbl_ptr->data_pos = dbc_copy->obj_size;
//debug
//printf("table pointer =%u\n", tbl_ptr);
//printf("tbl_ptr->data_pos=%d\n", tbl_ptr->data_pos);
//printf("dbc_copy->obj_size=%d\n", dbc_copy->obj_size);
    }

return startup_status;

}

Status sync_db(Db* db_ptr)
{
    Status sync_status;
    sync_status.code = OK;

    FILE *fp_obj_name, *fp_obj_size, *fp_obj_file_name;
    
    struct stat st = {0};

    if (stat("data", &st) == -1) 
    {
        mkdir("data", 0700);
    }

    fp_obj_size = fopen("data/DBCatalog_Object_Size.txt", "w+");
    fp_obj_name = fopen("data/DBCatalog_Object_Name.txt", "w+");
    fp_obj_file_name = fopen("data/DBCatalog_File_Name.txt", "w+");
    
    Table* tbl_ptr = db_ptr->tables;
    Table* tbl_ptr_copy = tbl_ptr;

    size_t total_column_count = 0;
    for (size_t i = 0; i < db_ptr->tables_size; i++)
    {
        total_column_count += tbl_ptr_copy->col_count;
        tbl_ptr_copy++;
    }

    size_t catalog_size = (db_ptr->tables_size) + (total_column_count) + 1;
    
    DbCatalog* dbc = malloc(sizeof(DbCatalog)*catalog_size);
    DbCatalog* dbc_copy = dbc;
    
    strcpy(dbc_copy->obj_name, db_ptr->name);
    dbc_copy->obj_size = db_ptr->tables_size;
    strcpy(dbc_copy->obj_file_name, "null");

    size_t table_count = db_ptr->tables_size;

    if (table_count == 0)
        return sync_status;

    for(size_t i = 0; i < table_count; i++)
    {
        dbc_copy++;
        strcpy(dbc_copy->obj_name, tbl_ptr->name);
        dbc_copy->obj_size = tbl_ptr->col_count;
        //dbc_copy->obj_size = db_ptr->tables_size;
        strcpy(dbc_copy->obj_file_name, "null");

        size_t column_count = tbl_ptr->col_count;
        Column* col_ptr = tbl_ptr->columns;

        for(size_t j = 0; j < column_count; j++)
        {
            dbc_copy++;

            strcpy(dbc_copy->obj_file_name, db_ptr->name);
            strcat(dbc_copy->obj_file_name, ".");
            strcat(dbc_copy->obj_file_name, tbl_ptr->name);
            strcat(dbc_copy->obj_file_name, ".");
            strcat(dbc_copy->obj_file_name, col_ptr->name);
            strcat(dbc_copy->obj_file_name, ".bin");

            strcpy(dbc_copy->obj_name, col_ptr->name);
            dbc_copy->obj_size = tbl_ptr->data_pos;

            col_ptr++;
        }

        tbl_ptr++;
   }

//persisting the DB Catalog on disk
    dbc_copy = dbc;
    char str_obj_size[64];

    char str_total_column_count[MAX_SIZE_NAME];
    sprintf(str_total_column_count, "%zu", total_column_count);

    fputs(str_total_column_count, fp_obj_size);
    fputs("\n", fp_obj_size);

    for (size_t i = 0; i < catalog_size; i++)
    {
        fputs(dbc_copy->obj_name, fp_obj_name);
        fputs("\n", fp_obj_name);
        
        sprintf(str_obj_size, "%d", dbc_copy->obj_size);
        
        fputs(str_obj_size, fp_obj_size);
        fputs("\n", fp_obj_size);
        
        fputs(dbc_copy->obj_file_name, fp_obj_file_name);
        fputs("\n", fp_obj_file_name);

        dbc_copy++;
    }
        
        fclose(fp_obj_name);
        fclose(fp_obj_size);
        fclose(fp_obj_file_name);
   

//persisting the DB columns on disk now

    tbl_ptr = db_ptr->tables;
    Column* col_ptr = NULL;
    //tbl_ptr->columns;
    int col_data;
    //= col_ptr-Data;
    dbc_copy = dbc;

    for(size_t i = 0; i < catalog_size; i++)
    {
        if(strcmp(dbc_copy->obj_file_name,  "null") == 0)
        {
            dbc_copy++;
            continue;
        }

        char str_file_name[64];
        strcpy(str_file_name, "data/");
        strcat(str_file_name, dbc_copy->obj_file_name);
        FILE* fp_col_data = fopen(str_file_name, "wb+");

        //code fix for col_lookup anomoly
        message_status mes_status = OK_DONE;
        char* str_obj_file_name = (char* )malloc(sizeof(char) * 64);;
        strcpy(str_obj_file_name, dbc_copy->obj_file_name);

        //char* obj_file_name_base = &str_obj_file_name[0];
        //char* obj_file_name_base = "vishal.kumar.sinha";
        char** obj_file_name_index = &str_obj_file_name;

        char* db_name = next_dot_token(obj_file_name_index, &mes_status);
        char* tbl_name = next_dot_token(obj_file_name_index, &mes_status);
        char* col_name = next_dot_token(obj_file_name_index, &mes_status);

        Table* tbl_ptr = lookup(tbl_name);
        Column* col_ptr = tbl_ptr->columns;
        while((strcmp(col_ptr->name, col_name) != 0))
            col_ptr++;

        //col_ptr = col_lookup(dbc_copy->obj_name);

        for (int i = 0; i < dbc_copy->obj_size; i++)
        {
            col_data = col_ptr->data[i];
            fwrite(&col_data, sizeof(int), 1, fp_col_data);
        }

        fclose(fp_col_data);
        dbc_copy++;
    }

    return sync_status;
}

void load_insert(DbOperator* dbo, message* send_message, LoadFile* lf_ptr)
{
    message_status mes_status = OK_DONE;

    lf_ptr++; //lf[1] has the header
    char** handle_index = &(lf_ptr->element);
    char* db_name = next_dot_token(handle_index, &mes_status);
    //get rid of load
    db_name = db_name + 4;
    char* table_name = next_dot_token(handle_index, &mes_status);

    Table* insert_table = lookup(table_name);
    //Column* col = insert_table->columns;
    lf_ptr++; //lf[2] is where the actual data starts    

    char* token = NULL;

    dbo->type = INSERT;
    dbo->operator_fields.insert_operator.table = insert_table;
    dbo->operator_fields.insert_operator.values = malloc(sizeof(int) * insert_table->col_count);
    //int columns_inserted = 0;
    char* result;
    char** command_index = NULL;

    while((lf_ptr->element) != NULL)
    {
        lf_ptr->element += 4;
        command_index = &(lf_ptr->element);
        size_t columns_inserted = 0;

        //get rid of load keyword
        //token = token + 4;

        while ((token = strsep(command_index, ",")) != NULL)
        {
            //get rid of load keyword
            //token = token + 4;
            int insert_val = atoi(token);
            dbo->operator_fields.insert_operator.values[columns_inserted] = insert_val;
            columns_inserted++;
        }

        if (columns_inserted != insert_table->col_count)
        {
            send_message->status = INCORRECT_FORMAT;
            free (dbo);
        }
        result = execute_DbOperator(dbo);
        lf_ptr++;
    }

}

