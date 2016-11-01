#include "cs165_api.h"
#include "utils.h"
#include "message.h"

#define MAX_TABLE_CAP 10
#define DEFAULT_QUERY_BUFFER_SIZE 1024

//#define MAX_TABLE_LENGTH 2
// In this class, there will always be only one active database at a time
Db *current_db;

/*char* next_token(char** tokenizer, message_status* status) 
{
    char* token = strsep(tokenizer, ",");
    if (token == NULL) 
    {
        *status= INCORRECT_FORMAT;
    }
    return token;
}
*/
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
printf("********in lookup fn\n");
printf("the value of table in lookup fn is ***%s\n", tbl_name);

    Table* table_ptr = current_db->tables;
    int count = 0;
//Debug
printf("inside lookup the value of table_ptr->name and tbl_name is %s%s****** \n", table_ptr->name,tbl_name);
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
    int count = 0;

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

printf("The value in the query is %d\n", query);
printf("inside DBOperator-start\n");
printf("The query type is %d\n", query->type);
printf("inside DBOperator-end\n");

    Table* table_ptr;
    Column* col_ptr;
    message_status status;
    
    switch(query->type)
    {
        case 0 : //CREATE Queries
            return "This is a Create query! Cant return results on this";
        
        case 1 : //INSERT QUERIES
//Debug
for (int i=0;i<(current_db->tables)->col_count;i++)
printf("The values to be inserted is %d\n", query->operator_fields.insert_operator.values[i]);
//printf("%d\n", query->operator_fields.insert_operator.values[1]);           
           
            col_ptr = (query->operator_fields.insert_operator.table)->columns;
            table_ptr = query->operator_fields.insert_operator.table;

            if(table_ptr->data_pos == 0)
            {
                for(int i = 0; i < table_ptr->col_count; i++)
                {
                    col_ptr->data = (int* )malloc(sizeof(int)*table_ptr->table_length);
                    col_ptr++;
                }
            }

            if(table_ptr->data_pos >= table_ptr->table_length)
            {
                table_ptr->table_length = table_ptr->table_length + MAX_TABLE_LENGTH;

                for (int i = 0; i < table_ptr->col_count; i++)
                {
                    status = TABLE_FULL;
                    void *temp = (int* )realloc((col_ptr->data),sizeof(int)*MAX_TABLE_LENGTH);
                    if(temp != NULL)
                    {
                        (col_ptr->data) = temp;
                    }
                    else
                    {
                        printf("could not reallocate and value of temp is %u\n",temp);
                    }
                    col_ptr++;
                }
            }

            col_ptr = table_ptr->columns;
            
            for(int j = 0; j < table_ptr->col_count; j++)
            {
                (col_ptr)->data[table_ptr->data_pos] = query->operator_fields.insert_operator.values[j];
                col_ptr++;

//Debug
printf("data position value %d \n", (table_ptr->data_pos));
//printf("value inserted at this point is %d \n", (col_ptr)->data[table_ptr->data_pos]);
            }

            (table_ptr->data_pos)++;
//debug
printf("data position value after increment %d \n", (table_ptr->data_pos));
Column *column_ptr = (current_db->tables)->columns;

//Debug printing
/*
for (int j=0;j<(current_db->tables)->col_count;j++)
{
for (int i=0;i<(current_db->tables)->data_pos;i++)
{
printf("Inserted values are %d\n",(column_ptr)->data[i]);
}
column_ptr++;
}
*/
            return "Data inserted successfully";
            
        case 2 : //SELECTS
            printf("This is select query and results will be returned \n");
            free(query);

        case 3 : //LOADS
            printf("This is a load  query and results will NOT  be returned \n");
            free(query);

        case 6 :
            printf("This is a shutdown  query and results will NOT  be returned \n");
            Status synch_status = sync_db(current_db);

        default : //EVERYTHING ELSE
            return "Hello 165";
    }
}



//end of execute DB


Result* select_results(char* db_name, char* table_name, char* col_name, char* lower_bound, char* upper_bound)
{

//Debug
printf("the lower bd is %s \n", lower_bound);
printf("the upper bd is %s \n", upper_bound);
//printf("the db obj is %s \n", db_object);


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
    Column* col_ptr = col_lookup(col_name);
    Result* res_ptr = (Result* )malloc(sizeof(Result));

    res_ptr->data_type = INT;
    res_ptr->num_tuples = 0;
    
    res_ptr->payload = (int* )malloc(sizeof(int) * tbl_ptr->table_length);

    for (int i=0; i<(tbl_ptr->data_pos); i++)
    {
        if(col_ptr->data[i] < comp->p_high && col_ptr->data[i] >= comp->p_low)
        {
            res_ptr->payload[res_ptr->num_tuples] = i;
            res_ptr->num_tuples++;
        }
    }

return res_ptr;   
}

Column* create_column(const char* column_name, char* table_name, bool sorted, Status *ret_status)
{
//Debug
printf("*********in create column fn******\n");
printf("column name inside create col fn is ***********%s\n", column_name);
printf("table  name inside create column fn is ***********%s\n", table_name);

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

    printf("$$$$$$$$$$$$$$$$$$$printing col_ptr before returning$$$$$$$$$$\n");
    printf("col_ptr is %u\n",col_ptr);
    printf("$$$$$$$$$$$$$$$$$$$printing col_ptr->name$$$$$$$$$$$$$$$$%s\n",col_ptr->name);
    printf("$$$$$$$$$$$$$$$$$$$printing col_name$$$$$$$$$$$$$$$$%s\n",column_name);

    return col_ptr;
}


Table* create_table(Db* db, const char* name, size_t num_columns, Status *ret_status) 
{
//Debug line
printf("Entering create table fn \n");
printf("the table capacity is %d", db->tables_capacity);

    Table *table_ptr;

    if(db->tables_capacity == 0)
    {
        ret_status->code = ERROR;
        return NULL;
    }

//Debug line
printf("Inside create table fn beyong NULL \n");
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
printf("Table name is %s \n",table_ptr->name);
printf("Table columns are %d \n",table_ptr->col_count);

	return db->tables;
}

Status add_db(const char* db_name, bool new) 
{
	struct Status ret_status;
//Debug line
printf("Name of the DB is %s\n", db_name);

    current_db = (Db *)malloc(sizeof(Db)*1);
    //current_db->name = db_name;
    strcpy(current_db->name,db_name);
    current_db->tables_capacity = MAX_TABLE_CAP;
    current_db->tables_size = 0;
    current_db->tables = NULL;
//Debug line
printf("Name of the DB in current_db pointer is  %s\n", current_db->name);

	ret_status.code = OK;
	return ret_status;
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
    for (int i = 0; i < db_ptr->tables_size; i++)
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

    for(int i = 0; i < table_count; i++)
    {
        dbc_copy++;
        strcpy(dbc_copy->obj_name, tbl_ptr->name);
        dbc_copy->obj_size = db_ptr->tables_size;
        strcpy(dbc_copy->obj_file_name, "null");

        size_t column_count = tbl_ptr->col_count;
        Column* col_ptr = tbl_ptr->columns;

        for(int j = 0; j < column_count; j++)
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
    char* str_obj_size[64];

    for (int i = 0; i < catalog_size; i++)
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
    Column* col_ptr = tbl_ptr->columns;;
    dbc_copy = dbc;

    for(int i = 0; i < catalog_size; i++)
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

        for (int i = 0; i < dbc_copy->obj_size; i++)
        {
            fwrite(&col_ptr, sizeof(col_ptr), 1, fp_col_data);
            col_ptr++;
        }

        fclose(fp_col_data);
        dbc_copy++;
    }


//testing the column object here
//Commenting for now
/*
Column* col_test;
Table* tbl_test;
FILE *fp_col_test;
fp_col_test=fopen("data/db1.tbl1.col1.bin","rb");

FILE* fp_test_table = fopen("data/testTable.bin", "wb+");
fwrite(&tbl_ptr, sizeof(tbl_ptr), 1, fp_test_table);
fclose(fp_test_table);

fread(&col_test,sizeof(Column),1,fp_col_test);
printf("%s\n",col_test->name);
printf("%d\n",col_test->data[0]);

FILE* fp_tbl_read = fopen("data/testTable.bin","rb");
fread(&tbl_test, sizeof(Table), 1, fp_tbl_read);
printf("%s\n", tbl_test->name);
printf("%s\n", ((tbl_test)->columns)->name);
printf("%d\n", ((tbl_test)->columns)->data[1]);

fclose(fp_col_test);
fclose(fp_tbl_read);


FILE* fp_test_db = fopen("data/testDB.bin", "wb+");
fwrite(&db_ptr, sizeof(db_ptr), 1, fp_test_db);
fclose(fp_test_db);

Db* db_test;
FILE* fp_db_read = fopen("data/testDB.bin","rb");
fread(&db_test, sizeof(Db), 1, fp_db_read);
printf("%s\n", db_test->name);
*/
//End testing
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
    Column* col = insert_table->columns;
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
        int columns_inserted = 0;

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

