#include "cs165_api.h"
#define MAX_TABLE_CAP 10
//#define MAX_TABLE_LENGTH 2
// In this class, there will always be only one active database at a time
Db *current_db;


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

#define DEFAULT_QUERY_BUFFER_SIZE 1024

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
for (int j=0;j<(current_db->tables)->col_count;j++)
{
for (int i=0;i<(current_db->tables)->data_pos;i++)
{
printf("Inserted values are %d\n",(column_ptr)->data[i]);
}
column_ptr++;
}

            return "Data inserted successfully";
            
        case 2 : //SELECTS
            printf("This is select query and results will be returned \n");
            free(query);

        case 3 : //LOADS
            printf("This is a load  query and results will NOT  be returned \n");
            free(query);

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

void load_insert(DbOperator* dbo, message* send_message, char* lf_ptr)
{
    struct Status ret_status;

    ret_status.code = OK;
    return ret_status;
}

