#include "cs165_api.h"
#define MAX_TABLE_CAP 10
#define MAX_TABLE_LENGTH 20
// In this class, there will always be only one active database at a time
Db *current_db;


Table* lookup(const char* tbl_name)
{
    Table* table_ptr = current_db->tables;
    while(table_ptr->name != tbl_name)
    {
        table_ptr++;
    }
    if (strcmp(table_ptr->name, tbl_name) != 0)
    {
        cs165_log(stdout, "Table name not found");
        return NULL;
    }
    return table_ptr;
}

Column* create_column(const char* column_name, char* table_name, bool sorted, Status *ret_status)
{
    
    Table* table_ptr = lookup(table_name);
    
    if(current_db->tables_capacity == 0)
    {
        ret_status->code = ERROR;
        return NULL;
    }
    Column* col_ptr;

    table_ptr->columns_size++;

    if(table_ptr->columns == NULL)
    {
        table_ptr->columns = (Column* )(malloc(sizeof(Column) * 1));
        col_ptr = table_ptr->columns;
    }
    else
    {
        if(table_ptr->columns_size <= table_ptr->col_count)
        {
            table_ptr->columns = realloc(table_ptr->columns,1);
            col_ptr = table_ptr->columns + table_ptr->columns_size;
        }
        else
        {
            ret_status->code = ERROR;
            return NULL;
        }

        strcpy(col_ptr->name,column_name);
        col_ptr->data = NULL;
    }
    ret_status->code = OK;
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
        db->tables = (Table *)realloc(db->tables,1);
        table_ptr = db->tables+db->tables_size;
    }

    strcpy(table_ptr->name,name);
    table_ptr->col_count = num_columns;
    table_ptr->table_length = MAX_TABLE_LENGTH;
    table_ptr->columns_size = 0;

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
