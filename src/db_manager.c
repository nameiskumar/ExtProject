#include "cs165_api.h"
#define MAX_TABLE_CAP 10
#define MAX_TABLE_LENGTH 20
// In this class, there will always be only one active database at a time
Db *current_db;

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
