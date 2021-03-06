

/* BREAK APART THIS API (TODO MYSELF) */
/* PLEASE UPPERCASE ALL THE STUCTS */

/*
Copyright (c) 2015 Harvard University - Data Systems Laboratory (DASLab)
Permission is hereby granted, free of charge, to any person obtaining a copy of
this software and associated documentation files (the "Software"), to deal in
the Software without restriction, including without limitation the rights to
use, copy, modify, merge, publish, distribute, sublicense, and/or sell copies
of the Software, and to permit persons to whom the Software is furnished to do
so, subject to the following conditions:
The above copyright notice and this permission notice shall be included in all
copies or substantial portions of the Software.
THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
SOFTWARE.
*/

#ifndef CS165_H
#define CS165_H

#include <stdlib.h>
#include <stdbool.h>
#include <stdio.h>
#include "message.h"
#include <sys/types.h>
#include <sys/stat.h>
#include <unistd.h>


//#include "common.h"
//#include "parse.h"
//#include "utils.h"
//#include "message.h"

// Limits the size of a name in our database to 64 characters
#define MAX_SIZE_NAME 64
#define HANDLE_MAX_SIZE 64
#define MAX_TABLE_LENGTH 1024


/**
 *
 * DataCatalog to store contents of Data Catalog before transferring 
 * to the corresponding files.
 * Flag to mark what type of data is held in the struct.
**/

typedef struct DbCatalog
{
    char obj_name[MAX_SIZE_NAME];
    int obj_size;
    char obj_file_name[MAX_SIZE_NAME];
} DbCatalog;



/**
 * EXTRA
 * DataType
 * Flag to mark what type of data is held in the struct.
 * You can support additional types by including this enum and using void*
 * in place of int* in db_operator simliar to the way IndexType supports
 * additional types.
 **/

typedef enum DataType {
     INT,
     LONG,
     FLOAT
} DataType;

//struct Comparator;
//struct ColumnIndex;

typedef enum IndexType
{
    NONE,
    BTREE_CLUSTERED,
    BTREE_UNCLUSTERED,
    SORTED_CLUSTERED,
    SORTED_UNCLUSTERED
} IndexType;

typedef struct BTNode
{
} BTNode;


typedef struct BTree
{
    struct BTNode* root;
    struct BTNode* last_node;
    struct Table* tbl;
    struct Column* col;
} BTree;

/**
 * Added to track the Clumn Indices
 * index pointer will point to the Btree struct for tree indices
 * and the first element of the array for sorted indices
 * priority nbr starts from 1 onward with1 being max priority
 **/
typedef struct ColumnIndex
{
    IndexType type;
    void* index;
    //size_t priority_nbr;
} ColumnIndex;

typedef struct Column {
    char name[MAX_SIZE_NAME];
    int* data;
    // You will implement column indexes later. 
    //void* index;
    struct ColumnIndex* index;
    //bool clustered;
} Column;

//new struct to store data streaming from clients
typedef struct LoadFile
{
    char name[MAX_SIZE_NAME];
    char* element;
    int socket_no;
    int number_of_elements;
} LoadFile;

/**
 * table
 * Defines a table structure, which is composed of multiple columns.
 * We do not require you to dynamically manage the size of your tables,
 * although you are free to append to the struct if you would like to (i.e.,
 * include a size_t table_size).
 * name, the name associated with the table. table names must be unique
 *     within a database, but tables from different databases can have the same
 *     name.
 * - col_count, the number of columns in the table
 * - col,umns this is the pointer to an array of columns contained in the table.
 * - table_length, the size of the columns in the table.
 
 * - Added columns_size to keep track on number of columns added to the table at given point in time
 * - Added data_pos to keep track of the data position (aka row id) on the table at any given time
 **/

typedef struct Table {
    char name [MAX_SIZE_NAME];
    Column *columns;
    size_t col_count;
    size_t table_length;
    size_t columns_size;
    size_t data_pos;
    //Will store the array of col ptr that are indices
    Column** index_priority;
    int index_count;
} Table;

/**
 * db
 * Defines a database structure, which is composed of multiple tables.
 * - name: the name of the associated database.
 * - tables: the pointer to the array of tables contained in the db.
 * - tables_size: the size of the array holding table objects
 * - tables_capacity: the amount of pointers that can be held in the currently allocated memory slot
 **/

typedef struct Db {
    char name[MAX_SIZE_NAME]; 
    Table *tables;
    size_t tables_size;
    size_t tables_capacity;
} Db;

/**
 * Error codes used to indicate the outcome of an API call
 **/
typedef enum StatusCode {
  /* The operation completed successfully */
  OK,
  /* There was an error with the call. */
  ERROR,
} StatusCode;

// status declares an error code and associated message
typedef struct Status {
    StatusCode code;
    char* error_message;
} Status;

// Defines a comparator flag between two values.
typedef enum ComparatorType {
    NO_COMPARISON = 0,
    LESS_THAN = 1,
    GREATER_THAN = 2,
    EQUAL = 4,
    LESS_THAN_OR_EQUAL = 5,
    GREATER_THAN_OR_EQUAL = 6
} ComparatorType;

/*
 * Declares the type of a result column, 
 which includes the number of tuples in the result, the data type of the result, and a pointer to the result data
 */
typedef struct Result {
    size_t num_tuples;
    DataType data_type;
    void *payload;
    //int* payload;
} Result;


typedef union ExtractFloat
{
    float* vf;
    void* vp;
} ExtractFloat;

/*
 * an enum which allows us to differentiate between columns and results
 */
typedef enum GeneralizedColumnType {
    RESULT,
    COLUMN
} GeneralizedColumnType;
/*
 * a union type holding either a column or a result struct
 */
typedef union GeneralizedColumnPointer {
    Result* result;
    Column* column;
} GeneralizedColumnPointer;

/*
 * unifying type holding either a column or a result
 */
typedef struct GeneralizedColumn {
    GeneralizedColumnType column_type;
    GeneralizedColumnPointer column_pointer;
} GeneralizedColumn;

/*
 * used to refer to a column in our client context
 */

typedef struct GeneralizedColumnHandle {
    char name[HANDLE_MAX_SIZE];
    GeneralizedColumn generalized_column;
} GeneralizedColumnHandle;
/*
 * holds the information necessary to refer to generalized columns (results or columns)
 */
typedef struct ClientContext {
    GeneralizedColumnHandle* chandle_table;
    int chandles_in_use;
    int chandle_slots;
} ClientContext;

/**
 * comparator
 * A comparator defines a comparison operation over a column. 
 **/
typedef struct Comparator {
    long int p_low; // used in equality and ranges.
    long int p_high; // used in range compares. 
    GeneralizedColumn* gen_col;
    ComparatorType type1;
    ComparatorType type2;
    char* handle;
} Comparator;

/*
 * tells the databaase what type of operator this is
 */
typedef enum OperatorType {
    CREATE,
    INSERT,
    OPEN,
    LOAD,
    SELECT,
    FETCH,
    PRINT,
    SHUTDOWN,
    MATH,
    ADDSUB,
    SELECT2,
    JOIN
} OperatorType;

typedef enum MathOperatorType
{
    MIN,
    MAX,
    AVG,
    SUM,
    ADD,
    SUB
} MathOperatorType;
/*
 * necessary fields for insertion
 */
typedef struct InsertOperator {
    Table* table;
    int* values;
} InsertOperator;

/*
 * Added LoadOperator struct
 */

typedef struct LoadOperator
{
    char* file_content;
} LoadOperator;

/*
 * necessary fields for selection
 */
typedef struct SelectOperator
{
    Table* table;
    Comparator* comparator;
} SelectOperator;


/*
 * necessary fields for selection
 */
typedef struct FetchOperator
{
    Table* table;
    Column* column;
    char select_handle_name[HANDLE_MAX_SIZE];
} FetchOperator;

typedef struct VariablePool
{
    char name[HANDLE_MAX_SIZE];
    DataType data_type;
    void* payload;
    int num_tuples;
} VariablePool;

typedef struct PrintOperator
{
    int var_count;
    VariablePool* var_pool;
} PrintOperator;

typedef struct OpenOperator
{
    char* db_name;
} OpenOperator;

typedef struct AddSubOperator
{
    char name[HANDLE_MAX_SIZE];
    Result* operand1;
    Result* operand2;
    int num_tuples;
    MathOperatorType type;
}AddSubOperator;

typedef struct MathOperator
{
    char name[HANDLE_MAX_SIZE];
    MathOperatorType type;
    Result* res_operand;
    Column* col_operand;
    int num_tuples;
} MathOperator;

typedef enum JoinOperatorType
{
    NESTED_LOOP,
    HASH
} JoinOperatorType;

typedef struct JoinOperator
{
    JoinOperatorType type;
    char name_left[HANDLE_MAX_SIZE];
    char name_right[HANDLE_MAX_SIZE];
    char select_handle1[HANDLE_MAX_SIZE];
    char select_handle2[HANDLE_MAX_SIZE];
    char fetch_handle1[HANDLE_MAX_SIZE];
    char fetch_handle2[HANDLE_MAX_SIZE];
    int num_tuples;
} JoinOperator;

/*
 * union type holding the fields of any operator
 * Added LoadOperator for file loading
 */
typedef union OperatorFields {
    InsertOperator insert_operator;
    OpenOperator open_operator;
    LoadOperator load_operator;
    SelectOperator select_operator;
    FetchOperator fetch_operator;
    PrintOperator print_operator;
    MathOperator math_operator;
    AddSubOperator addsub_operator;
    JoinOperator join_operator;
} OperatorFields;
/*
 * DbOperator holds the following fields:
 * type: the type of operator to perform (i.e. insert, select, ...)
 * operator fields: the fields of the operator in question
 * client_fd: the file descriptor of the client that this operator will return to
 * context: the context of the operator in question. This context holds the local results of the client in question.
 */
typedef struct DbOperator {
    OperatorType type;
    OperatorFields operator_fields;
    int client_fd;
    ClientContext* context;
} DbOperator;

extern Db *current_db;

Status db_startup();

/**
 * sync_db(db)
 * Saves the current status of the database to disk.
 *
 * db       : the database to sync.
 * returns  : the status of the operation.
 **/
Status sync_db(Db* db);

Status add_db(const char* db_name, bool new);

Table* create_table(Db* db, const char* name, size_t num_columns, Status *status);

//Changed the Column fn declaration to include table name instead of table 
//Column* create_column(char *name, Table *table, bool sorted, Status *ret_status);
Column* create_column(const char* column_name, char* table_name, bool sorted, Status *ret_status);

DbOperator* parse_load(char* query_command, message* send_message);

DbOperator* parse_select(char* query_command, char* handle, int client_socket, ClientContext* context, message* send_message);

Result* select_results(char* db_name, char* table_name, char* col_name, char* lower_bound, char* upper_bound);

Status shutdown_server();
Status shutdown_database(Db* db);

char* execute_DbOperator(DbOperator* query);
void db_operator_free(DbOperator* query);

void load_insert(DbOperator* dbo, message* send_message, LoadFile* loadfile_ptr);
Table*  lookup(const char* table_name);

void bulk_load(Table* );
void load_sorted_cluster(Table* , Column* );
void quickSort( int[], int[], int , int );
int partition( int[], int[], int, int);

#endif /* CS165_H */

