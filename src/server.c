/** server.c
 * CS165 Fall 2015
 *
 * This file provides a basic unix socket implementation for a server
 * used in an interactive client-server database.
 * The server should allow for multiple concurrent connections from clients.
 * Each client should be able to send messages containing queries to the
 * server.  When the server receives a message, it must:
 * 1. Respond with a status based on the query (OK, UNKNOWN_QUERY, etc.)
 * 2. Process any appropriate queries, if applicable.
 * 3. Return the query response to the client.
 *
 * For more information on unix sockets, refer to:
 * http://beej.us/guide/bgipc/output/html/multipage/unixsock.html
 **/
#include <errno.h>
#include <fcntl.h>
#include <stdlib.h>
#include <sys/types.h>
#include <sys/un.h>
#include <sys/socket.h>
#include <unistd.h>
#include <string.h>

#include "common.h"
#include "parse.h"
#include "cs165_api.h"
#include "message.h"
#include "utils.h"
#include "client_context.h"

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
        case 0 :
            return "This is a Create query! Cant return results on this";
        
        case 1 :
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
            
        case 2 :
            printf("This is select query and results will be returned");
            free(query);

        default :
            return "Hello 165";
    }
}


/**
 * handle_client(client_socket)
 * This is the execution routine after a client has connected.
 * It will continually listen for messages from the client and execute queries.
 **/
void handle_client(int client_socket) {
    int done = 0;
    int length = 0;

    log_info("Connected to socket: %d.\n", client_socket);

    // Create two messages, one from which to read and one from which to receive
    message send_message;
    message recv_message;

    // create the client context here
    ClientContext* client_context = NULL;

    // Continually receive messages from client and execute queries.
    // 1. Parse the command
    // 2. Handle request if appropriate
    // 3. Send status of the received message (OK, UNKNOWN_QUERY, etc)
    // 4. Send response of request.
    do {
        length = recv(client_socket, &recv_message, sizeof(message), 0);
        if (length < 0) {
            log_err("Client connection closed!\n");
            exit(1);
        } else if (length == 0) {
            done = 1;
        }

        //debug line
        //printf("First received msg is %s", recv_message.payload);

        if (!done) {
            char recv_buffer[recv_message.length];
            length = recv(client_socket, recv_buffer, recv_message.length,0);
            recv_message.payload = recv_buffer;
            recv_message.payload[recv_message.length] = '\0';

            //Debug line
            printf("Subsequent received msg at the server is %s", recv_message.payload);

            // 1. Parse command
            DbOperator* query = parse_command(recv_message.payload, &send_message, client_socket, client_context);

//Debug line
if(query != NULL)
{
printf("query is %d\n", query);
printf("CLient-fd value in the query is %d\n", query->client_fd);
}
            // 2. Handle request
            char* result = execute_DbOperator(query);
            send_message.length = strlen(result);

            // 3. Send status of the received message (OK, UNKNOWN_QUERY, etc)
            if (send(client_socket, &(send_message), sizeof(message), 0) == -1) {
                log_err("Failed to send message.");
                exit(1);
            }

            // 4. Send response of request
            if (send(client_socket, result, send_message.length, 0) == -1) {
                log_err("Failed to send message.");
                exit(1);
            }
        }
    } while (!done);

    log_info("Connection closed at socket %d!\n", client_socket);
    close(client_socket);
}

/**
 * setup_server()
 *
 * This sets up the connection on the server side using unix sockets.
 * Returns a valid server socket fd on success, else -1 on failure.
 **/
int setup_server() {
    int server_socket;
    size_t len;
    struct sockaddr_un local;

    log_info("Attempting to setup server...\n");

    if ((server_socket = socket(AF_UNIX, SOCK_STREAM, 0)) == -1) {
        log_err("L%d: Failed to create socket.\n", __LINE__);
        return -1;
    }

    local.sun_family = AF_UNIX;
    strncpy(local.sun_path, SOCK_PATH, strlen(SOCK_PATH) + 1);
    unlink(local.sun_path);

    /*
    int on = 1;
    if (setsockopt(server_socket, SOL_SOCKET, SO_REUSEADDR, (char *)&on, sizeof(on)) < 0)
    {
        log_err("L%d: Failed to set socket as reusable.\n", __LINE__);
        return -1;
    }
    */

    len = strlen(local.sun_path) + sizeof(local.sun_family) + 1;
    if (bind(server_socket, (struct sockaddr *)&local, len) == -1) {
        log_err("L%d: Socket failed to bind.\n", __LINE__);
        return -1;
    }

    if (listen(server_socket, 5) == -1) {
        log_err("L%d: Failed to listen on socket.\n", __LINE__);
        return -1;
    }

    return server_socket;
}

// Currently this main will setup the socket and accept a single client.
// After handling the client, it will exit.
// You will need to extend this to handle multiple concurrent clients
// and remain running until it receives a shut-down command.
int main(void)
{
    int server_socket = setup_server();
    if (server_socket < 0) {
        exit(1);
    }

    log_info("Waiting for a connection %d ...\n", server_socket);

    struct sockaddr_un remote;
    socklen_t t = sizeof(remote);
    int client_socket = 0;

    if ((client_socket = accept(server_socket, (struct sockaddr *)&remote, &t)) == -1) {
        log_err("L%d: Failed to accept a new connection.\n", __LINE__);
        exit(1);
    }

    handle_client(client_socket);

    return 0;
}

