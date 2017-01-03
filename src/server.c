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

#define DEFAULT_QUERY_BUFFER_SIZE 2048
#define DEFAULT_CLIENT_HANDLE_SLOTS 16

/**
 * init_client_context
 * This function initializes the client context
 **/

void init_client_context(ClientContext* client_context)
{
    client_context->chandles_in_use = 0;
    client_context->chandle_slots = DEFAULT_CLIENT_HANDLE_SLOTS;

    client_context->chandle_table = (GeneralizedColumnHandle* )malloc(sizeof(GeneralizedColumnHandle) * client_context->chandle_slots);
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

//start the db befoe taking client requests
    Status db_startup_status;
    db_startup_status = db_startup();

    // create the client context here
    ClientContext* client_context = (ClientContext* )malloc(sizeof(ClientContext));
    init_client_context(client_context);

//Debug
//int i = 0;
//LoadFile* lf = (LoadFile*) malloc(sizeof(LoadFile) * 1024);
//lf->element = (char*) malloc(sizeof(char) * 100);

LoadFile* lf = (LoadFile*) calloc(1024, sizeof(LoadFile));

LoadFile* loadfile_ptr = lf;
    // Continually receive messages from client and execute queries.
    // 1. Parse the command
    // 2. Handle request if appropriate
    // 3. Send status of the received message (OK, UNKNOWN_QUERY, etc)
    // 4. Send response of request.
    do
    {
        length = recv(client_socket, &recv_message, sizeof(message), 0);
        if (length < 0)
        {
            log_err("Client connection closed!\n");
            exit(1);
        }

        else if (length == 0)
        {
            //done = 1;
            continue;
        }
        if (!done)
        {
            char recv_buffer[recv_message.length];
            length = recv(client_socket, recv_buffer, recv_message.length,0);
            recv_message.payload = recv_buffer;
            recv_message.payload[recv_message.length] = '\0';

            char* result;

            if(strncmp(recv_message.payload, "load", 4) == 0)
            {
                if(strncmp(recv_message.payload, "load File Load Complete", 23) == 0)
                {
                    DbOperator* dbo = malloc(sizeof(DbOperator));
                    //struct Status mes_status;

                    dbo->client_fd = client_socket;
                    dbo->type = LOAD;
                    load_insert(dbo, &send_message, loadfile_ptr);
                    result = "Loaded......";
                    //new code
                    send_message.length = strlen(result);
                    send_message.status = OK_DONE;
                    send(client_socket, &(send_message), sizeof(message), 0);
                    send(client_socket, result, send_message.length, 0);
                }

                else
                {
                    lf->element = (char*) malloc(sizeof(char) * 100);
                    strcpy(lf->element, recv_message.payload);
                    lf++;
                    result = "Loading......";
                }
            }

            else
            {
                // 1. Parse command
                DbOperator* query = parse_command(recv_message.payload, &send_message, client_socket, client_context);

                // 2. Handle request
                result = execute_DbOperator(query);
                send_message.length = strlen(result);

                // 3. Send status of the received message (OK, UNKNOWN_QUERY, etc)
                if (send(client_socket, &(send_message), sizeof(message), 0) == -1)
                {
                    log_err("Failed to send message.");
                    exit(1);
                }

                // 4. Send response of request
                if (send(client_socket, result, send_message.length, 0) == -1)
                {
                    log_err("Failed to send message.");
                    exit(1);
                }

                if(strcmp(result, "shutdown") == 0)
                {
                    close(client_socket);
                    //return 1;
                    exit(1);
                }
            }
        }
    } while (!done);

    log_info("Connection closed at socket %d!\n", client_socket);
    close(client_socket);
    //return 0;
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
    int pid;
    int server_socket = setup_server();
    if (server_socket < 0) {
        exit(1);
    }

    log_info("Waiting for a connection %d ...\n", server_socket);

    struct sockaddr_un remote;
    socklen_t t = sizeof(remote);
    int client_socket = 0;

    while(1)
    {
        if ((client_socket = accept(server_socket, (struct sockaddr *)&remote, &t)) == -1)
        {
            log_err("L%d: Failed to accept a new connection.\n", __LINE__);
            exit(1);
        }

        /*pid = fork();
        if(pid < 0)
        {
            perror("ERROR on fork");
            exit(1);
        }
        if(pid == 0)
        {
            close(server_socket);
            handle_client(client_socket);
            exit(0);
        }
        else
        {
            close(client_socket);
        }*/

        handle_client(client_socket);

    }
    //handle_client(client_socket);
    return 0;
}

