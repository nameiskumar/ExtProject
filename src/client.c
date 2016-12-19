#define _XOPEN_SOURCE
#define _BSD_SOURCE

/**
 * client.c
 *  CS165 Fall 2015
 *
 * This file provides a basic unix socket implementation for a client
 * used in an interactive client-server database.
 * The client receives input from stdin and sends it to the server.
 * No pre-processing is done on the client-side.
 *
 * For more information on unix sockets, refer to:
 * http://beej.us/guide/bgipc/output/html/multipage/unixsock.html
 **/
#include <stdlib.h>
#include <unistd.h>
#include <string.h>
#include <stdio.h>
#include <stddef.h>

#include <sys/types.h>
#include <sys/socket.h>
#include <sys/un.h>

#include "common.h"
#include "message.h"
#include "utils.h"

#include "cs165_api.h"
#include "parse.h"

#define DEFAULT_STDIN_BUFFER_SIZE 2048
//Db *current_db;

/**
 * connect_client()
 *
 * This sets up the connection on the client side using unix sockets.
 * Returns a valid client socket fd on success, else -1 on failure.
 *
 **/

char* next_token(char** tokenizer, message_status* status) 
{
    char* token = strsep(tokenizer, ",");
    if (token == NULL) 
    {
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


int connect_client() {
    int client_socket;
    size_t len;
    struct sockaddr_un remote;

    log_info("Attempting to connect...\n");

    if ((client_socket = socket(AF_UNIX, SOCK_STREAM, 0)) == -1) {
        log_err("L%d: Failed to create socket.\n", __LINE__);
        return -1;
    }

    remote.sun_family = AF_UNIX;
    strncpy(remote.sun_path, SOCK_PATH, strlen(SOCK_PATH) + 1);
    len = strlen(remote.sun_path) + sizeof(remote.sun_family) + 1;
    if (connect(client_socket, (struct sockaddr *)&remote, len) == -1) {
        perror("client connect failed: ");
        return -1;
    }

    log_info("Client connected at socket: %d.\n", client_socket);
    return client_socket;
}

//send file code
//
//

message_status send_file_to_server(char* query_cmd, int client_socket)
{
    message_status mes_status = OK_DONE;

    if(strncmp(query_cmd, "(", 1) != 0)
    {
        mes_status = INCORRECT_FORMAT;
        return mes_status;
    }

    query_cmd++;
    query_cmd = trim_newline(query_cmd);
    query_cmd = trim_whitespace(query_cmd);

    int last_char = strlen(query_cmd) - 1;

    if (query_cmd[last_char] != ')')
    {
        mes_status = INCORRECT_FORMAT;
        return mes_status;
    }

    query_cmd[last_char] = '\0';
    query_cmd = trim_quotes(query_cmd);

//debug
//printf("the file path and name is %s", query_cmd);

    char* fs_name = (char* )malloc(DEFAULT_STDIN_BUFFER_SIZE);
    strcpy(fs_name,query_cmd);

    char read_buffer[DEFAULT_STDIN_BUFFER_SIZE];
    message send_message;
    char *output_str = NULL;

    FILE *fs = fopen(fs_name, "r");
    if(fs == NULL)
    {
        printf("ERROR: File %s not found.\n", fs_name);
        exit(1);
    }
    bzero(read_buffer, DEFAULT_STDIN_BUFFER_SIZE);
    send_message.payload = read_buffer;
    //int load_file_count = 0;

    while (output_str = fgets(read_buffer, DEFAULT_STDIN_BUFFER_SIZE, fs), !feof(fs))
    {
        send_message.length = strlen(read_buffer) + 4;
        if (send_message.length > 1)
        {
            // Send the message_header, which tells server payload size
            if (send(client_socket, &(send_message), sizeof(message), 0) == -1)
            {
                log_err("Failed to send message header.");
                exit(1);
            }

            char* str_buffer = (char*) calloc(100, sizeof(char));
            strcat(str_buffer, "load");

            strcat(str_buffer, send_message.payload);
            strcpy(send_message.payload, str_buffer);
            send_message.payload = trim_newline(send_message.payload);
            if (send(client_socket, send_message.payload, send_message.length, 0) == -1) 
            {
                log_err("Failed to send query payload.");
                exit(1);
            }

            bzero(read_buffer, DEFAULT_STDIN_BUFFER_SIZE);
            free(str_buffer);

//            bzero(str_load, DEFAULT_STDIN_BUFFER_SIZE);
        }
    }
//    send_message.length = strlen("load File Load Complete") + 1;
    strcpy(send_message.payload, "load File Load Complete");
    send_message.length = strlen(send_message.payload);

    send(client_socket, &(send_message), sizeof(message), 0);
    send(client_socket, send_message.payload, send_message.length, 0);

return mes_status;
}
//

int main(void)
{
    int client_socket = connect_client();
    if (client_socket < 0) 
    {
        printf("client exiting due to socket connect");
        exit(1);
    }

    message send_message;
    message recv_message;

    // Always output an interactive marker at the start of each command if the
    // input is from stdin. Do not output if piped in from file or from other fd
    char* prefix = "";
    if (isatty(fileno(stdin))) 
    {
       // printf("client exiting due to stdin");
        prefix = "db_client > ";
    }

    char *output_str = NULL;
    int len = 0;

    // Continuously loop and wait for input. At each iteration:
    // 1. output interactive marker
    // 2. read from stdin until eof.
    char read_buffer[DEFAULT_STDIN_BUFFER_SIZE];
    send_message.payload = read_buffer;

//debug line
//printf("First msg is %s\n",send_message.payload);

    while (printf("%s", prefix), output_str = fgets(read_buffer, DEFAULT_STDIN_BUFFER_SIZE, stdin), !feof(stdin))
    {
 //debug
 //printf("The payload from client is %s", output_str);

        if (output_str == NULL)
        {
            log_err("fgets failed.\n");
            break;
        }
//debug line
//printf("Subsequent  msg is %s",send_message.payload);
        // Only process input that is greater than 1 character.
        // Ignore things such as new lines.
        // Otherwise, convert to message and send the message and the
        // payload directly to the server.
        send_message.length = strlen(read_buffer);
        if (send_message.length > 1)
        {
      // Send the message_header, which tells server payload size
            if (send(client_socket, &(send_message), sizeof(message), 0) == -1)
            {
                log_err("Failed to send message header.");
                exit(1);
            }

            // Send the payload (query) to server
            if (send(client_socket, send_message.payload, send_message.length, 0) == -1)
            {
                log_err("Failed to send query payload.");
                exit(1);
            }

            if (strncmp(send_message.payload, "load", 4) == 0)
            {
                char* query_cmd = send_message.payload;
                query_cmd += 4;
                send_file_to_server(query_cmd, client_socket);
            }

            // Always wait for server response (even if it is just an OK message)
            if ((len = recv(client_socket, &(recv_message), sizeof(message), 0)) > 0)
            {
                int num_bytes = (int) recv_message.length;
                char payload[num_bytes + 1];
                if (recv_message.status == OK_WAIT_FOR_RESPONSE && (int) recv_message.length > 0)
                {
                    // Calculate number of bytes in response package
                    //int num_bytes = (int) recv_message.length;
                    //char payload[num_bytes + 1];

                    // Receive the payload and print it out
                    if ((len = recv(client_socket, payload, num_bytes, 0)) > 0)
                    {
                        payload[num_bytes] = '\0';
                        printf("%s\n", payload);
                    }
                }
                else
                {
                    len = recv(client_socket, payload, num_bytes, 0);
                }
            }
            else
            {
                if (len < 0)
                {
                    log_err("Failed to receive message.");
                }
                else
                {
		            log_info("Server closed connection\n");
		        }
                   exit(1);
            }
        }

        /*if (strncmp(send_message.payload, "shutdown", 8) != 0)
             continue;*/
    }
    return 0;
}
