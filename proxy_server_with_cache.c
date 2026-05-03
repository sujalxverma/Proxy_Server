#include "proxy_parse.h"
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/types.h>
#include <sys/socket.h>
#include <netinet/in.h>
#include <netdb.h>
#include <arpa/inet.h>
#include <unistd.h>
#include <fcntl.h>
#include <time.h>
#include <sys/wait.h>
#include <errno.h>
#include <pthread.h>
#include <semaphore.h>
#include <time.h>

/*
 * Time base LRU cache : because they are least recently used (LRU) or because they are too old (time expired)
 * The server uses multiple threads to handle multiple client requests at the same time, instead of processing them one by one.
 *
 * int sem_init(sem_t *sem, int pshared, unsigned int value);  pshared -> either 0 or 1.
 * 0 → shared between threads of same process (most common)
   1 → shared between different processes (via shared memory)
 *
 * AF_INET stands for Address Family: Internet (IPv4).
 * Yes — when you create a socket, you get a file descriptor.
 *
 * inet_addr converts an IPv4 address string into a 32-bit binary format usable by the system.
 * inet_ntop converts a binary IP address → human-readable string.
 *
 * Sockets allow communication between two different processes on the same or different machines
 * void* calloc(size_t num_elements, size_t size_per_element);
 *


 */

typedef struct cacheElement cacheElement;
#define MAX_BYTES 4096           // max allowed size of request/response
#define MAX_CLIENTS 400          // max number of client requests served at a time
#define MAX_SIZE 200 * (1 << 20) // size of the cache
#define MAX_ELEMENT_SIZE 10 * (1 << 20)

struct cacheElement
{
    char *data;
    int len;
    char *url;
    time_t lru_time_track;
    cacheElement *next; // its a linked list.
};

cacheElement *find(char *url);
int add_cacheElement(char *data, int size, char *url);
void removeCacheElement();

int portNumber = 8080; // on this port number,  our proxy server will work.
int proxySocketId;
pthread_t tid[MAX_CLIENTS]; // each thread will create a new socket.
sem_t semaphore;
pthread_mutex_t lock;

cacheElement *head; // global head of the cache linked list.
int cacheSize;

int sendErrorMessage(int socket, int status_code)
{
    char str[1024];
    char currentTime[50];
    time_t now = time(0);

    struct tm data = *gmtime(&now);
    strftime(currentTime, sizeof(currentTime), "%a, %d %b %Y %H:%M:%S %Z", &data);

    switch (status_code)
    {
    case 400:
        snprintf(str, sizeof(str), "HTTP/1.1 400 Bad Request\r\nContent-Length: 95\r\nConnection: keep-alive\r\nContent-Type: text/html\r\nDate: %s\r\nServer: VaibhavN/14785\r\n\r\n<HTML><HEAD><TITLE>400 Bad Request</TITLE></HEAD>\n<BODY><H1>400 Bad Rqeuest</H1>\n</BODY></HTML>", currentTime);
        printf("400 Bad Request\n");
        send(socket, str, strlen(str), 0);
        break;

    case 403:
        snprintf(str, sizeof(str), "HTTP/1.1 403 Forbidden\r\nContent-Length: 112\r\nContent-Type: text/html\r\nConnection: keep-alive\r\nDate: %s\r\nServer: VaibhavN/14785\r\n\r\n<HTML><HEAD><TITLE>403 Forbidden</TITLE></HEAD>\n<BODY><H1>403 Forbidden</H1><br>Permission Denied\n</BODY></HTML>", currentTime);
        printf("403 Forbidden\n");
        send(socket, str, strlen(str), 0);
        break;

    case 404:
        snprintf(str, sizeof(str), "HTTP/1.1 404 Not Found\r\nContent-Length: 91\r\nContent-Type: text/html\r\nConnection: keep-alive\r\nDate: %s\r\nServer: VaibhavN/14785\r\n\r\n<HTML><HEAD><TITLE>404 Not Found</TITLE></HEAD>\n<BODY><H1>404 Not Found</H1>\n</BODY></HTML>", currentTime);
        printf("404 Not Found\n");
        send(socket, str, strlen(str), 0);
        break;

    case 500:
        snprintf(str, sizeof(str), "HTTP/1.1 500 Internal Server Error\r\nContent-Length: 115\r\nConnection: keep-alive\r\nContent-Type: text/html\r\nDate: %s\r\nServer: VaibhavN/14785\r\n\r\n<HTML><HEAD><TITLE>500 Internal Server Error</TITLE></HEAD>\n<BODY><H1>500 Internal Server Error</H1>\n</BODY></HTML>", currentTime);
        // printf("500 Internal Server Error\n");
        send(socket, str, strlen(str), 0);
        break;

    case 501:
        snprintf(str, sizeof(str), "HTTP/1.1 501 Not Implemented\r\nContent-Length: 103\r\nConnection: keep-alive\r\nContent-Type: text/html\r\nDate: %s\r\nServer: VaibhavN/14785\r\n\r\n<HTML><HEAD><TITLE>404 Not Implemented</TITLE></HEAD>\n<BODY><H1>501 Not Implemented</H1>\n</BODY></HTML>", currentTime);
        printf("501 Not Implemented\n");
        send(socket, str, strlen(str), 0);
        break;

    case 505:
        snprintf(str, sizeof(str), "HTTP/1.1 505 HTTP Version Not Supported\r\nContent-Length: 125\r\nConnection: keep-alive\r\nContent-Type: text/html\r\nDate: %s\r\nServer: VaibhavN/14785\r\n\r\n<HTML><HEAD><TITLE>505 HTTP Version Not Supported</TITLE></HEAD>\n<BODY><H1>505 HTTP Version Not Supported</H1>\n</BODY></HTML>", currentTime);
        printf("505 HTTP Version Not Supported\n");
        send(socket, str, strlen(str), 0);
        break;

    default:
        return -1;
    }
    return 1;
}

int connectRemoteServer(char *host_addr, int port_num)
// It creates a client socket inside your proxy server and connects it to the remote (actual) server.
{
    // Creating Socket for remote server ---------------------------
    // create socket in proxy server, which will connect to remote server (the main server).
    int remoteSocket = socket(AF_INET, SOCK_STREAM, 0);

    if (remoteSocket < 0)
    {
        printf("Error in Creating Socket.\n");
        return -1;
    }

    // Get host by the name or ip address provided

    struct hostent *host = gethostbyname(host_addr);
    if (host == NULL)
    {
        // it is used to print formatted output to a file (or stream).
        fprintf(stderr, "No such host exists.\n");
        // printf("DNS look up failed.\n");
        return -1;
    }

    // inserts ip address and port number of host in struct `server_addr`
    struct sockaddr_in server_addr;

    bzero((char *)&server_addr, sizeof(server_addr));
    server_addr.sin_family = AF_INET;
    server_addr.sin_port = htons(port_num);

    // bcopy(src , dest , n)
    bcopy((char *)host->h_addr, (char *)&server_addr.sin_addr.s_addr, host->h_length);

    // Connect to Remote server ----------------------------------------------------

    if (connect(remoteSocket, (struct sockaddr *)&server_addr, (socklen_t)sizeof(server_addr)) < 0)
    {
        fprintf(stderr, "Error in connecting !\n");
        return -1;
    }
    // free(host_addr);
    return remoteSocket;
}

int checkHTTPversion(char *msg)
{
    int version = -1;

    if (strncmp(msg, "HTTP/1.1", 8) == 0)
    {
        version = 1;
    }
    else if (strncmp(msg, "HTTP/1.0", 8) == 0)
    {
        version = 1; // Handling this similar to version 1.1
    }
    else
        version = -1;

    return version;
}

int handle_request(int clientSocket, struct ParsedRequest *request, char *tempReq)
{
    char *buf = (char *)malloc(sizeof(char) * MAX_BYTES);
    strcpy(buf, "GET ");
    strcat(buf, request->path);
    strcat(buf, " ");
    strcat(buf, request->version);
    strcat(buf, "\r\n");

    size_t len = strlen(buf);

    if (ParsedHeader_set(request, "Connection", "close") < 0)
    {
        printf("set header key not work\n");
    }

    if (ParsedHeader_get(request, "Host") == NULL)
    {
        // if host header is missing, then add it using request->host.
        if (ParsedHeader_set(request, "Host", request->host) < 0)
        {
            printf("Set \"Host\" header key not working\n");
        }
    }

    if (ParsedRequest_unparse_headers(request, buf + len, (size_t)MAX_BYTES - len) < 0)
    {
        printf("unparse failed\n");
        // return -1;				// If this happens Still try to send request without header
    }

    // end server port.
    int server_port = 80; // Default Remote Server Port
    if (request->port != NULL)
        server_port = atoi(request->port);

    // end server (removteSockerId)
    int remoteSocketID = connectRemoteServer(request->host, server_port);

    if (remoteSocketID < 0)
        return -1;

    int bytes_send = send(remoteSocketID, buf, strlen(buf), 0);

    bzero(buf, MAX_BYTES);

    bytes_send = recv(remoteSocketID, buf, MAX_BYTES - 1, 0);
    char *temp_buffer = (char *)malloc(sizeof(char) * MAX_BYTES); // temp buffer
    int temp_buffer_size = MAX_BYTES;
    int temp_buffer_index = 0;

    while (bytes_send > 0)
    {
        bytes_send = send(clientSocket, buf, bytes_send, 0);

        for (int i = 0; i < bytes_send / sizeof(char); i++)
        {
            temp_buffer[temp_buffer_index] = buf[i];
            // printf("%c",buf[i]); // Response Printing
            temp_buffer_index++;
        }
        temp_buffer_size += MAX_BYTES;
        temp_buffer = (char *)realloc(temp_buffer, temp_buffer_size);

        if (bytes_send < 0)
        {
            perror("Error in sending data to client socket.\n");
            break;
        }
        bzero(buf, MAX_BYTES);

        bytes_send = recv(remoteSocketID, buf, MAX_BYTES - 1, 0);
    }
    temp_buffer[temp_buffer_index] = '\0';
    free(buf);
    add_cacheElement(temp_buffer, strlen(temp_buffer), tempReq);
    printf("Done\n");
    free(temp_buffer);

    close(remoteSocketID);
    return 0;
}

void *threadFunction(void *socketNew) // socketNew is client socket.
{
    sem_wait(&semaphore);         // if semaphore < 0 , it blocks.
    int p;                        // will store value of semaphor.
    sem_getvalue(&semaphore, &p); // to get value of semaphore.
    printf("Semaphore value is  : %d\n", p);
    int *t = (int *)socketNew; // pointer to client socket.
    int socket = *t;           // derefrencing.

    int bytes_received, len;

    char *buffer = (char *)calloc(MAX_BYTES, sizeof(char));
    // bzero(buffer, MAX_BYTES); all elements set to 0, no need of bzero, if calloc is used.
    bytes_received = recv(socket, buffer, MAX_BYTES, 0);

    while (bytes_received > 0)
    {
        /* code */
        // receiving request and storing it in buffer.
        len = strlen(buffer);
        if (strstr(buffer, "\r\n\r\n") == NULL)
        {
            bytes_received = recv(socket, buffer + len, MAX_BYTES - len, 0);
        }
        else
        {
            break;
        }
    }

    // create copy of request, and to search it in cache.
    char *tempReq = (char *)malloc(strlen(buffer) * sizeof(char) + 1);
    for (int i = 0; i < strlen(buffer); i++)
    {
        // copy buffer to tempReq. (its pointer)
        tempReq[i] = buffer[i];
    }
    struct cacheElement *temp = find(tempReq); // returns pointer to tempReq.

    if (temp != NULL)
    {
        // found request in LRU cache.
        int size = temp->len / sizeof(char);
        int pos = 0;
        char response[MAX_BYTES];
        while (pos < size)
        {
            int chunks = 0;
            bzero(response, MAX_BYTES);
            for (int i = 0; i < MAX_BYTES; i++)
            {
                response[i] = temp->data[pos];
                pos++;
                chunks++;
            }
            // its possible that at last limited data is send,
            // which is not of MAX_BYTES size, so we use chunks,
            // that counts how much size is used.
            send(socket, response, chunks, 0); // why 0 ?, its default.
        }
        printf("Data retrieved from the cache.\n");
        printf("%s\n\n", response);
    }
    else if (bytes_received > 0 && temp == NULL)
    {
        // element not present in the cache.
        // so, need to resolve this request.
        len = strlen(buffer);

        struct ParsedRequest *request = ParsedRequest_create();

        if (ParsedRequest_parse(request, buffer, len) < 0)
        {
            printf("Parsing failed.\n");
        }
        else
        {
            bzero(buffer, MAX_BYTES);
            // handle get request only.
            if (!strcmp(request->method, "GET")) // method is GET.
            {
                if (request->host && request->path && checkHTTPversion(request->version) == 1)
                {
                    // checkHTTPversion : only ches fir 1.0 and 1.1
                    bytes_received = handle_request(socket, request, tempReq);
                    if (bytes_received == -1)
                    {
                        sendErrorMessage(socket, 500);
                    }
                }
                else
                {
                    sendErrorMessage(socket, 500);
                }
            }
            else
            {
                printf("This code does not support any method apart from GET.\n");
            }
        }
        ParsedRequest_destroy(request);
    }
    else if (bytes_received == 0)
    {
        printf("Client is disconnected.\n");
    }

    shutdown(socket, SHUT_RDWR);
    close(socket);
    free(buffer);
    sem_post(&semaphore);
    sem_getvalue(&semaphore, &p);
    printf("Semaphore post value is : %d\n", p);
    free(tempReq);
    return NULL;
}

int main(int argc, char *argv[])
{
    int clientSocketId, clientLen;
    struct sockaddr_in serverAddr, clientAddr;

    sem_init(&semaphore, 0, MAX_CLIENTS); // 0 -> shared b/w threads |  1 -> shared b/w processes, so place semaphore in shared memory.
    pthread_mutex_init(&lock, NULL);

    if (argc == 2)
    {
        portNumber = atoi(argv[1]); /// convert string to integer.
        // our proxy server will work on this port.
    }
    else
    {
        printf("Too few arguments\n");
        exit(1); // exit with an error.
    }

    printf("Starting Proxy Server at port : %d\n", portNumber);
    printf("\n");
    proxySocketId = socket(AF_INET, SOCK_STREAM, 0); // IPV4 TCP , proxySocketId is like a file decriptor.
    if (proxySocketId < 0)
    {
        perror("Failed to create a socket.\n");
        exit(1);
    }
    int reuse = 1;

    if (setsockopt(proxySocketId, SOL_SOCKET, SO_REUSEADDR, (const char *)&reuse, sizeof(reuse)) < 0)
    {
        // used to config the socket created with socket().
        perror("setSockOpt failed\n");
        exit(1);
    }

    bzero((char *)&serverAddr, sizeof(serverAddr)); // bzero is used to set a block of memory to zero (all bytes = 0).
    // bzero(&serverAddr , sizeof(serverAddr));
    serverAddr.sin_family = AF_INET;
    serverAddr.sin_port = htons(portNumber); // specify the port to listen on.
    serverAddr.sin_addr.s_addr = INADDR_ANY;
    /*
    Server listens on:
        localhost ✔
        LAN IP ✔
        any interface ✔
    */

    if (bind(proxySocketId, (struct sockaddr *)&serverAddr, sizeof(serverAddr)) < 0)
    {
        perror("Port is not available.\n");
        exit(1);
    }

    printf("Binding is on port : %d\n", portNumber);

    int listenStatus = listen(proxySocketId, MAX_CLIENTS);
    if (listenStatus < 0)
    {
        perror("Error is listening\n");
        exit(1);
    }

    int i = 0;
    int connectedSocketId[MAX_CLIENTS];

    while (1)
    {
        bzero((char *)&clientAddr, sizeof(clientAddr));
        clientLen = sizeof(clientAddr);
        clientSocketId = accept(proxySocketId, (struct sockaddr *)&clientAddr, (socklen_t *)&clientLen);

        if (clientSocketId < 0)
        {
            printf("Not able to connect\n");
            exit(1);
        }
        else
        {
            connectedSocketId[i] = clientSocketId;
        }

        char str[INET_ADDRSTRLEN];                                         // buffer to store ipv4 address.
        struct sockaddr_in *clientPtr = (struct sockaddr_in *)&clientAddr; // creates a pointer to clientAddr.
        struct in_addr ipAddr = clientPtr->sin_addr;                       // client address, stores ip address of client.
        inet_ntop(AF_INET, &ipAddr, str, INET_ADDRSTRLEN);                 // convert client ip address to human readable form,
        printf("Client is connected with Port number %d and IP address is %s\n", ntohs(clientAddr.sin_port), str);

        pthread_create(&tid[i], NULL, threadFunction, (void *)&connectedSocketId[i]);
        i++;
    }
    close(proxySocketId);
    return 0;
}
cacheElement *find(char *url)
{

    // Checks for url in the cache if found returns pointer to the respective cache element or else returns NULL
    cacheElement *site = NULL;
    // sem_wait(&cache_lock);
    int temp_lock_val = pthread_mutex_lock(&lock);
    printf("Remove Cache Lock Acquired %d\n", temp_lock_val);
    if (head != NULL)
    {
        site = head;
        while (site != NULL)
        {
            if (!strcmp(site->url, url))
            {
                printf("LRU Time Track Before : %ld", site->lru_time_track);
                printf("\nurl found\n");
                // Updating the time_track
                site->lru_time_track = time(NULL);
                printf("LRU Time Track After : %ld", site->lru_time_track);
                break;
            }
            site = site->next;
        }
    }
    else
    {
        printf("\nurl not found\n");
    }
    // sem_post(&cache_lock);
    temp_lock_val = pthread_mutex_unlock(&lock);
    printf("Remove Cache Lock Unlocked %d\n", temp_lock_val);
    return site;
}

void remove_cacheElement()
{
    // If cache is not empty searches for the node which has the least lru_time_track and deletes it
    cacheElement *p;    // cacheElement Pointer (Prev. Pointer)
    cacheElement *q;    // cacheElement Pointer (Next Pointer)
    cacheElement *temp; // Cache element to remove
    // sem_wait(&cache_lock);
    int temp_lock_val = pthread_mutex_lock(&lock);
    printf("Remove Cache Lock Acquired %d\n", temp_lock_val);
    if (head != NULL)
    { // Cache != empty
        for (q = head, p = head, temp = head; q->next != NULL;
             q = q->next)
        { // Iterate through entire cache and search for oldest time track
            if (((q->next)->lru_time_track) < (temp->lru_time_track))
            {
                temp = q->next;
                p = q;
            }
        }
        if (temp == head)
        {
            head = head->next; /*Handle the base case*/
        }
        else
        {
            p->next = temp->next;
        }
        cacheSize = cacheSize - (temp->len) - sizeof(cacheElement) -
                    strlen(temp->url) - 1; // updating the cache size
        free(temp->data);
        free(temp->url); // Free the removed element
        free(temp);
    }
    // sem_post(&cache_lock);
    temp_lock_val = pthread_mutex_unlock(&lock);
    printf("Remove Cache Lock Unlocked %d\n", temp_lock_val);
}

int add_cacheElement(char *data, int size, char *url)
{
    // Adds element to the cache
    // sem_wait(&cache_lock);
    int temp_lock_val = pthread_mutex_lock(&lock);
    printf("Add Cache Lock Acquired %d\n", temp_lock_val);
    int element_size = size + 1 + strlen(url) + sizeof(cacheElement); // Size of the new element which will be added to the cache
    if (element_size > MAX_ELEMENT_SIZE)
    {
        // sem_post(&cache_lock);
        //  If element size is greater than MAX_ELEMENT_SIZE we don't add the element to the cache
        temp_lock_val = pthread_mutex_unlock(&lock);
        printf("Add Cache Lock Unlocked %d\n", temp_lock_val);
        // free(data);
        // printf("--\n");
        // free(url);
        return 0;
    }
    else
    {
        while (cacheSize + element_size > MAX_SIZE)
        {
            // We keep removing elements from cache until we get enough space to add the element
            remove_cacheElement();
        }
        cacheElement *element = (cacheElement *)malloc(sizeof(cacheElement)); // Allocating memory for the new cache element
        element->data = (char *)malloc(size + 1);                             // Allocating memory for the response to be stored in the cache element
        strcpy(element->data, data);
        element->url = (char *)malloc(1 + (strlen(url) * sizeof(char))); // Allocating memory for the request to be stored in the cache element (as a key)
        strcpy(element->url, url);
        element->lru_time_track = time(NULL); // Updating the time_track
        element->next = head;
        element->len = size;
        head = element;
        cacheSize += element_size;
        temp_lock_val = pthread_mutex_unlock(&lock);
        printf("Add Cache Lock Unlocked %d\n", temp_lock_val);
        // sem_post(&cache_lock);
        //  free(data);
        //  printf("--\n");
        //  free(url);
        return 1;
    }
    return 0;
}