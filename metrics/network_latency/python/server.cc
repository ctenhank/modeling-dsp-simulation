
// Server side C/C++ program to demonstrate Socket
// programming
#include <netinet/in.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/socket.h>
#include <unistd.h>
#include <time.h>
#include <chrono>
#include <string>
#define PORT 8080

long parse_data(char* msg){
    //long ret;
    char t[19];
    for (int i = 0; i < 19;i++){
        t[i] = msg[i];
    }
    char *eptr;
    long ret = strtol(t, &eptr, 10);
    //printf("%ld\n", ret);
    return ret;
}

int main(int argc, char const* argv[])
{
    int server_fd, new_socket, valread;
    struct sockaddr_in address;
    int opt = 1;
    int addrlen = sizeof(address);
    char buffer[1024] = { 0 };
    char* hello = "Hello from server";
 
    // Creating socket file descriptor
    if ((server_fd = socket(AF_INET, SOCK_STREAM, 0))
        == 0) {
        perror("socket failed");
        exit(EXIT_FAILURE);
    }
 
    // Forcefully attaching socket to the port 8080
    if (setsockopt(server_fd, SOL_SOCKET,
                   SO_REUSEADDR | SO_REUSEPORT, &opt,
                   sizeof(opt))) {
        perror("setsockopt");
        exit(EXIT_FAILURE);
    }
    address.sin_family = AF_INET;
    address.sin_addr.s_addr = INADDR_ANY;
    address.sin_port = htons(PORT);
 
    // Forcefully attaching socket to the port 8080
    if (bind(server_fd, (struct sockaddr*)&address,
             sizeof(address))
        < 0) {
        perror("bind failed");
        exit(EXIT_FAILURE);
    }
    if (listen(server_fd, 3) < 0) {
        perror("listen");
        exit(EXIT_FAILURE);
    }

    int cnt = 0;
    int max = 50000;
    float arr[max];
    //printf("%s\n", argv[1]);

    while (cnt < max)
    {
        if ((new_socket = accept(server_fd, (struct sockaddr *)&address,
                                 (socklen_t *)&addrlen)) < 0)
        {
            perror("accept");
            exit(EXIT_FAILURE);
        }
        //auto end = std::chrono::high_resolution_clock::now();
        //system_clock::time_point currTimePoint = system_clock::now();
        //time_t currTime = system_clock::to_time_t(currTimePoint);

        // printf("Heelo")
        //time_t timer = time(NULL);
        std::chrono::system_clock::time_point now = std::chrono::system_clock::now();
        auto now_ns = std::chrono::time_point_cast<std::chrono::nanoseconds>(now);
        auto epoch = now_ns.time_since_epoch();
        auto value = std::chrono::duration_cast<std::chrono::nanoseconds>(epoch);
        long val = value.count();
        // printf("%lld", timer);

        valread = read(new_socket, buffer, 1024);
        //printf("%s\n", buffer);
        //printf("%ld\n", val);
        long event_time = parse_data(buffer);
        //printf("%ld\n", event_time);
        //printf("time diff: %ld\n", (val - event_time) / 1000000.0);
        float v = (val - event_time) / 1000000.0;
        arr[cnt] = v;
        //printf("%f\n", arr[cnt]);

        // printf("time diff: %f\n", (val - event_time) / 1000000.0);

        send(new_socket, hello, strlen(hello), 0);
        cnt += 1;
        // printf("Hello message sent\n");
    }

    FILE *file = NULL;

    file = fopen(argv[1], "w");
    for (int i = 0; i < cnt;i++){
        fprintf(file, (std::to_string(arr[i])+"\n").c_str());
    }
    fclose(file);
    //  if ((new_socket
    //       = accept(server_fd, (struct sockaddr*)&address,
    //                (socklen_t*)&addrlen))
    //      < 0) {
    //      perror("accept");
    //      exit(EXIT_FAILURE);
    //  }
    //  valread = read(new_socket, buffer, 1024);
    //  printf("%s\n", buffer);
    //  send(new_socket, hello, strlen(hello), 0);
    //  printf("Hello message sent\n");
    return 0;
}