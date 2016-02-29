#include "fmacros.h"
#include <sys/types.h>
#include <sys/socket.h>
#include <sys/stat.h>
#include <sys/un.h>
#include <netinet/in.h>
#include <netinet/tcp.h>
#include <arpa/inet.h>
#include <unistd.h>
#include <fcntl.h>
#include <string.h>
#include <netdb.h>
#include <errno.h>
#include <stdarg.h>
#include <stdio.h>
#include "anet.h"

static void anetSetError(char *err, const char *fmt, ...){
    /*write error messages to err by format fmt*/
    va_list ap;
    va_start(ap, fmt);
    vsnprintf(err, ANET_ERR_LEN, fmt, ap);
    va_end(ap);
}

int anetNonBlock(char *err, int fd){
    /*set given fd with NON_BLOCK*/
    int flags;
    if((flags = fcntl(fd, F_GETFL, 0)) < 0){
        anetSetError(err, "fcntl(F_GETFL): %s", strerror(errno));
        return ANET_ERR;
    }
    /*set flags with NON_BLOCK*/
    flags |= O_NONBLOCK;
    if(fcntl(fd, F_SETFL, O_NONBLOCK) < 0){
        anetSetError(err, "fcntl(F_SETFL): %s", strerror(errno));
        return ANET_ERR;
    }
    return ANET_OK;
}

int anetTcpNoDelay(char *err, int fd){
    /*set given fd with TCP_NODELAY*/
    int optval=1;
    if(setsockopt(fd, SOL_SOCKET, TCP_NODELAY, &optval, sizeof(optval)) == -1){
        anetSetError(err, "setsockopt TCP_NODELAY: %s", strerror(errno));
        return ANET_ERR;
    }
    return ANET_OK;
}

int anetSetSendBuffer(char *err, int fd, int buffsize){
    /*set TCP send buffer default size as buffsize*/
    if(setsockopt(fd, SOL_SOCKET, SO_RCVBUF, &buffsize, sizeof(buffsize)) == -1){
        anetSetError(err, "setsockopt SO_RCVBUF: %s", strerror(errno));
        return ANET_ERR;
    }
    return ANET_OK;
}

int anetTcpKeepAlive(char *err, int fd){
    /*set given fd with SO_KEEPALIVE*/
    int optval=1;
    if(setsockopt(fd, SOL_SOCKET, SO_KEEPALIVE, &optval, sizeof(optval)) == -1){
        anetSetError(err, "setsockopt SO_KEEPALIVE: %s", strerror(errno));
        return ANET_ERR;
    }
    return ANET_OK;
}

int anetResolve(char *err, char *host, char *ipbuf){
    /*resolve host from domain format to xxx.xxx format, and save it to ipbuf*/
    struct sockaddr_in sa;
    if(inet_aton(host, &(sa.sin_addr)) == 1){
        /*host is in xxx.xxx format*/
        strcpy(ipbuf, inet_ntoa(sa.sin_addr));     //turn binary 32bit IP address back to xxx.xxx format;
        return ANET_OK;
    }
    struct hostent *hptr;
    if((hptr = gethostbyname(host)) == NULL){
        anetSetError(err, "can't resolve: %s", host);
        return ANET_ERR;
    }
    memcpy(&(sa.sin_addr), hptr->h_addr, sizeof(struct in_addr));
    strcpy(ipbuf, inet_ntoa(sa.sin_addr));          //turn binary 32bit IP address back to xxx.xxx format;
    return ANET_OK;
}

static int anetCreateSocket(char *err, int domain){
    /*create socket fd, and set fd with SO_REUSEADDR*/
    int fd;
    int optval = 1;
    if((fd = socket(domain, SOCK_STREAM, IPPROTO_TCP)) == -1){
        anetSetError(err, "creating socket: %s", strerror(errno));
        return ANET_ERR;
    }
    if(setsockopt(fd, SOL_SOCKET, SO_REUSEADDR, &optval, sizeof(optval)) == -1){
        anetSetError(err, "setsockopt SO_REUSEADDR: %s", strerror(errno));
        return ANET_ERR;
    }
    /*return socket fd if success*/
    return fd;
}

static int anetTcpGenericConnect(char *err, char *addr, int port, int flags){
    /*generic function: connect to server by given addr and port, flags used to set NON_BLOCK*/
    struct sockaddr_in sa;
    int fd;
    /*create socket fd*/
    if((fd = anetCreateSocket(err, AF_INET)) == ANET_ERR){
        return ANET_ERR;
    }
    bzero(&sa, sizeof(sa));                   //don't forget!
    /*turn addr into form of 32bit IP address*/
    if(inet_aton(addr, &(sa.sin_addr)) == 0){
        /*input addr is in domain format*/
        struct hostent *hptr;
        if((hptr = gethostbyname(addr)) == NULL){
            anetSetError(err, "can't resolve: %s", addr);
            close(fd);
            return ANET_ERR;
        }
        memcpy(&(sa.sin_addr), hptr->h_addr, sizeof(struct in_addr));
    }
    sa.sin_port = htons(port);             //don't forget htons!
    sa.sin_family = AF_INET;
    /*consider flags is set as NON_BLOCK*/
    if(flags & ANET_CONNECT_NONBLOCK){
        anetNonBlock(err, fd);
    }
    if(connect(fd, (struct sockaddr *)(&sa), sizeof(sa)) == -1){
        if((!(flags & ANET_CONNECT_NONBLOCK)) || (errno != EINPROGRESS)){
            anetSetError(err, "connect: %s", strerror(errno));
            close(fd);
            return ANET_ERR;
        }
    }
    return fd;
}

int anetTcpConnect(char *err, char *addr, int port){
    /*blocked connection*/
    return anetTcpGenericConnect(err, addr, ANET_CONNECT_NONE);
}

int anetTcpNonBlockConnect(char *err, char *addr, int port){
    /*non_block connection*/
    return anetTcpGenericConnect(err, addr, ANET_CONNECT_NONBLOCK);
}

int anetUnixGenericConnect(char *err, char *path, int flags){
    /*generic function: connect to server process by given path and port, flags used to set NON_BLOCK*/
    struct sockadd_un su;
    int fd;
    /*create unix socket fd*/
    if((fd = anetCreateSocket(err, AF_LOCAL)) == ANET_ERR){
        return ANET_ERR;
    }
    unlink(path);                    //don't forget!
    bzero(&su, sizeof(su));
    su.sun_family = AF_LOCAL;
    strcpy(su.sun_path, path);
    /*consider flags is set as NON_BLOCK*/
    if(flags & ANET_CONNECT_NONBLOCK){
        anetNonBlock(err, fd);
    }
    if(connect(fd, (struct sockaddr *)(&sa), sizeof(sa)) == -1){
        if((!(flags & ANET_CONNECT_NONBLOCK)) || (errno != EINPROGRESS)){
            anetSetError(err, "connect: %s", strerror(errno));
            close(fd);
            return ANET_ERR;
        }
    }
    return fd;
}

int anetUnixConnect(char *err, char *path){
    /*blocked connection*/
    return anetUnixGenericConnect(err, path, ANET_CONNECT_NONE);
}

int anetUnixNonBlockConnect(char *err, char *path){
    /*non_block connection*/
    return anetUnixGenericConnect(err, path, ANET_CONNECT_NONBLOCK);
}

int anetRead(int fd, char *buf, int count){
    /*robust read, consider short count and try to write count bytes into buf*/
    int nleft, nread;
    char *ptr;

    ptr = buf;
    nleft = count;
    while(nleft > 0){
        if((nread = read(fd, ptr, nleft)) < 0){
            if(errno == EINTR){
                nread = 0;        //and call read() again;
            }
            else{
                return -1;
            }
        }
        else if(nread == 0){
            break;                //EOF;
        }
        nleft -= nread;
        ptr += nread;
    }
    return (count-nleft);
}

int anetWrite(int fd, char *buf, int count){
    /*robust write, consider short count and try to write count bytes into buf*/
    int nleft, nwriten;
    const char *ptr;

    ptr = buf;
    nleft = count;
    while(nleft > 0){
        if((nwriten = write(fd, ptr, nleft)) <= 0){
            if((nwriten < 0) && (errno == EINTR)){
                nwriten = 0;               //and call write() again;
            }
            else{
                return -1;                //error;
            }
        }
        nleft -= nwriten;
        ptr += nwriten;
    }
    return count;
}

static int anetListen(char *err, int fd, struct sockaddr *sa, socklen_t len){
    /*bind given socket fd with given addr, and make it to listening fd*/
    if(bind(fd, sa, len) == -1){
        anetSetError(err, "bind: %s", strerror(errno));
        close(fd);
        return ANET_ERR;
    }
    if(listen(fd, 511) == -1){
        anetSetError(err, "listen: %s", strerror(errno));
        close(fd);
        return ANET_ERR;
    }
    return ANET_OK;
}

int anetTcpServer(char *err, int port, char *bindaddr){
    /*listening at given port and given addr*/
    struct sockaddr_in sa;
    int fd;
    /*create socket fd*/
    if((fd = anetCreateSocket(err, AF_INET)) == ANET_ERR){
        return ANET_ERR;
    }
    bzero(&sa, sizeof(sa));
    sa.sin_family = AF_INET;
    sa.sin_port = htons(port);
    /*turn bindaddr into 32bit binary form*/
    if(bindaddr == NULL){
        sa.sin_addr.s_addr = htonl(INADDR_ANY);
    }
    else if(inet_aton(bindaddr, &(sa.sin_addr)) == 0){
        struct hostent *hptr;
        if((hptr = gethostbyname(bindaddr)) == NULL){
            anetSetError(err, "invalid bind address: %s", bindaddr);
            close(fd);
            return ANET_ERR;
        }
        memcpy(&(sa.sin_addr), hptr->h_addr, sizeof(struct in_addr));
    }
    if(anetListen(err, fd, (struct sockaddr *)(&sa), sizeof(sa)) == ANET_ERR){
        return ANET_ERR;
    }
    else{
        return fd;
    }
}

int anetUnixServer(char *err, char *path, mode_t perm){
    /*listening at given port and path*/
    sockadd_un su;
    int fd;
    /*create socket fd*/
    if((fd = anetCreateSocket(err, AF_LOCAL)) == ANET_ERR){
        return ANET_ERR;
    }
    bzero(&su, sizeof(su));
    su.sun_family = AF_LOCAL;
    strcpy(su.sun_path, path);
    if(anetListen(err, fd, (struct sockaddr *)(&su), sizeof(su)) == ANET_ERR){
        return ANET_ERR;
    }
    if(perm){
        chmod(sa.sun_path, perm);
    }
    return fd;
}

static int anetGenericAccept(char *err, int s, struct sockaddr *sa, socklen_t *len){
    /*waiting for new connection from client, return connfd*/
    int fd;
    /*wait for new connection*/
    while(1){
        if((fd = accept(s, sa, len)) == -1){
            if(errno == EINTR){
                continue;               //restart if interrupted by signal;
            }
            else{
                anetSetError(err, "accept: %s", strerror(errno));
                return ANET_ERR;
            }
        }
        break;
    }
    /*connection established, return connfd*/
    return fd;
}

int anetTcpAccept(char *err, int s, char *ip, int *port){
    /*wait for connection from client by TCP, and save client ip and port*/
    int connfd;
    struct sockaddr_in sa;
    socklen_t len;
    /*waiting for and accept client connection*/
    len = sizeof(sa);
    if((connfd = anetGenericAccept(err, s, (struct sockaddr *)(&sa), &len)) == ANET_ERR){
        return ANET_ERR;
    }
    if(ip){
        ip = inet_ntoa(sa.sin_addr);
    }
    if(port){
        *port = ntohs(len);
    }
    return connfd;
}

int anetUnixAccept(char *err, int s){
    /*wait for connection from client by UNIX domain protocol*/
    int connfd;
    struct sockadd_un su;
    socklen_t len;
    /*waiting for and accept client connection*/
    len = sizeof(su);
    if((connfd = anetGenericAccept(err, s, (struct sockaddr *)(&su), &len)) == ANET_ERR){
        return ANET_ERR;
    }
    return connfd;
}

int anetPeerToString(int fd, char *ip, int *port){
    /*get peer ip addr and port*/
    struct sockaddr_in sa;
    socklen_t len;
    /*get peer socket addr*/
    len = sizeof(sa);
    if(getpeername(fd, (struct sockaddr *)(&sa), &len) == -1){
        if(ip){
            ip[0] = '?';
            ip[1] = '\0';
        }
        if(port){
            *port = 0;
        }
        return ANET_ERR;
    }
    if(ip){
        strcpy(ip, inet_ntoa(sa.sin_addr));
    }
    if(port){
        *port = ntohs(sa.sin_port);
    }
    return ANET_OK;
}

int anetSockName(int fd, char *ip, int *port){
    /*get local ip addr and port*/
    struct sockaddr_in sa;
    socklen_t len;
    /*get peer socket addr*/
    len = sizeof(sa);
    if(getsockname(fd, (struct sockaddr *)(&sa), &len) == -1){
        if(ip){
            ip[0] = '?';
            ip[1] = '\0';
        }
        if(port){
            *port = 0;
        }
        return ANET_ERR;
    }
    if(ip){
        strcpy(ip, inet_ntoa(sa.sin_addr));
    }
    if(port){
        *port = ntohs(sa.sin_port);
    }
    return ANET_OK;
}
