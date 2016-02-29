#ifndef ANET_INCLUDED
#define ANET_INCLUDED

#define  ANET_OK   0
#define  ANET_ERR   -1
#define  ANET_ERR_LEN   256

#define  ANET_CONNECT_NONE    0
#define  ANET_CONNECT_NONBLOCK   1

int anetNonBlock(char *err, int fd);
int anetTcpNoDelay(char *err, int fd);
int anetTcpKeepAlive(char *err, int fd);
int anetResolve(char *err, char *host, char *ipbuf);
int anetTcpConnect(char *err, char *addr, int port);
int anetTcpNonBlockConnect(char *err, char *addr, int port);
int anetUnixConnect(char *err, char *path);
int anetUnixNonBlockConnect(char *err, char *path);
int anetRead(int fd, char *buf, int count);
int anetWrite(int fd, char *buf, int count);
int anetTcpServer(char *err, int port, char *bindaddr);
int anetUnixServer(char *err, char *path, mode_t perm);


#endif
