#ifndef AE_INCLUDED
#define AE_INCLUDED

/*event excution states*/
#define  AE_OK  0      /*success*/
#define  AE_ERR -1     /*fail*/

/*file event states*/
#define  AE_NONE    0    /*not set*/
#define  AE_READABLE  1  /*readable*/
#define  AE_WRITABLE  2  /*writeable*/

/*flags*/
#define  AE_FILE_EVENTS   1
#define  AE_TIME_EVENTS   2
#define  AE_ALL_EVENTS    (AE_FILE_EVENTS|AE_TIME_EVENTS)
#define  AE_DONT_WAIT     4

/*timing event flag*/
#define  AE_NOMORE  -1

#define  AE_NOTUSED(V)    ((void) V)

/*State of an event based program*/
struct aeEventLoop;

/*Tyoes and data structures*/
typedef void aeFileProc(struct aeEventLoop *eventLoop, int fd, void *clientData, int mask);
typedef int aeTimeProc(struct aeEventLoop *eventLoop, long long id, void *clientData);
typedef void aeEventFinalizerProc(struct aeEventLoop *eventLoop, void *clientData);
typedef void aeBeforeSleepProc(struct aeEventLoop *eventLoop);

/*File event structure*/
typedef struct aeFileEvent{
    /*file event type, AE_READABLE & AE_WRITABLE*/
    int mask;
    /*handler for read event*/
    aeFileProc *rfileProc;
    /*handler for write event*/
    aeFileProc *wfileProc;
    /*private data for IO multiplexing*/
    void *clientData;
} aeFileEvent;

/*time event structure*/
typedef struct aeTimeEvent{
    /*time event identifier*/
    long long id;
    /*arrive time*/
    long when_sec;
    long when_ms;
    /*event handler*/
    aeTimeProc *timeProc;
    /*event release function*/
    aeEventFinalizerProc *finalizerProc;
    /*private data for IO multiplexing*/
    void *clientData;
    /*the pointer to next event node*/
    struct aeTimeEvent *next;
} aeTimeEvent;

/*A fired event*/
typedef struct aeFiredEvent{
    int fd;     /*file descriptor*/
    int mask;   /*file type*/
} aeFiredEvent;

/*State of an event based program*/
typedef struct aeEventLoop{
    /*highest file descriptor currently registered*/
    int maxfd;
    /*max number of file descriptor tracked*/
    int setsize;
    /*next time event's ID*/
    long long timeEventNextId;
    /*used to detect system clock skew*/
    time_t lastTime;
    /*registered events*/
    aeFileEvent *events;
    /*Fired events*/
    aeFiredEvent *fired;
    /*Time events*/
    aeTimeEvent *timeEventHead;
    /*event handler bottom*/
    int stop;
    /*private and specific data for polling API*/
    void *apidata;
    /*handler function before sleep*/
    aeBeforeSleepProc *beforesleep;
} aeEventLoop;

/*Function Prototypes*/
aeEventLoop *aeCreateEventLoop(int setsize);
void aeDeleteEventLoop(aeEventLoop *eventLoop);
void aeStop(aeEventLoop *eventLoop);
int aeCreateFileEvent(aeEventLoop *eventLoop, int fd, int mask, aeFileProc *proc, void *clientData);
void aeDeleteFileEvent(aeEventLoop *eventLoop, int fd);
int aeGetFileEvents(aeEventLoop *eventLoop, int fd);
long long aeCreateTimeEvent(aeEventLoop *eventLoop, long long milliseconds, aeTimeProc *proc, void *clientData, aeEventFinalizerProc *finalizerProc);
int aeDeleteTimeEvent(aeEventLoop *eventLoop, long long id);
int aeProcessEvents(aeEventLoop *eventLoop, int flags);
int aeWait(int fd, int mask, long long milliseconds);
void aeMain(aeEventLoop *eventLoop);
char *aeGetApiName(void);
void aeSetBeforeSleepProc(aeEventLoop *eventLoop, aeBeforeSleepProc *beforesleep);

#endif
