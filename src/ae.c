#include <stdio.h>
#include <sys/time.h>
#include <sys/types.h>
#include <unistd.h>
#include <stdlib.h>
#include <poll.h>
#include <string.h>
#include <time.h>

#include "ae.h"
#include "zmalloc.h"
#include "config.h"

/*Include the best multiplexing layer supported by this system,the following should be ordered by performances,descending.*/
#ifdef HAVE_EPOLL
#include "ae_epoll.c"
#else
#include "ae_select.c"
#endif

/*PART1:*event handlers*/
aeEventLoop *aeCreateEventLoop(int setsize){
    /*initialize the state of file event handler*/
    aeEventLoop *eventLoop;
    int i;

    eventLoop = zmalloc(sizeof(aeEventLoop));
    /*exception: not enough storage to assign*/
    if(!eventLoop){
        goto err;
    }
    eventLoop->events = zmalloc(sizeof(aeFileEvent)*setsize);
    eventLoop->fired = zmalloc(sizeof(aeFiredEvent)*setsize);
    /*exception: not enough storage*/
    if((!eventLoop->events)||(!eventLoop->fired)){
        goto err;
    }
    /*initialize time event linked list*/
    eventLoop->timeEventHead = NULL;
    eventLoop->timeEventNextId = 0;
    /*other properties*/
    eventLoop->stop = 0;
    eventLoop->maxfd = -1;
    eventLoop->beforesleep = NULL;
    /*initialize apidata (I/O multiplexing specific dataset)*/
    if(aeApiCreate(eventLoop) == -1){
        goto err;
    }
    /*initialize events mask*/
    for(i=0; i<setsize; i++){
        eventLoop->events[i].mask = AE_NONE;
    }
    return eventLoop;

err:
    if(eventLoop){
        zfree(eventLoop->events);
        zfree(eventLoop->fired);
        zfree(eventLoop);
    }
    return NULL;
}

void aeDeleteEventLoop(aeEventLoop *eventLoop){
    /*delete file event handler eventLoop*/
    aeApiFree(eventLoop);       /*free I/O multiplexing specific dataset*/
    zfree(eventLoop->events);
    zfree(eventLoop->fired);
    zfree(eventLoop);
}

void aeStop(aeEventLoop *eventLoop){
    /*stop file event handler*/
    eventLoop->stop = 1;
}

/*PART2: file event handlers*/
int aeCreateFileEvent(aeEventLoop *eventLoop, int fd, int mask, aeFileProc *proc, void *clientData){
    /*add given event with given fd into the listening scope,link the event with event handler*/
    aeFileEvent *fe;
    /*exception: given fd is out of range*/
    if(fd > eventLoop->setsize){
        return AE_ERR;
    }
    fe = &(eventLoop->events[fd]);
    /*add event into listening scope, track it with event handler*/
    if(aeApiAddEvent(eventLoop, fd, mask) == -1){
        return AE_ERR;
    }
    fe->mask |= mask;
    if(fe->mask & AE_READABLE){
        fe->rfileProc = proc;
    }
    if(fe->mask & AE_WRITABLE){
        fe->wfileProc = proc;
    }
    fe->clientData = clientData;
    if(fd > eventLoop->maxfd){
        eventLoop->maxfd = fd;
    }

    return AE_Ok;
}

void aeDeleteFileEvent(aeEventLoop *eventLoop, int fd, int mask){
    /*cancel given evnet with given fd from listening scope, untrack the event with event handler*/
    aeFileEvent *fe;
    int i;
    /*exception: given fd is out of range*/
    if(fd > eventLoop->setsize){
        return AE_ERR;
    }
    fe = &(eventLoop->events[fd]);
    /*exception: event not tracked yet, do nothing*/
    if(fe->mask == AE_NONE){
        return;
    }
    /*update mask and maxfd(if necessary)*/
    fe->mask &= (~mask);
    if((fe->maxfd == fd) && (fe->mask == AE_NONE)){
        for(i=eventLoop->maxfd-1; i>=0; i--){
            if(eventLoop->events[i].mask != AE_NONE){
                break;
            }
        }
        maxfd = i;
    }
    /*cancel listening given type event for the given fd*/
    aeApiDelEvents(eventLoop, fd, mask);
}

int aeGetFileEvents(aeEventLoop *eventLoop, int fd){
    /*get the type of file events the fd is listening*/
    if(fd >= eventLoop->maxfd){
        return 0;
    }
    fe = eventLoop->events[fd];
    return fe->mask;
}

/*PART3: time event handlers*/
static void aeGetTime(long *seconds, long *milliseconds){
    /*Get the current time in the form of x seconds + y milliseconds*/
    struct timeval tv;
    gettimeofday(&tv, NULL);
    *seconds = tv.tv_sec;
    *milliseconds = tv.tv_usec/1000;
}

static void aeAddMillisecondsToNow(long long milliseconds, long *sec, long *ms){
    /*add the milliseconds to current time, and return back in the form of sec+ms*/
    long cur_sec, cur_ms;

    aeGetTime(&cur_sec, &cur_ms);
    /*calculate seconds and milliseconds after add the milliseconds*/
    cur_sec = cur_sec + milliseconds/1000;
    cur_ms = cur_ms + milliseconds%1000;
    /*carry a second if needed*/
    if(cur_ms >= 1000){
        cur_sec++;
        cur_ms -= 1000;
    }
    *sec = cur_sec;
    *ms = cur_ms;
}

long long aeCreateTimeEvent(aeEventLoop *eventLoop, long long milliseconds, aeTimeProc *proc, void *clientData,
        aeEventFinalizerProc *finalizerProc){
    /*add a new time event to server, this new time event will comes after the milliseconds, and the event handler is proc*/
    /*create a time event structure*/
    aeTimeEvent *te = zmalloc(sizeof(aeTimeEvent));
    if(te == NULL){
        return AE_ERR;
    }
    /*set time event properties*/
    te->id = eventLoop->timeEventNextId++;
    te->timeProc = proc;
    te->finalizerProc = finalizerProc;
    te->clientData = clientData;
    addMillisecondsToNow(milliseconds, &(te->when_sec), &(te->when_ms));
    /*insert the new event into event linked list*/
    te->next = (eventLoop->timeEventHead)->next;
    eventLoop->timeEventHead = te;

    return eventLoop->timeEventNextId;
}

int aeDeleteTimeEvent(aeEventLoop *eventLoop, long long id){
    /*accept time event id, and delete that event from server*/
    aeTimeEvent *te, *pte;
    /*release the time event from linked list*/
    te = eventLoop->timeEventHead;
    pte = NULL;
    while(te){
        if(te->id == id){
            if(pte){
                pte->next = te->next;
            }
            else{
                eventLoop->timeEventHead = te->next;
            }
            break;
        }
        pte = te;
        te = te->next;
    }
    if(!te){
        return AE_ERR;    /*no event in list with this ID*/
    }
    /*delete the time event*/
    te->finalizerProc(eventLoop, te->clientData);
    zfree(te);
    return AE_OK;
}

static aeTimeEvent *aeSearchNearestTimer(aeEventLoop *eventLoop){
    /*return the nearest time event in time*/
    aeTimeEvent *te = eventLoop->timeEventHead, *pte;

    while(te){
        pte = te;
        te = te->next;
        if(te && ((te->when_sec*1000+te->when_ms) < (pte->when_sec*1000+pte->when_ms))){
            break;
        }
    }
    return te;
}

static int processTimeEvents(aeEventLoop *eventLoop){
    /*actuator for processing time events*/
    /*traverse all time events, and execute the overdue events*/
    int processed = 0;
    aeTimeEvent *te;
    long long maxId;
    time_t now = time(NULL);
    long now_sec, now_ms;
    /*special case: system clock skews happens when system clock moved to future and then set back to the right value*/
    /*we handle this by trying to process events ASAP, the idea is processing events earlier is less dangerous than delaying them*/
    if(now < eventLoop->lastTime){
        te = eventLoop->timeEventHead;
        while(te){
            te->when_sec = 0;
            te->when_ms = 0;
            te = te->next;
        }
    }
    /*update the lastTime properties*/
    eventLoop->lastTime = now;
    /*set maxId, events with id greater than it will be taken as null events*/
    maxId = eventLoop->timeEventNextId-1;
    /*travese link list and execute the overdue events*/
    te = eventLoop->timeEventHead;
    while(te){
        long long id;
        id = te->id;
        /*ignore null events*/
        if(id > maxId){
            te = te->next;
            continue;
        }
        aeGetTime(&now_sec, &now_ms);
        if((now_sec*1000 + now_ms) >= (te->when_sec*1000 + te->when_ms)){
            int retval = te->timeProc(eventLoop, id, clientData);
            processed++;
            if(retval == AE_NOMORE){
                aeDeleteTimeEvent(eventLoop, id);
            }
            else{
                aeAddMillisecondsToNow(retval, &(te->when_sec), &(te->when_ms));
            }
            te = eventLoop->timeEventHead;
            processed++;
        }
        else{
            te = te->next;
        }
    }
    return processed;
}

/*PART4: event dispatching and execution*/
int aeProcessEvents(aeEventLoop *eventLoop, int flags){
    /*event dispatching and execution, return the number of processed events*/
    /*flags is used to set execution event type*/
    int processed = 0, numevents;

    if((eventLoop->maxfd != -1)||((flags & AE_TIME_EVENTS) && !(flags & AE_DONT_WAIT))){
        int i;
        aeTimeEvent *shortest = NULL;
        struct timeval tv, *tvp;

        if((flags & AE_TIME_EVENTS) && !(flags & AE_DONT_WAIT)){
            shortest = aeSearchNearestTimer(eventLoop);
        }
        if(shortest){
            long now_sec, now_ms;
            tvp = &tv;
            aeGetTime(&now_sec, &now_ms);
            tvp->tv_sec = shortest->when_sec - now_sec;
            tvp->tv_usec = (shortest->when_ms - now_ms)*1000;
            if(tvp->tv_usec < 0){
                tvp->tv_sec--;
                tvp->tv_usec += 1000000;
            }
            if(tvp->tv_sec < 0){
                tvp->tv_sec = 0;
                tvp->tv_usec = 0;
            }
        }
        else{
            tvp = NULL;
        }
        /*block and wait for file event*/
        numevents = aeApiPoll(eventLoop, tvp);
        /*process all happened file events*/
        for(i=0; i<numevents; i++){
            aeFileEvent *fe = &(eventLoop->events[eventLoop->fired[i].fd]);
            int mask = eventLoop->fired[i].mask;
            int fd = eventLoop->fired[i].fd;
            /*set write/read flag, ensure only one of them executes*/
            int rfired;
            if(fe->mask & mask & AE_READABLE){
                rfired = 1;
                fe->rfileProc(eventLoop, fd, clientData, mask);
            }
            else if((fe->mask & mask & AE_WRITABLE) && (fe->wfileProc != fe->rfileProc)){
                rfired = 0;
                fe->wfileProc(eventLoop, fd, clientData, mask);
            }
            processed++;
        }
    }
    /*check time events*/
    if(flags & AE_TIME_EVENTS){
        processed += processTimeEvents(eventLoop);
    }
    return processed;
}

int aeWait(int fd, int mask, long long milliseconds){
    /*block and wait the given fd with given type, return if events finished or time exceeds*/
    /*return -1 if error, return 0 if time exceeds, return event state if success*/
    struct pollfd pfd;
    int retmask = 0, retval;
    /*initialize pfd*/
    memset(&pfd, 0, sizeof(pfd));
    /*set pfd members*/
    pfd.fd = fd;
    if(mask & AE_WRITABLE){
        pfd.events |= POLLIN;
    }
    if(mask & AE_READABLE){
        pfd.events |= POLLOUT;
    }
    retval = poll(&pfd, 1, milliseconds);
    if(retval <= 0){
        return retval;
    }
    if(pfd.events & POLLIN){
        retmask |= AE_READABLE;
    }
    if(pfd.events & POLLOUT){
        retmask |= AE_WRITABLE;
    }
    if(pfd.events & POLLERR){
        retmask |= AE_WRITABLE;
    }
    if(pfd.events & POLLHUP){
        retmask |= AE_WRITABLE;
    }
    return retmask;
}

void aeMain(aeEventLoop *eventLoop){
    /*the main circle of event handler*/
    eventLoop->stop = 0;
    while(!eventLoop->stop){
        if(eventLoop->beforesleep){
            beforesleep(eventLoop);
        }
        aeProcessEvents(eventLoop, AE_ALL_EVENTS);
    }
}

char *aeGetApiName(void){
    /*return name of I/O multiplexing using*/
    return aeApiName();
}

void aeSetBeforeSleepProc(aeEventLoop *eventLoop, aeSetBeforeSleepProc *beforesleep){
    /*set proc needed to be excuted before handling events*/
    eventLoop->beforesleep = beforesleep;
}
