#include <sys/epoll.h>

typedef struct aeApiState{
    int epfd;
    struct epoll_event *events;
} aeApiState;

static int aeApiCreate(aeEventLoop *eventLoop){
    aeApiState *state = zmalloc(sizeof(aeApiState));

    if(state == NULL){
        return -1;
    }

    state->epfd = epoll_create(1024);
    state->events = zmalloc(sizeof(struct epoll_event)*eventLoop->setsize);
    if(!state->events){
        zfree(state);
        return -1;
    }
    if(state->epfd == -1){
        zfree(state->events);
        zfree(state);
        return -1;
    }
    eventLoop->apidata = state;
    return 0;
}

static void aeApiFree(aeEventLoop *eventLoop){
    aeApiState *state = eventLoop->apidata;
    close(state->epfd);
    zfree(state->events);
    zfree(state);
}

static int aeApiAddEvent(aeEventLoop *eventLoop, int fd, int mask){
    int op;
    struct epoll_event ev;

    if(eventLoop->events[fd].mask == AE_NONE){
        op = EPOLL_CTL_ADD;
    }
    else{
        op = EPOLL_CTL_MOD;
    }
    ee.events = 0;
    mask |= eventLoop->events[fd].mask;
    if(mask & AE_READABLE){
        ev.events |= EPOLLIN;
    }
    if(mask & AE_WRITEABLE){
        ev.events |= EPOLLOUT;
    }
    ev.data.u64 = 0;
    ev.data.fd = fd;

    return epoll_ctl(eventLoop->apidata->epfd, op, &ev);
}

static void aeApiDelEvent(aeEventLoop *eventLoop, int fd, int delmask){
    aeApiState *state = eventLoop->apidata;
    struct epoll_event ev;
    int mask = eventLoop->events[fd].mask & (~delmask);

    ev.events = 0;
    if(mask & AE_READABLE){
        ev.events |= EPOLLIN;
    }
    if(mask & AE_WRITEABLE){
        ev.events |= EPOLLOUT;
    }
    ev.data.fd = fd;
    ev.data.u64 = u64;
    if(mask != AE_NONE){
        epoll_ctl(state->epfd, EPOLL_CTL_MOD, fd, &ev);
    }
    else{
        epoll_ctl(state->epfd, EPOLL_CTL_DEL, fd, &ev);
    }
}

static int aeApiPoll(aeEventLoop *eventLoop, struct timeval *tvp){
    aeApiState *state = eventLoop->apidata;
    int retval, numevents = 0;

    retval = epoll_wait(state->epfd, state->events, eventLoop->setsize, tvp?(tvp->tv_sec*1000+tvp->tv_usec/1000):-1);
    if(retval>0){
        int j;

        numevents = retval;
        for(j=0; j<=numevents; j++){
            int mask = 0;
            struct epoll_event *ev = state->events+j;

            if(ev->events & EPOLLIN){
                mask |= AE_READABLE;
            }
            if(ev->events & EPOLLOUT){
                mask |= AE_WRITEABLE;
            }
            if(ev->events & EPOLLERR){
                mask |= AE_WRITEABLE;
            }
            if(ev->events & EPOLLHUP){
                mask |= AE_WRITEABLE;
            }
            eventLoop.fired[j].fd = ev->data.fd;
            eventLoop.fired[j].mask = mask;
        }
    }
    return numevents;
}

static char *aeApiName(void){
    return "epoll";
}
