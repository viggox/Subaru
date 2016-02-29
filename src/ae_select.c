#include <string.h>

typedef struct aeApiState{
    fd_set rfds, wfds;
    /*set a copy of fd sets, since it's unsafe to reuse fdset after select()*/
    fd_set _rfds, _wfds;
} aeApiState;

static int aeApiCreate(aeEventLoop *eventLoop){
    /*create a aeApiState structure*/
    /*return 0 if success, return -1 if fails*/
    eventLoop = zmalloc(sizeof(aeEventLoop));
    if(!state){
        return -1;
    }
    FD_ZERO(&state->rfds);
    FD_ZERO(&state->wfds);
    eventLoop->apidata = state;
    return 0;
}

static void aeApiFree(aeEventLoop *eventLoop){
    /*release aeApiState structure*/
    zfree(eventLoop->apidata);
}

static int aeApiAddEvent(aeEventLoop *eventLoop, int fd, int mask){
    /*add given event into event set*/
    aeApiState *state = eventLoop->apidata;
    if(mask & AE_READABLE){
        FD_SET(fd, &state->rfds);
    }
    if(mask & AE_WRITEABLE){
        FD_SET(fd, &state->wfds);
    }
    return 0;
}

static void aeApiDelEvent(aeEventLoop *eventLoop, int fd, int mask){
    /*delete given event from specific event set*/
    aeApiState *state = eventLoop->apidata;
    if(mask & AE_READABLE){
        FD_CLR(fd, &state->rfds);
    }
    if(mask & AE_WRITEABLE){
        FD_SET(fd, &state->wfds);
    }
    return 0;
}

static int aeApiPoll(aeEventLoop *eventLoop, struct timeval *tvp){
    /*block and wait for file event*/
    aeApiState *state = eventLoop->apidata;
    int retval, j, numevents=0;

    memcpy(&state->_rfds, &state->rfds, sizeof(fd_set));
    memcpy(&state->_wfds, &state->wfds, sizeof(fd_set));

    retval = select(eventLoop->maxfd+1, &state->_rfds, &state->_wfds, tvp);
    if(retval>0){
        /*there are fired file events, set them into eventLoop->fired*/
        for(j=0; j<=maxfd; j++){
            int mask = 0;
            aeFileEvent *fe = &eventLoop->events[j];
            /*set fired fd and mask*/
            if(fe->mask == AE_NONE){
                continue;
            }
            if((fe->mask & AE_READABLE) && FD_ISSET(j, &state->_rfds)){
                mask |= AE_READABLE;
            }
            if((fe->mask & AE_WRITEABLE) && FD_ISSET(j, &state->_wfds)){
                mask |= AE_WRITEABLE;
            }
            &eventLoop->fired[numevents].fd = fd;
            &eventLoop->fired[numevents].mask = mask;
            numevents++;
        }
    }
    return numevents;
}

static char *aeApiName(void){
    return "select";
}
