#include "xredis.h"
#include <sys/uio.h>

/*PART1: client preparation functions*/
static void setProtocolError(xredisClient *xc, int pos){
    /*trim the input buffer, and reply the protocol error*/
    sds client;

    if(server.verbosity >= XREDIS_VERBOSE){
        sds client = getClientInfoString(xc);
        xredisLog(XREDIS_VERBOSE, "Protocol error from client: %s", client);
    }
    xc->flags |= XREDIS_CLOSE_AFTER_REPLY;
    sdsrange(xc->querybuf, pos, -1);
}

unsigned long getClientOutputBufferMemoryUsage(xredisClient *xc){
    /*return total bytes the changeable output buffer takes*/
    size_t nodelen = sizeof(xrobj) + sizeof(listNode);
    return (xc->reply_bytes + nodelen*listLength(xc->reply));
}

sds getClientInfoString(xredisClient *xc){
    /*save the current client state info into a string*/
    char ip[32], flags[16], events[3];
    int port, emask;
    /*get ip and port for TCP*/
    if(!(flag & XREDIS_UNIX_SOCKET)){
        anetPeerToString(xc->fd, ip, &port);
    }
    /*get the client flags info*/
    char *p = flags;
    if(xc->flags & XREDIS_MULTI){
        *p++ = 'x';
    }
    if(xc->flags & XREDIS_BLOCKED){
        *p++ = 'b';
    }
    if(xc->flags & XREDIS_DIRTY_CAS){
        *p++ = 'd';
    }
    if(xc->flags & XREDIS_CLOSE_AFTER_REPLY){
        *p++ = 'c';
    }
    if(xc->flags & XREDIS_UNBLOCKED){
        *p++ = 'u';
    }
    if(xc->flags & XREDIS_CLOSE_ASAP){
        *p++ = 'A';
    }
    if(xc->flags & XREDIS_UNIX_SOCKET){
        *p++ = 'U';
    }
    /*we set 'N' if no flags in client*/
    if(p == flags){
        *p++ = 'N';
    }
    *p = '\0';
    /*get the event type*/
    emask = aeGetFileEvents(server.el, xc->fd);
    p = events;
    if(emask & AE_READABLE){
        *p++ = 'r';
    }
    if(emask & AE_WRITABLE){
        *p++ = 'w';
    }
    *p = '\0';
    /*output and save the message*/
    return sdscatprintf(sdsempty(), "addr=%s:%d fd=%d age=%ld idle=%d flags=%s db=%d multi=%d qbuf=%lu qbuf-free=%lu obl=%lu oll=%lu omem=%lu event=%s cmd=%s",
            (xc->flags & REDIS_UNIX_SOCKET) ? server.unixsocket : ip,
            port,xc->fd,
            (long)(server.unixtime - xc->ctime),
            (long)(server.unixtime - xc->lastinteraction),
            flags,
            xc->db->id,
            (xc->flags & REDIS_MULTI) ? xc->mstate.count : -1,
            (unsigned long) sdslen(xc->querybuf),
            (unsigned long) sdsavail(xc->querybuf),
            (unsigned long) xc->bufpos,
            (unsigned long) listLength(xc->reply),
            getClientOutputBufferMemoryUsage(xc),
            events,
            xc->lastcmd ? xc->lastcmd->name : "NULL");
}

size_t zmalloc_size_sds(sds s){
    /*return the size needed to hold whole sds and its header*/
    return zmalloc_size(s-sizeof(sdshdr));
}

void *dupClientReplyValue(void *xo){
    incrRefCount((xrobj *)xo);
    return xo;
}

int listMatchObjects(void *a, void *b){
    return equalStringObjects(a, b);
}

xredisClient *createClient(int fd){
    /*create a new client, create fake client if fd == -1*/
    xredisClient *xc = zmalloc(sizeof(xredisClient));
    if(fd != -1){
        anetNonBlock(NULL, fd);
        anetTcpNoDelay(NULL, fd);
    }
    /*link acceptTcpHandler with AE_READABLE event*/
    if(aeCreateFileEvent(server.el, fd, AE_READABLE, readQueryFromClient, xc) == AE_ERR){
        close(fd);
        zfree(xc);
        return NULL;
    }
    /*initialize client properties*/
    selectDb(db, 0);
    xc->fd = fd;
    xc->bufpos = 0;
    xc->querybuf = sdsempty();
    xc->querybuf_peak = 0;
    xc->reptype = 0;
    xc->argc = 0;
    xc->argv = NULL;
    xc->cmd = xc->lastcmd = NULL;
    xc->multibulklen = 0;
    xc->bulklen = -1;
    xc->sentlen = 0;
    xc->flags = 0;
    xc->ctime = xc->lastinteraction = server.unixtime;
    xc->reply = listCreate();
    xc->reply_bytes = 0;
    xc->obuf_soft_limit_reached_time = 0;
    listSetFreeMethod(xc->reply,decrRefCount);
    listSetDupMethod(xc->reply,dupClientReplyValue);
    xc->bpop.keys = dictCreate(&setDictType,NULL);
    xc->bpop.timeout = 0;
    xc->bpop.target = NULL;
    xc->io_keys = listCreate();
    xc->watched_keys = listCreate();
    listSetFreeMethod(xc->io_keys,decrRefCount);
    /*add client into server's client list if the client is not fake*/
    if(fd != -1){
        listAddNodeTail(server.clients, xc);
    }
    /*initialize multi state*/
    initClientMultiState(xc);

    return xc;
}

int prepareClientToWrite(xredisClient *xc){
    /*before sending reply to the client, link sendReplyToClient handler with AE_WRITABLE event*/
    if(xc->fd < 0){
        return XREDIS_ERR;   //fake client;
    }
    /*create AE_WRITABLE event handler fails*/
    if(((xc->bufpos == 0) && (listLength(xc->reply) == 0)) &&
            (aeCreateFileEvent(server.el, xc->fd, AE_WRITABLE, sendReplyToClient, xc) == AE_ERR)){
        return XREDIS_ERR;
    }
    return XREDIS_OK;
}

xrobj *dupLastObjectIfNeeded(list *reply){
    /*create a new object copy to replace the last object in reply list, if it is shared*/
    xredisAssert(listLength(reply) > 0);
    listNode *last = listLast(reply);
    xrobj *xold = listNodeValue(last), *xnew;
    if(xold->refcount > 1){
        xnew = dupStringObject(xold);
        decrRefCount(xold);
        listNodeValue(last) = xnew;
    }
    return listNodeValue(last);
}

/*Low level functions to add more data to output buffers.*/

int _addReplyToBuffer(xredisClient *xc, char *s, size_t len){
    /*add s into buf*/
    freelen = XREDIS_REPLY_CHUNK_BYTES - bufpos -1;
    /*close is going to be closed*/
    if(xc->flags & XREDIS_CLOSE_AFTER_REPLY){
        return XREDIS_OK;
    }
    /*return if reply list isnot empty*/
    if(listLength(xc->reply) > 0){
        return XREDIS_ERR;
    }
    if(len > freelen){
        return XREDIS_ERR;
    }
    memcpy(xc->buf+xc->bufpos, s, len);
    bufpos += len;

    return XREDIS_OK;
}

void _addReplyObjectToList(xredisClient *xc, xrobj *xo){
    /*add xo into reply list*/
    if(xc->flags & XREDIS_CLOSE_AFTER_REPLY){
        return;
    }
    /*case which reply is empty*/
    if(listLength(xc->reply) == 0){
        incrRefCount(xo);
        listAddNodeTail(xc->reply, xo);
        xc->reply_bytes += zmalloc_size_sds(xo->ptr);
    }
    else{
        xrobj *tail = listNodeValue(listLast(xc->reply));
        if((tail != NULL) && ((sdslen(tail->ptr)+sdslen(xo->ptr)) <= XREDIS_REPLY_CHUNK_BYTES)){
            xc->reply_bytes -= zmalloc_size_sds(tail->ptr);
            tail = dupLastObjectIfNeeded(xc->reply);
            tail->ptr = sdscatlen(tail->ptr, xo->ptr, sdslen(xo->ptr));
            xc->reply_bytes += zmalloc_size_sds(tail->ptr);
        }
        else{
            incrRefCount(xo);
            listAddNodeTail(xc->reply, xo);
            xc->reply_bytes += zmalloc_size_sds(xo->ptr);
        }
    }
    /*close client if output buffer is reached*/
    asyncCloseClientOnOutputBufferLimitReached(xc);
}

void _addReplySdsToList(xredisClient *xc, sds s){
    /*add sds to the reply list*/
    if(xc->flags & XREDIS_CLOSE_AFTER_REPLY){
        return;
    }
    if(listLength(xc->reply) == 0){
        xrobj *xo = createObject(XREDIS_STRING, s);
        listAddNodeTail(xc->reply, xo);
        xc->reply_bytes += zmalloc_size_sds(s);
    }
    else{
        xrobj *tail = listNodeValue(listLast(xc->reply));
        if((tail != NULL) && ((sdslen(tail->ptr)+sdslen(xo->ptr)) <= XREDIS_REPLY_CHUNK_BYTES)){
            xc->reply_bytes -= zmalloc_size_sds(tail->ptr);
            tail = dupLastObjectIfNeeded(xc->reply);
            tail->ptr = sdscatlen(tail->ptr, s, sdslen(s));
            xc->reply_bytes += zmalloc_size_sds(tail->ptr);
        }
        else{
            xrobj *xo = createObject(XREDIS_STRING, s);
            listAddNodeTail(xc->reply, xo);
            xc->reply_bytes += zmalloc_size_sds(s);
        }
    }
    asyncCloseClientOnOutputBufferLimitReached(xc);
}

void _addReplyStringToList(xredisClient *xc, char *s, size_t len){
    /*add string to the reply list*/
    if(xc->flags & XREDIS_CLOSE_AFTER_REPLY){
        return;
    }
    if(listLength(xc->reply) == 0){
        xrobj *xo = createStringObject(s, len);
        listAddNodeTail(xc->reply, xo);
        xc->reply_bytes += zmalloc_size_sds(xo->ptr);
    }
    else{
        xrobj *tail = listNodeValue(listLast(xc->reply));
        if((tail != NULL) && ((sdslen(tail->ptr)+sdslen(xo->ptr)) <= XREDIS_REPLY_CHUNK_BYTES)){
            xc->reply_bytes -= zmalloc_size_sds(tail->ptr);
            tail = dupLastObjectIfNeeded(xc->reply);
            tail->ptr = sdscatlen(tail->ptr, s, len);
            xc->reply_bytes += zmalloc_size_sds(tail->ptr);
        }
        else{
            xrobj *xo = createStringObject(s, len);
            listAddNodeTail(xc->reply, xo);
            xc->reply_bytes += zmalloc_size_sds(s);
        }
    }
    asyncCloseClientOnOutputBufferLimitReached(xc);
}

/*Higher level functions to queue data on the client output buffer*/

void addReply(xredisClient *xc, xrobj *xo){
    /*add the contents in xo into client output buffer*/
    /*corner case: fake client or fail to create AE_WRITABLE event handler*/
    if(prepareClientToWrite(xc) != XREDIS_OK){
        return;
    }
    if(xo->encoding == XREDIS_ENCODING_RAW){
        if(_addReplyToBuffer(xc, xo->ptr, sdslen(xo->ptr)) != XREDIS_OK){
            _addReplyObjectToList(xc, xo);
        }
    }
    else if(xo->encoding == XREDIS_ENCODING_INT){
        if((listLength(xc->reply) == 0) && ((sizeof(xc->buf)-bufpos) >= 32)){
            char buf[32];
            int len;

            len = ll2string(buf, 32, (long)(xo->ptr));
            if(_addReplyToBuffer(xc, buf, len) == XREDIS_OK){
                return;                    //XREDIS_ERR case should never happen;
            }
        }
        /*create a decoded copy of INT object*/
        xrobj *nxo = getDecodedObject(xo);
        if(_addReplyToBuffer(xc, nxo->ptr, sdslen(nxo->ptr)) != XREDIS_OK){
            _addReplyObjectToList(xc, nxo);
        }
        decrRefCount(nxo);    //IT'S USELESS NOW, HAHAHAHA =_=;
    }
    else{
        xredisPanic("Wrong obj->encoding in addReply()");
    }
}

void addReplySds(xredisClient *xc, sds s){
    /*add sds reply, format: content\r\n*/
    if(prepareClientToWrite(xc) != XREDIS_OK){
        /*this sds is no lenger needed, free it*/
        sdsfree(s);
        return;
    }
    if(_addReplyToBuffer(xc, s, sdslen(s)) == XREDIS_OK){
        sdsfree(s);         //sds is no longer neeeded;
        return;
    }
    _addReplySdsToList(xc, s);
}

void addReplyString(xredisClient *xc, char *s, size_t len){
    /*add string reply, format: content\r\n*/
    if(prepareClientToWrite(xc) != XREDIS_OK){
        return;
    }
    if(_addReplyToBuffer(xc, s, len) != XREDIS_OK){
        _addReplyStringToList(xc, s, len);
    }
}

void addReplyErrorLength(xredisClient *xc, char *s, size_t len){
    /*add s as error message*/
    addReplyString(xc, "-ERR ", 5);
    addReplyString(xc, s, len);
    addReplyString(xc, "\r\n", 2);
}

void addReplyError(xredisClient *xc, char *err){
    /*add error reply, format: content\r\n*/
    addReplyErrorLength(xc, err, strlen(err));
}

void addReplyErrorFormat(xredisClient *xc, const char *fmt, ...){
    /*add multi error messages to reply*/
    size_t len;
    int j;
    sds s;

    va_list ap;
    va_start(ap, fmt);
    s = sdscatvprintf(sdsempty(), fmt, ap);
    va_end(ap);
    /*avoid new line in the string, otherwise invalid protocol is emitted*/
    len = sdslen(s);
    for(j=0; j<len; j++){
        if((s[j] == '\r')||(s[j] == '\n')){
            s[j] = ' ';
        }
    }
    /*add s as error message*/
    addReplyErrorLength(xc, s, sdslen(s));
    /*s is useless, free it!*/
    sdsfree(s);
}

void addReplyStatusLength(xredisClient *xc, char *s, size_t len){
    /*add s as status message*/
    addReplyString(xc,"+", 1);
    addReplyString(xc, s, len);
    addReplyString(xc, "\r\n", 2);
}

void addReplyStatus(xredisClient *xc, char *status){
    /*add string err as status message*/
    addReplyStatusLength(xc, status, strlen(status));
}

void addReplySatusFormat(xredisClient *xc, const char *fmt, ...){
    /*add multi status messages to reply*/
    size_t len;
    sds s;

    va_list ap;
    va_start(ap, fmt);
    s = sdscatvprintf(sdsempty(), fmt, ap);
    va_end(ap);
    /*add s as status message*/
    addReplyStatusLength(xc, s, sdslen(s));
    /*s is useless, free it!*/
    sdsfree(s);
}

void *addDeferredMultiBulkLength(xredisClient *xc){
    /*add a node hold NULL into reply list, as placeholder of unknown message*/
    /*return the last object*/
    if(prepareClientToWrite(xc) != XREDIS_OK){
        return NULL;
    }
    xrobj *xo = createObject(XREDIS_STRING, NULL);
    /*add empty object to list as placeholder*/
    listAddNodeTail(xc->reply, xo);
    /*return it*/
    return listLast(xc->reply);
}

void setDeferredMultiBulkLength(xredisClient *xc, void *node, long length){
    /* Populate the length object and try glueing it to the next chunk. */
    listNode *ln = (listNode *)node;
    xrobj *len, *next;
    /*add length to the node*/
    len = listNodeValue(ln);
    sdscatprintf(len->ptr, "%ld", length);
    /*glue next node to it*/
    next = ln->next;
    /*only glue when next node exists and is non-NULL*/
    if((next) && (next->ptr)){
        xc->reply_bytes -= zmalloc_size_sds(len->ptr);
        xc->reply_bytes -= zmalloc_size_sds(next->ptr);
        sdscatlen(len->ptr, next->ptr, sdslen(next->ptr));
        listDelNode(xc->reply, next);
        xc->reply_bytes += zmalloc_size_sds(len->ptr);
    }
    asyncCloseClientOnOutputBufferLimitReached(xc);
}

void addReplyDouble(xredisClient *xc, double d){
    /*add a double as a bulk reply, in the format: "3\r\n679\r\n"*/
    char dbuf[128], sbuf[128];
    int dlen, slen;
    /*generate protocol*/
    dlen = snprintf(dbuf, "%.17g", d);
    slen = snprintf(sbuf, "$%d\r\n%s\r\n", dlen, dbuf);
    addReplyString(xc, sbuf, slen);
}

void addReplyLongLongWithPrefix(xredisClient *xc, long long ll, char prefix){
    /* Add a long long as integer reply or bulk len / multi bulk count*/
    /*Basically this is used to output <prefix><long long><crlf>. */
    char buf[128];
    int len;
    /* '*' case and '$' case */
    if(ll < XREDIS_SHARED_BULKHDR_LEN){
        if(prefix == '*'){
            addReply(xc, shared.mbulk[ll]);
            return;
        }
        if(prefix == '$'){
            addReply(xc, shared.bulk[ll]);
            return;
        }
    }
    /*usual case*/
    buf[0] = prefix;
    len = snprintf(buf, "%s", ll);
    buf[len+1] = "\r";
    buf[len+2] = "\n";
    addReplyString(xc, buf, len+3);
}

void addReplyLongLong(xredisClient *xc, long long ll){
    /*add long long reply*/
    if(ll == 0){
        addReply(xc, shared.czero);
    }
    else if(ll == 1){
        addReply(xc, shared.cone);
    }
    else{
        addReplyLongLongWithPrefix(xc, ll, ":");
    }
}

void addReplyMultiBulkLen(xredisClient *xc, long length){
    /*add the length of multi bulk into reply*/
    addReplyLongLongWithPrefix(xc, length, "*");
}

void addReplyBulkLen(xredisClient *xc, xrobj *xo){
    /*Create the length prefix of a bulk reply, example: $2567 */
    int len;
    /*case1: RAW encoding*/
    if(xo->encoding == XREDIS_ENCODING_RAW){
        len = sdslen(xc->ptr);
    }
    else if(xc->encoding == XREDIS_ENCODING_INT){
        /*find number of bytes the integer takes*/
        len = 1;
        long num = (long)(xo->ptr);
        if(num < 0){
            len++;
            num = -num;
        }
        while((num /= 10)){
            len++;
        }
    }
    addReplyLongLongWithPrefix(xc, len, "$");
}

void addReplyBulk(xredisClient *xc, xrobj *xo){
    /*add a xredis object as a bulk reply*/
    addReplyBulkLen(xc, xo);
    addReply(xc, xo);
    addReply(xc, shared.crlf);
}

void addReplyBulkCBuffer(xredisClient *xc, void *p, size_t len){
     /*add a C buffer as bulk reply*/
    addReplyLongLongWithPrefix(xc, len, '$');
    addReplyString(xc, p, len);
    addReply(xc, shared.crlf);
}

void addReplyBulkCString(xredisClient *xc, char *s){
    /*add a C null termainated string as bulk reply*/
    if(s == NULL){
        addReply(xc, shared.nullbulk);
    }
    else{
        addReplyBulkCBuffer(xc, s, strlen(s));
    }
}

void addReplyBulkLongLong(xredisClient *xc, long long ll){
    /*add a long long as a bulk reply*/
    char lbuf[32];
    int len;
    /*turn long long to string*/
    len = ll2string(lbuf, 32, ll);
    addReplyLongLongWithPrefix(xc, len, '$');
    addReplyString(xc, lbuf, len);
    addReply(xc, shared.crlf);
}

/*------handlers for file events--------*/

/*------ACCEPT handlers--------*/
void copyClientOutputBuffer(xredisClient *dst, xredisClient *src){
    /*copy contents in src buffer into dst buffer*/
    /*clear dst's reply list*/
    listRelease(dst->reply);
    dst->reply = listDup(src->reply);
    dst->reply_bytes = src->reply_bytes;
    memcpy(dst->buf, src->buf, sizeof(src->buf));
    dst->bufpos = src->bufpos;
}

static void acceptCommandHandler(int fd, int flags){
    /*create new client state with fd and flags*/
    xredisClient *xc = createClient(fd);
    if(!xc){
        close(fd);
        xredisLog(XREDIS_WARNING, "Error allocating resources for the client");
        return;
    }
    /*clients number exceed, write error message to client, then close it*/
    if(listLength(server.clients) > server.maxclients){
        char *err = "-ERR max number of clients reached\r\n";
        write(fd, err, strlen(err));
        /*write error first, close client second*/
        freeClient(xc);
        server.stat_rejected_conn++;
        return;
    }
    server.stat_numconnections++;
    xc->flags |= flags;
}

void acceptTcpHandler(aeEventLoop *el, int fd, void *privdata, int mask){
    /*XREDIS connection accept handler*/
     char cip[128];
     int cport, cfd;
     /*wait for connection from client*/
     cfd = anetTcpAccept(server.neterr, fd, cip, cport);
     if(cfd == XREDIS_ERR){
        xredisLog(XREDIS_WARNING, "Accepting client connection: %s", server.neterr);
        return;
     }
     xredisLog(XREDIS_VERBOSE, "Accepted %s:%d", cip, cport);
     acceptCommandHandler(cfd, 0);
}

void acceptUnixHandler(aeEventLoop *el, int fd, void *privdata, int mask){
    /*connection accept handler for UNIX socket*/
    int cfd;
    /*wait for connection from client by UNIX socket*/
    cfd = anetUnixAccept(server.neterr, fd);
    if(cfd == XREDIS_ERR){
        xredisLog(XREDIS_WARNING, "Accepting client connection: %s", server.neterr);
        return;
    }
    xredisLog(XREDIS_VERBOSE, "Accepted connection to %s", server.unixsocket);
    acceptCommandHandler(cfd, 0);
}

/*-------free client------*/
static void freeClientArgv(xredisClient *xc){
    /*free argvs in client*/
    int i;
    for(i=0; i < xc->argc; i++){
        decrRefCount(xc->argv[i]);
    }
    xc->argc = 0;
    xc->cmd = NULL;
}

void freeClient(xredisClient *xc){
    /*free the client*/
    /*clear current client*/
    if(server.current_client == xc){
        server.current_client = NULL;
    }
    /*release the querybuf*/
    sdsfree(xc->querybuf);
    xc->querybuf = NULL;
    if(xc->flags & XREDIS_BLOCKED){
        unblockClientWaitingData(xc);
    }
    dictRelease(xc->bpop.keys);
    /*UNWATCH all the keys*/
    unwatchAllKeys(c);
    listRelease(c->watched_keys);
    /*delete the client from listening scope, both for AE_READABLE and AE_WRITABLE events*/
    aeDeleteFileEvent(server.el, xc->fd, AE_READABLE);
    aeDeleteFileEvent(server.el, xc->fd, AE_WRITABLE);
    /*release reply list*/
    listRelease(xc->reply);
    /*free argvs in client*/
    freeClientArgv(xc);
    close(xc->fd);
    /*remove this client from list of clients*/
    listNode *ln = listSearchKey(server.clients, xc);
    xredisAssert(ln != NULL);
    listDelNode(server.clients, ln);
    /* When client was just unblocked because of a blocking operation,
     * remove it from the list with unblocked clients. */
    if(xc->flags & XREDIS_UNBLOCKED){
        ln = listSearchKey(server.unblocked_clients, xc);
        xredisClient(ln != NULL);
        listDelNode(server.unblocked_clients, ln);
    }
    /*release io_keys list*/
    listRelease(xc->io_keys);
    /*If this client was scheduled for async freeing we need to remove it from the queue.*/
    if(xc->flags & XREDIS_CLOSE_ASAP){
        ln = listSearchKey(server.clients_to_close, xc);
        xredisAssert(ln != NULL);
        listDelNode(server.clients_to_close, ln);
    }
    /*release memory*/
    zfree(xc->argv);
    freeClientMultiState(xc);
    zfree(xc);
}

void freeClientAsync(xredisClient *xc){
    if(xc->flags & XREDIS_CLOSE_ASAP){
        return;
    }
    xc->flags |= XREDIS_CLOSE_ASAP;
    listAddNodeTail(server.clients_to_close, xc);
}

void freeClientsInAsyncFreeQueue(void){
     /*free server.clients_to_close list, free clients on it*/
    while(listLength(server.clients_to_close)){
        listNode *ln = listFirst(server.clients_to_close);
        xredisClient *xc = listNodeValue(ln);
        /*free the header node*/
        xc->flags &= ~XREDIS_CLOSE_ASAP;
        freeClient(xc);
        listDelNode(server.clients_to_close, ln);
    }
}

/*----------SEND REPLY handlers----------*/
void sendReplyToClient(aeEventLoop *el, int fd, void *privdata, int mask){
    /*XREDIS reply sending handler*/
    xredisClient *xc = privdata;
    int nwritten = 0, totlen = 0;
    int objlen = 0, objbytes = 0;

    while((xc->bufpos > 0) || (listLength(xc->reply)>0)){
        /*case1: contents in static buffer*/
        if(xc->bufpos > 0){
            if((nwritten = write(fd, xc->buf+xc->sentlen, xc->bufpos-xc->sentlen)) <= 0){
                break;
            }
            xc->sentlen += nwritten;
            totlen  += nwritten;
            /*all contents in buf are sent, reset bufpos and sentlen*/
            if(xc->bufpos == xc->sentlen){
                xc->bufpos = 0;
                xc->sentlen = 0;
            }
        }
        /*case2: contents in reply list*/
        else{
            listNode *ln = listFirst(xc->reply);
            xrobj *xo = listNodeValue(ln);
            objlen = sdslen(xo->ptr);
            objbytes = zmalloc_size_sds(xo->ptr);
            /*corner case: first node holds empty sds*/
            if(objlen == 0){
                listDelNode(xc->reply, ln);
                continue;                 //delete first node, then loop again;
            }
            if((nwritten = write(fd, xo->ptr, objlen)) <= 0){
                break;
            }
            xc->sentlen += nwritten;
            totlen += nwritten;
            /* If we fully sent the object on head go to the next one */
            if(nwritten == objlen){
                listDelNode(xc->reply, ln);
                xc->sentlen = 0;
                xc->reply_bytes = objbytes;
            }
        }
        /*break if total written length greater than XREDIS_MAX_WRITE_PER_EVENT*/
        /*to avoid a huge reply monopolize server*/
        if((totlen > XREDIS_MAX_WRITE_PER_EVENT) && ((server.maxmemory == 0) ||(zmalloc_used_memory() < server.maxmemory))){
            break;
        }
    }
    /*handle write error*/
    if(nwritten == -1){
        if(errno = EAGAIN){
            /*NON_BLOCK case, no data to write*/
            nwritten = 0;
        }
        else{
            xredisLog(XREDIS_VERBOSE, "Error writting to client: %s", strerror(errno));
            freeClient(xc);
            return;
        }
    }
    /*update server's lastinteraction time*/
    if(totlen > 0){
        server.lastinteraction = server.unixtime;
    }
    /*no contents in output buffer now, delete the event from listening scope*/
    if((xc->bufpos == 0) && (listLength(xc->reply) == 0)){
        xc->sentlen = 0;
        aeDeleteFileEvent(server.el, fd, AE_WRITABLE);
        if(xc->flags & XREDIS_CLOSE_AFTER_REPLY){
            freeClient(xc);
        }
    }
}

void resetClient(xredisClient *xc){
    /*reset the client for processing the next command*/
    freeClientArgv(xc);
    xc->reqtype = 0;
    xc->multibulklen = 0;
    xc->bulklen = 0;
    /* We clear the ASKING flag as well if we are not inside a MULTI. */
    if(!(xc->flags & XREDIS_MULTI)){
        xc->flags &= (~XREDIS_ASKING);
    }
}

/*------------buffer manipulation, content analysis--------------*/
int processInlineBuffer(xredisClient *xc){
    /*extract contents from buffer for inline commands*/
    char *newline = strstr(xc->querybuf, "\r\n");
    sds *argv;
    int buflen, argc, i;
    /*corner case: no inline commands*/
    if(newline == NULL){
        if(sdslen(xc->querybuf) > XREDIS_INLINE_MAX_SIZE){
            addReplyError(xc, "Protocol error: too big inline request");
            setProtocolError(xc, 0);
        }
        return XREDIS_ERR;
    }
    /*get command parameters into sds array*/
    buflen = newline - xc->querybuf;
    argv = sdssplitlen(xc->querybuf, buflen, " ", 1, &argc);
    /*clear the used inline contents*/
    xc->querybuf = sdsrange(xc->querybuf, buflen+2, -1);
    /*put getted argv array and argc into xc->argv and xc->argc*/
    if(xc->argv){
        zfree(xc->argv);     //free existed argv
    }
    xc->argv = zmalloc(sizeof(xrobj*)*argc);
    xc->argc = 0;
    for(i=0; i<argc; i++){
        if(argv[i]){
            xc->argv[xc->argc] = createObject(XREDIS_STRING, argv[i]);
            xc->argc++;
        }
        else{
            sdsfree(argv[i]);
        }
    }
    /*free argv*/
    zfree(argv);
    return XREDIS_OK;
}

int processMultibulkBuffer(xredisClient *xc){
    /*turn the contents in xc->querybuf into objects in xc->argv*/
    char *newline = NULL;
    int pos = 0, r;
    long long ll;
    /*multibulklen is 0 means we need to save number of command parameters into xc->multibulklen*/
    if(xc->multibulklen == 0){
        /*the client should have been reset*/
        xredisAssertWithInfo(xc, NULL, xc->argc == 0);
        newline = strchr(xc->querybuf, '\r');
        if(!newline){
            if(sdslen(xc->querybuf) > XREDIS_INLINE_MAX_SIZE){
                addReplyError(xc, "Protocol error: too big mbulk count string");
                setProtocolError(xc, 0);
            }
            return XREDIS_ERR;
        }
        /*ensure '\n' is also in querybuf*/
        if((newline-(xc->querybuf)) > ((signed)sdslen(xc->querybuf)-2)){
            return XREDIS_ERR;
        }
        /* the first char of protocol must be '*' */
        xredisAssertWithInfo(xc, NULL, xc->querybuf[0] == '*');
        /*get the number of command parameters(multibulklen) from protocol*/
        r = string2ll(newline-1, newline-xc->querybuf+1, &ll);
        /*case which multibulk length is too big*/
        if((!r) && (ll > 1024*1024)){
            addReplyError(xc, "Protocol error: invalid multibulk length");
            setProtocolError(xc, pos);
            return XREDIS_ERR;
        }
        /*update pos to first '$'*/
        pos = newline-xc->querybuf+2;
        /*case which ll less than 0, which means the command is null, delete it from querybuf*/
        if(ll <= 0){
            sdsrange(xc->querybuf, pos, -1);
            return XREDIS_OK;
        }
        /*set up client structure's multibulklen property*/
        xc->multibulklen = ll;
        /*set up argv on client structure*/
        if(xc->argv){
            zfree(xc->argv);
        }
        xc->argv = zmalloc(sizeof(xrobj*)*(xc->multibulklen));
    }
    /*avoid special case with error properties*/
    xredisAssertWithInfo(xc, NULL, xc->multibulklen > 0);
    /*read commands from querybuf and save them to xc->argv*/
    while(xc->multibulklen){
        /*bulklen needs to be reset*/
        if(xc->buflen == -1){
            newline = strchr(xc->querybuf+pos, '\r');
            if(newline == NULL){
                if(sdslen(xc->querybuf) > XREDIS_INLINE_MAX_SIZE){
                    addReplyError(xc, "Protocol error: too big count string");
                    setProtocolError(xc, 0);
                }
                break;
            }
            if((newline-(xc->querybuf)) > ((signed)sdslen(xc->querybuf)-2)){
                break;
            }
            /*ensure the format of protocol is right*/
            if(xc->querybuf[pos] != '$'){
                addReplyErrorFormat(xc, "Protocol error: expected $, get '%c'", xc->querybuf[pos]);
                setProtocolError(xc, pos);
                return XREDIS_ERR;
            }
            /*pos to begining of a command parameter*/
            pos += newline-(xc->querybuf+pos)+2;
            xc->bulklen = ll;
        }
        if((sdslen(xc->querybuf)-pos) < (unsigned)(xc->bulklen+2)){
            break;
        }
        else{
            /*create string objects for command parameters*/
            xc->argv[xc->argc++] = createStringObject(xc->querybuf+pos, xc->bulklen);
            /*update pos to next command parameter*/
            pos += (xc->buflen+2);
            /*clear bulklen*/
            xc->bulklen = -1;
            xc->multibulklen--;
        }
    }
    /*delete the taken parameters from xc->querybuf*/
    sdsrange(xc->querybuf, pos, -1);
    if(xc->multibulklen == 0){
        return XREDIS_OK;
    }
    else{
        return XREDIS_ERR;
    }
}

void processInputBuffer(xredisClient *xc){
    /*process the client sent contents in input buffer*/
    while(sdslen(xc->querybuf)){
        /*ensure all contents in querybuf are processed*/
        if(xc->flags & XREDIS_BLOCKED){
            return;
        }
        if(xc->flags & XREDIS_CLOSE_AFTER_REPLY){
            return;
        }
        /*set reqtype by format of protocol*/
        if(xc->reqtype == 0){
            if(xc->querybuf[0] == '*'){
                xc->reqtype = XREDIS_REQ_MULTIBULKLEN;
            } else{
                xc->reqtype = XREDIS_REQ_INLINE;
            }
        }
        /*process buffer contents into argvs*/
        if(xc->reqtype == XREDIS_REQ_MULTIBULKLEN){
            if(processMultibulkBuffer(xc) != XREDIS_OK){
                break;
            }
        } else if(xc->reqtype == XREDIS_REQ_INLINE){
            if(processInlineBuffer(xc) != XREDIS_OK){
                break;
            }
        } else{
            xredisPanic("Unknown request type");
        }
        /*corner case: nothing parsed*/
        if(xc->argc == 0){
            resetClient(xc);
        }
        else{
            if(processCommand(xc) == XREDIS_OK){
                resetClient(xc);
            }
        }
    }
}

/*--------------XREDIS query handler------------*/

void readQueryFromClient(aeEventLoop *el, int fd, void *privdata, int mask){
    /*XREDIS command query handler*/
    xredisClient *xc = (xredisClient *)privdata;
    int qlen = sdslen(xc->querybuf), addlen = XREDIS_IOBUF_LEN, nread;

    server.current_client = xc;
    if(qlen > server.querybuf_peak){
        server.querybuf_peak = qlen;
    }
    xc->querybuf = sdsMakeRoomFor(xc->querybuf, addlen);
    /*read contents from connected fd*/
    if((nread = read(fd, xc->querybuf+qlen, addlen)) < 0){
        if(errno == EAGAIN){
            server.current_client = NULL;
            return;
        } else{
            xredisLog(XREDIS_VERBOSE, "Reading from client: %s", strerror(errno));
            freeClient(xc);
            return;
        }
    } else if(nread == 0){
        xredisLog(XREDIS_VERBOSE, "Client closed connection");
        freeClient(xc);
        return;
    }
    /*read completed, update sds->len*/
    sdsIncrLen(s, nread);
    /*update lastinteraction time*/
    xc->lastinteraction = server.unixtime;
    /*querybuf len cannot exceed server limit*/
    if(sdslen(xc->querybuf) > server.client_max_querybuf_len){
        sds info = getClientInfoString(xc), bytes = sdsempty();
        bytes = sdscatrepr(bytes, xc->querybuf, 64);
        xredisLog(XREDIS_WARNING, "Closing client that reached max query buffer length: %s (qbuf initial bytes: %s)", ci, bytes);
        sdsfree(info);
        sdsfree(bytes);
        freeClient(xc);
        return;
    }
    /*process command*/
    processInputBuffer(xc);
    /*reset current client*/
    server.current_client = NULL;
}

/*-----------------------get client info---------------------*/

void getClientsMaxBuffers(unsigned long *longest_output_list, unsigned long *biggest_input_buffer){
    /*get the biggest output reply list size and querybuf size among all clients*/
    listIter *li = listGetIterator(server.clients, AL_START_HEAD);
    unsigned long long llen=0, blen=0, tmp;
    listNode *ln;
    xredisClient *xc;
    /*iter in the list to get every node client*/
    while((ln = listNext(li))){
        xc = listNodeValue(ln);
        if((tmp = sdslen(xc->querybuf)) > blen){
            blen = tmp;
        }
        if((tmp = listLength(xc->reply)) > llen){
            llen = tmp;
        }
    }

    *longest_output_list = llen;
    *biggest_input_buffer = blen;
    listRelease(li);
}

sds getAllClientInfoString(void){
    /*get and return the info of all clients*/
    listIter *li = listGetIterator(server.clients, AL_START_HEAD);
    xredisClient *xc;
    listNode *ln;
    sds allinfo = sdsempty(), cinfo;
    /*iter in the list to get every client's info*/
    while((ln = listNext(li))){
        xc = listNodeValue(ln);
        cinfo = getClientInfoString(xc);
        allinfo = sdscatlen(allinfo, cinfo, sdslen(cinfo));
        allinfo = sdscatlen(allinfo, "\n", 1);
        sdsfree(cinfo);
    }
    listRelease(li);
    return allinfo;
}

/*---------------CLIENT command realization-------------*/

void clientCommand(xredisClient *xc){
    /*proc function for CLIENT command*/
    /*case1: CLIENT LIST*/
    if((xc->argc == 2) && (strcasecmp(xc->argv[1]->ptr, "LIST") == 0)){
        sds info;
        info = getAllClientInfoString();
        addReplyBulkCBuffer(xc, info, sdslen(info));
        sdsfree(info);
    }
    else if((xc->argc == 3) && (strcasecmp(xc->argv[1]->ptr, "KILL") == 0)){
        listIter *li = listGetIterator(server.clients, AL_START_HEAD);
        listNode *ln;
        char ip[32], addr[64];
        int port;
        /*iterate in the client list*/
        while((ln = listNext(li))){
            if(anetPeerToString(xc->fd, ip, &port) == -1){
                continue;
            }
            snprintf(addr, sizeof(addr), "%s:%d", ip, port);
            if(strcmp(addr, xc->argv[3]->ptr) == 0){
                addReply(xc, shared.ok);
                /*we need to kill current client*/
                if(xc == listNodeValue(ln)){
                    xc->flags |= XREDIS_CLOSE_AFTER_REPLY;
                }
                else{
                    freeClient(xc);
                }
            }
        }
    }
    else{
        addReplyError(xc, "Syntax error, try CLIENT (LIST|KILL ip:port)");
    }
}

/*-----------client struct arguments modification--------------*/

void rewriteClientCommandVector(xredisClient *xc, inr argc, ...){
    /*rewrite argc and argv of client struct*/
    va_list ap;
    xrobj **argv = zmalloc(sizeof(argv*)*argc);
    int i;
    /*save new argvs*/
    va_start(ap, argc);
    for(i=0; i<argc; i++){
        xrobj *tmp;
        tmp = va_arg(ap, xrobj*);
        argv[i] = tmp;
        incrRefCount(argv[i]);
    }
    /*delete old argvs*/
    for(i=0; i<xc->argc; i++){
        decrRefCount(xc->argv[i]);
    }
    zfree(xc->argv);
    /*replace client argv and argc*/
    xc->argc = argc;
    xc->argv = argv;
    /*get new xc->cmd*/
    xc->cmd = lookupCommand(xc->argv[0]->ptr);
    xredisAssertWithInfo(xc, NULL, xc->cmd != NULL);
    va_end(ap);
}

void rewriteClientCommandArgument(xredisClient *xc, int i, xrobj *newval){
    /*rewrite the command parameter specified by index i*/
    xredisAssertWithInfo(xc, NULL, xc->argc > i);
    decrRefCount(xc->argv[i]);
    xc->argv[i] = newval;
    incrRefCount(xc->argv[i]);
    /*update cmd if needed*/
    if(i == 0){
        xc->cmd = lookupCommand(xc->argv[0]->ptr);
    }
    xredisAssertWithInfo(xc, NULL, xc->cmd != NULL);
}

/*----------client output buffer checking----------*/

int checkClientOutputBufferLimits(xredisClient *xc){
    /*check whether client output buffer size reaches soft or hard limit*/
    int soft=0, hard=0;
    unsigned long bsize = getClientOutputBufferMemoryUsage(xc);
    /*set soft if bszie exceed soft limit*/
    if(bsize > server.client_obuf_limits.soft_limit_bytes){
        soft = 1;
    }
    /*set hard if bsize exceed hard limit*/
    if(bsize > server.client_obuf_limits.hard_limit_bytes){
        hard = 1;
    }
    if(soft){
        if(xc->obuf_soft_limit_reached_time == 0){
            xc->obuf_soft_limit_reached_time = server.unixtime;
            soft = 0;
        }
        else{
            time_t tpass = server.unixtime - xc->obuf_soft_limit_reached_time;
            if(tpass <= server.client_obuf_limits.soft_limit_seconds){
                soft = 0;
            }
        }
    }
    /*clear soft reached time if client left or never enter into soft limit size*/
    else{
        xc->obuf_soft_limit_reached_time = 0;
    }
    return (soft || hard);
}

void asyncCloseClientOnOutputBufferLimitReached(xredisClient *xc){
    /*close the client asynchronously if the output buffer size limit is reached*/
    xredisAssert(xc->reply_bytes < (ULONG_MAX - 1024*64));
    /*return if no content in output buffer or async close was setted*/
    if((xc->reply_bytes == 0) || (xc->flags & XREDIS_CLOSE_ASAP)){
        return;
    }
    if(checkClientOutputBufferLimits(xc)){
        sds client = getClientInfoString(xc);
        freeClientAsync(xc);         //close the client asynchronously;
        xredisLog(XREDIS_WARNING, "Client %s scheduled to be close ASAP for overcoming of output buffer limits.", client);
        sdsfree(client);
    }
}
