#include "xredis.h"
#include "bio.h"
#include "xrio.h"

#include <signal.h>
#include <fcntl.h>
#include <sys/stat.h>
#include <sys/types.h>
#include <sys/time.h>
#include <sys/resource.h>
#include <sys/wait.h>

/*PART1: AOF rewrite buffer manipulation functions*/

int aofUpdateCurrentSize(void){
    /*set AOF current size by the size of AOF file*/
    struct xredis_stat sb;

    if(xredis_fstat(server.aof_fd, &sb) == -1){
        xredisLog(XREDIS_WARNING,"Unable to obtain the AOF file length. stat: %s",strerror(errno));
        return -1;
    }
    server.aof_current_size = sb.st_size;
    return 0;
}

/*Build AOF buffer data structure, here we use a list composite with blocks of size AOF_RW_BUF_BLOCK_SIZE*/
#define AOF_RW_BUF_BLOCK_SIZE   (1024*1024*10)      /*10MB per block*/

typedef struct aofrwblock{
     unsigned long used,      //used bytes
                   free;      //free bytes
     char buf[AOF_RW_BUF_BLOCK_SIZE];
} aofrwblock;

void aofRewriteBufferReset(void){
    /*release old buffer list if it exists, create new buffer list*/
    if(server.aof_rewrite_buf_blocks){
        listRelease(server.aof_rewrite_buf_blocks);
    }
    server.aof_rewrite_buf_blocks = listCreate();
    listSetFreeMethod(server.aof_rewrite_buf_blocks, zfree);
}

unsigned long aofRewriteBufferSize(void){
    /*return the contents size of current AOF rewrite buffer*/
    listNode *last;
    unsigned long lastused, size;

    if((last = listLast(server.aof_rewrite_buf_blocks)) == NULL){
         return 0;
    }
    lastused = (last->value)->used;
    size = (listLength(server.aof_rewrite_buf_blocks)-1)*AOF_RW_BUF_BLOCK_SIZE + lastused;

    return size;
}

void aofRewriteBufferAppend(unsigned char *s, unsigned long len){
    /*add string s at the end of AOF buffer, assign a new block if needed*/
    listNode *last;
    aofrwblock *block;
    unsigned int clen;
    int counts;

    last = listLast(server.aof_rewrite_buf_blocks);
    if(last){
        block = last->value;
    }
    else{
        block = NULL;
    }

    while(len > 0){
        if(block){
            clen = (block->free < len)? block->free: len;
            memcpy(block->buf, s, clen);
            block->free -= clen;
            block->used += clen;
            s += clen;
            len -= clen;
        }
        if(len){
            block = zmalloc(sizeof(aofrwblock));
            block->free = AOF_RW_BUF_BLOCK_SIZE;
            block->used = 0;
            listAddNodeTail(server.aof_rewrite_buf_blocks, block);
            /*NOTICE when node number is coming to n*10, WARNING when n*100*/
            counts = listLength(server.aof_rewrite_buf_blocks);
            if(((counts+1) % 100) == 0){
                xredisLog(XREDIS_WARNING, "Background AOF buffer size: %lu MB", aofRewriteBufferSize()/(1024*1024));
            }
            else if(((counts+1)%10) == 0){
                xredisLog(XREDIS_NOTICE, "Background AOF buffer size: %lu MB", aofRewriteBufferSize()/(1024*1024));
            }
        }
    }
}

ssize_t aofRewriteBufferWrite(int fd){
    /*write cotents in AOF rewrite buffer blocks into given file*/
    /*return -1 for short count or write error, else return bytes written*/
    listIter *li;
    listNode *ln;
    aofrwblock *block;
    int nwritten, nbytes=0;

    li = listGetIterator(server.aof_rewrite_buf_blocks, AL_START_HEAD);
    while((ln = listNext(li)) != NULL){
        block = listNodeValue(ln);
        if((nwritten = write(fd, block->buf, block->used)) == -1){
            return -1;
        }
        else if((nwritten == 0) && (block.used != 0)){
            errno = EIO;
            return 0;
        }
        nbytes += nwritten;
    }

    listReleaseIterator(li);
    return nbytes;
}

/*PART2 : AOF file manipulation functions*/

void aof_background_fsync(int fd) {
     /* Starts a background task that performs fsync() against the specified
      * file descriptor (the one of the AOF file) in another thread. */
        bioCreateBackgroundJob(XREDIS_BIO_AOF_FSYNC,(void*)(long)fd,NULL,NULL);
}

void stopAppendOnly(void){
    /*Called when the user switches from "appendonly yes" to "appendonly no"at runtime using the CONFIG command.*/
    assert(server.aof_state == XREDIS_AOF_OFF);
    /*force buffer contents to AOF file*/
    flushAppendOnlyFile(1);
    /*wait until disk writting is finished*/
    aof_fsync(server.aof_fd);
    /*set aof state*/
    server.aof_fd = -1;
    server.aof_selected_db = -1;
    server.aof_state = XREDIS_AOF_OFF;
    /*rewrite operation in progress? kill it, wait child exit*/
    if(server.aof_child_pid != -1){
        int statloc;

        xredisLog(XREDIS_NOTICE, "killing running AOF rewrite child: %ld", (long)server.aof_child_pid);
        if(kill(server.aof_child_pid, SIGKILL) != -1){
            wait3(&statloc, 0, NULL);
        }
        /* reset the buffer accumulating changes while the child saves */
        aofRewriteBufferReset();        //release rewrite buffer;
        aofRemoveTempFile(server.aof_child_pid);   //remove temp file;
        /*change server state*/
        server.aof_child_pid = -1;
        server.aof_rewrite_time_start = -1;
    }
}

int startAppendOnly(void){
    /*Called when the user switches from "appendonly no" to "appendonly yes", at runtime using the CONFIG command.*/
    server.aof_last_fsync = server.unixtime;
    /*Open AOF file*/
    if((server.aof_fd = open(server.aof_filename, O_WRONLY|O_APPEND|O_CREAT, 0644)) == -1){
        xredisLog(XREDIS_WARNING, "xredis needs to enable the AOF but can't open the append only file: %s",strerror(errno));
        return XREDIS_ERR;
    }
    xredisAssert(server.aof_state == XREDIS_AOF_OFF);
    /*start aof Background rewrite*/
    if(rewriteAppendOnlyFileBackground() == XREDIS_ERR){
        close(server.aof_fd);
        xredisLog(XREDIS_WARNING,"Xredis needs to enable the AOF but can't trigger a background AOF rewrite operation. Check the above logs for more info about the error.");
        return XREDIS_ERR;
    }
    /*set AOF state as WAIT_REWRITE*/
    server.aof_state = XREDIS_AOF_WAIT_REWRITE;
    return XREDIS_OK;
}

void flushAppendOnlyFile(int force){
    /*write contents in AOF buffer into AOF file and sync it to disk*/
    ssize_t nwritten;
    int sync_in_progress = 0;
    /*corner case which AOF buffer is empty*/
    if(sdslen(server.aof_buf) == 0){
        return;
    }
    /*if appendfsync set as everysec, get the number of pending fsync jobs*/
    if(server.aof_fsync == AOF_FSYNC_EVERYSEC){
        sync_in_progress = bioPendingJobOfType(REDIS_BIO_AOF_FSYNC) != 0;
    }
    /*case1: appendfsync set as everysec*/
    if((server.aof_fsync == AOF_FSYNC_EVERYSEC) && (force != 1)){
        /*There is tasks pending in fsync queue*/
        if(sync_in_progress != 0){
            /*if the buffer never been flush postponed before, set the postponed start time*/
            if(server.aof_flush_postponed_start == 0){
                server.aof_flush_postponed_start = server.unixtime;
                return;
            }
            /*we allow flush be postponed within 2 seconds*/
            if(server.unixtime - server.aof_flush_postponed_start < 2){
                 return;
            }
            server.aof_delayed_fsync++;
            xredisLog(XREDIS_NOTICE,"Asynchronous AOF fsync is taking too long (disk is busy?).
                    Writing the AOF buffer without waiting for fsync to complete, this may slow down xredis.");
        }
    }
    /*flush cannot be postponed anymore, we need to start to write*/
    server.aof_flush_postponed_start = 0;     //reset postponed start time;
    /*write AOF buffer into AOF file*/
    nwritten = write(server.fd, server.aof_buf, sdslen(server.aof_buf));
    /*check nwritten*/
    if(nwritten == -1){
        xredisLog(XREDIS_WARNING,"Exiting on error writing to the append-only file: %s",strerror(errno));
        exit(1);
    }
    else if(nwritten < sdslen(server.aof_buf)){
        xredisLog(XREDIS_WARNING,"Exiting on short write while writing to the append-only file: %s (nwritten=%ld, expected=%ld)",
                strerror(errno), (long)nwritten, (long)sdslen(server.aof_buf));
        if(ftruncate(server.aof_fd, server.aof_current_size) == -1){
            xredisLog(XREDIS_WARNING, "Could not remove short write from the append-only file.
                    xredis may refuse to load the AOF the next time it starts. ftruncate: %s", strerror(errno));
        }
        exit(1);
    }
    /*update AOF current size*/
    server.aof_current_size += nwritten;
    /*reset AOF buffer*/
    if((sdslen(server.aof_buf)+sdsavail(server.aof_buf)) < 4000){
         sdsclear(server.aof_buf);
    }
    /*buffer less than 4000 bytes, we will reuse it, else, we will free and recreate*/
    else{
        sdsfree(server.aof_buf);
        server.aof_buf = sdsempty();
    }
    /*the writting process finished, start fsync to disk*/
    /*if we set server.aof_no_fsync_on_rewrite, then we cannot fsync along BGSAVE/REWRITE is running*/
    if((server.aof_no_fsync_on_rewrite) && ((server.rdb_child_pid != -1)||(server.aof_child_pid != -1))){
        return;
    }
    /*fsync if necessary*/
    if(server.aof_fsync == AOF_FSYNC_EVERYSEC){
        if(server.unixtime - server.aof_last_fsync >= 1){
            /*only fsync when no sync waiting in background thread*/
            if(!sync_in_progress){
                aof_background_fsync(server.aof_fd);
            }
            server.aof_last_fsync = server.unixtime;
        }
    }
    else if(server.aof_fsync == AOF_FSYNC_ALWAYS){
        aof_fsync(server.aof_fd);
        server.aof_last_fsync = server.unixtime;
    }
}

sds catAppendOnlyGenericCommand(sds dst, int argc, xrobj **argv){
    /*turn the commands series into string proctols*/
    char buf[32];
    long long len;
    int i;
    xrobj *xo;
    /*save the command counting part in"*argc\r\n"*/
    buf[0] = '*';
    len = ll2string(buf+1, sizeof(buf)-1, (long long)argv);
    buf[len++] = '\r';
    buf[len++] = '\n';
    dst = sdscatlen(dst, buf, len);
    /*save commands part, "$len\r\nCMD\r\n"*/
    for(i=0; i<argc; i++){
        xo = getDecodedObject(argv[i]);
        /*write the "$len\r\n" part*/
        buf[0] = '$';
        len = ll2string(buf+1, sizeof(buf)-1, sdslen(xo->ptr));
        buf[len++] = '\r';
        buf[len++] = '\n';
        dst = sdscatlen(dst, buf, len);
        /*write the "CMD\r\n" part*/
        dst = sdscatlen(dst, xo->ptr, sdslen(xo->ptr));
        dst = sdscatlen(dst, "\r\n", 2);
    }
    return dst;
}

sds catAppendOnlyExpireAtCommand(sds buf, struct xredisCommand *cmd, xrobj *key, xrobj *seconds){
    /*turn command EXPIRE,PEXPIRE to PEXPIREAT as writting command string proctols, and write it to buf*/
    long long t;
    int argc;
    xrobj *argv[3];
    xrobj *nseconds;
    char *endptr;
    /*get the input time*/
    nseconds = getDecodedObject(seconds);
    t = strtoll(nseconds->ptr, &endptr, 10);
    xredisAssert(*endptr == '\0');
    /*turn time into miliseconds time stamp*/
    /*cases: expire/pexpire/expireat/setex/psetex */
    if(cmd->proc == expireCommand){
        t = t*1000+mstime();
    }
    else if(cmd->proc == pexpireCommand){
        t += mstime();
    }
    else if(cmd->proc == expireatCommand){
        t = t*1000;
    }
    else if(cmd->proc == setexCommand){
        t = t*1000+mstime();
    }
    else if(cmd->proc == psetexCommand){
        t += mstime();
    }
    /*clear unuse xobj*/
    decrRefCount(nseconds);
    decrRefCount(seconds);
    /*generatre argv with PEXPIREAT KEY WHEN*/
    argv[0] = createObject(XREDIS_STRING, "PEXPIREAT");
    argv[1] = key;
    argv[2] = createStringObjectFromLongLong(t);
    argc = 3;
    /*turn argvs into string proctols and save to buf*/
    buf = catAppendOnlyGenericCommand(buf, 3, argv);

    return buf;
}

void feedAppendOnlyFile(struct xredisCommand *cmd, int dictid, xrobj **argv, int argc){
    /*add given commands to given AOF file/buffer */
    sds buf = sdsempty();
    /*add SELECT command if db mismatch*/
    if(dictid != server.aof_selected_db){
        char addsele[64];
        snprintf(addsele, sizeof(addsele), "%s", dictid);
        buf = sdscatprintf(buf, "*2\r\n$6\r\nSELECT\r\n$%d\r\n%s\r\n", (unsigned long)strlen(addsele), addsele);
        server.aof_selected_db = dictid;
    }
    /*turn EXPIRE/EXPIREAT/PEXPIRE commands into PEXPIREAT*/
    if((cmd->proc == expireCommand)||(cmd->proc == expireatCommand)||(cmd->proc == pexpireCommand)){
        buf = catAppendOnlyExpireAtCommand(buf, cmd, argv[1], argv[2]);
    }
    /*turn SETEX/PSETEX commands into SET & PEXPIREAT*/
    else if((cmd->proc == setexCommand)||(cmd->proc == psetexCommand)){
        xrobj *xo = createStringObject("SET", 3);
        xrobj *tmpargv[3] = {xo, argv[1], argv[3]};
        buf = catAppendOnlyGenericCommand(buf, 3, tmpargv);
        buf = catAppendOnlyExpireAtCommand(buf, cmd, argv[1], argv[2]);
        /*Release the created xo*/
        decrRefCount(xo);
    }
    else{
        buf = catAppendOnlyGenericCommand(buf, argc, argv);
    }
    /*add the buf contents to AOF buffer if AOF is on*/
    if(server.aof_state == XREDIS_AOF_ON){
        server.aof_buf = sdscatlen(sever.aof_buf, buf, sdslen(buf));
    }
    /*add the buf contents to AOF rewrote buffer if AOF rewrite is on*/
    if(server.aof_child_pid != -1){
        aofRewriteBufferAppend((unsigned char *)buf, sdslen(buf));
    }
}

/*PART3: AOF file loading*/

/*create a fake client without net connection*/
struct xredisClient *createFakeClient(void){
    struct xredisClient *xc = zmalloc(sizeof(struct xredisClient));
    /*turn the client current db to No.0*/
    selecDb(xc, 0);
    /*set client properties*/
    xc->fd = -1;
    xc->querybuf = sdsempty();
    xc->querybuf_peak = 0;
    xc->argc = 0;
    xc->argv = NULL;
    xc->bufpos = 0;
    xc->flags = 0;
    xc->reply = listCreate();
    xc->reply_bytes = 0;
    xc->obuf_soft_limit_reached_time = 0;
    xc->watched_keys = listCreate();
    listSetFreeMethod(xc->reply, decrRefCount);
    listSetDupMethod(xc->reply, dupClientReplyValue);
    initClientMultiState(xc);

    return xc;
}

void freeFakeClient(struct xredisClient *xc){
    /*Release fake client*/
    sdsfree(xc->querybuf);
    listRelease(xc->reply);
    listRelease(xc->watched_keys);
    freeClientMultiState(xc);
    /*free the fake client struct*/
    zfree(xc);
}

int loadAppendOnlyFile(char *filename){
    /*load AOF file and execute it*/
    struct xredisClient *fakeClient;
    FILE *fp = fopen(filename, "r");
    struct xredis_stat sb;
    int old_aof_state = server.aof_state;
    long loops = 0;

    /*corner cases*/
    /*case1: AOF file exists, but it is empty*/
    if(fp && (xredis_fstat(filename, &sb) != -1) && (sb.st_size == 0)){
        server.aof_current_size = 0;
        close(fp);
        return XREDIS_ERR;
    }
    /*case2: error while opening AOF file*/
    if(!fp){
        xredisLog(XREDIS_WARNING, "Fatal error: can't open the append only file for reading :%s", strerr(errno));
        exit(1);
    }
    /*loading start*/
    fakeClient = createFakeClient();
    startLoading(fp);              //from rdb.c, update server loading state;
    /*read AOF commands and exec*/
    while(1){
        int argc, j;
        unsigned long len;
        xrobj **argv;
        char buf[128];
        sds argsds;
        struct xredisCommand *cmd;
        /*every 1000 loops handle client one time*/
        if((++loop)%1000 == 0){
            loadingProgress(ftello(fp));
            aeProcessEvents(server.el, AE_FILE_EVENT|AE_DONT_WAIT);
        }
        /*read AOF file to buf by whole line*/
        if(fgets(buf, sizeof(buf), fp) == NULL){
            if(feof(fp)){
                break;
            }
            else{
                goto readerr;
            }
        }
        /*get the commands and parameters out*/
        if(buf[0] != '*') goto fmterr;
        argc = atoi(buf+1);
        if(argc < 1) goto fmterr;
        /*use argv array to hold all commands and parameters*/
        argv = zmalloc(sizeof(*argv)*argc);
        for(j=0; j<argc; j++){
            if(fgets(buf,sizeof(buf), fp) == NULL){
                goto readerr;
            }
            if(buf[0] != '$'){
                goto fmterr;
            }
            len = strtol(buf+1, NULL, 10);
            argsds = sdsnewlen(NULL, len);
            if(len && (fread(argsds, len, 1, fp) == 0)){
                goto fmterr;
            }
            argv[j] = createObject(XREDIS_STRING, argsds);
            if(fread(buf, 2, 1, fp) == 0){
                goto fmterr;
            }
        }
        /*find in command table*/
        if((cmd = lookupCommand(argv[0]->ptr)) == NULL){
            xredisLog(XREDIS_WARNING, "Unknown Command '%s' reading the append only file", argv[0]->ptr);
            exit(1);
        }
        /*execute commands in the context of fake client*/
        fakeClient->argc = argc;
        fakeClient->argv = argv;
        cmd->proc(fakeClient);
        /*the fake client should not have a reply*/
        xredisAssert((fakeClient->bufpos == 0) && (listLength(fakeClient->reply) == 0));
        /*the fake client should never get blocked*/
        xredisAssert((fakeClient->flags & XREDIS_BLOCKED));
        /*decrRefCount of created objects and release the created array*/
        for(j=0; j<argc; j++){
            decrRefCount(argv[j]);
        }
        zfree(argv);
    }
    /*clear and update server properties*/
    fclose(fp);
    freeFakeClient(fakeClient);          //release the created fake client;
    server.aof_state = old_aof_state;
    stopLoading();                       //in rdb.c: set loading status as finished;
    aofUpdateCurrentSize();              //update server's aof_current_size by AOF file current size;
    server.aof_rewrite_base_size = server.aof_current_size;

    return XREDIS_OK;
/*read error, use when fread fails*/
readerr:
    if(feof(fp)){
        xredisLog(XREDIS_WARNING, "Unexpected end of file reading the append only file");
    }
    else{
        xredisLog(XREDIS_WARNING, "Unrecoverable error reading the append only file: %s", strerror(errno));
    }
    exit(1);
/*format error, use when proctol string format is wrong*/
fmterr:
    xredisLog(XREDIS_WARNING, "Bad file format reading the append only file");
    exit(1);
}

/*PART4: AOF rewrite*/

int xrioWriteBulkObject(xrio *x, xrobj *obj){
    /*write string or long long holds by a string object into r*/
    if(obj->encoding == XREDIS_ENCODING_RAW){
        return xrioWriteBulkString(x, obj->ptr, sdslen(obj->ptr));
    }
    else if(obj->encoding == XREDIS_ENCODING_INT){
        return xrioWriteBulkLongLong(x, (long long)obj->ptr);
    }
    else{
        xredisPanic("Unknown string encoding");
    }
}

int rewriteStringObject(xrio *x, xrobj *key, xrobj *val){
    /*write the commands for rewriting set string object to x*/
    /*return 1 if successed, return 0 if failed*/
    char cmds[] = "*3\r\n$3\r\nSET\r\n";

    if(xrioWrite(x, cmds, strlen(cmds)) == 0){
        return 0;
    }
    if(xrioWriteBulkObject(x, key) == 0){
        return 0;
    }
    if(xrioWriteBulkObject(x, val) == 0){
        return 0;
    }
    return 1;
}

int rewriteListObject(xrio *x, xrobj *key, xrobj *val){
    /*write the commands for rewriting rpush list object to x*/
    /*return 1 if successed, return 0 if failed*/
    long long items = listTypeLength(x); //we set items as long long so it can cover listTypeLength's unsigned long return;
    long long i = 0;

    while(items > 0){
        int len;
        /*use multi-commands to avoid long command make client output buffer overflow*/
        if(items > XREDIS_AOF_REWRITE_ITEMS_PER_CMD){
            len = XREDIS_AOF_REWRITE_ITEMS_PER_CMD;
            items -= XREDIS_AOF_REWRITE_ITEMS_PER_CMD;
        }
        else{
            len = items;
            items -= XREDIS_AOF_REWRITE_ITEMS_PER_CMD;
        }
        /*rewrite the key-value pair*/
        /*write the *len\r\n part*/
        if(xrioWriteBulkCount(x, '*', len+2) == 0){
            return 0;
        }
        /*write the RPUSH command*/
        if(xrioWriteBulkString(x, "RPUSH", 5) == 0){
            return 0;
        }
        /*write the key*/
        if(xrioWriteBulkObject(x, key) == 0){
            return 0;
        }
        /*write the list elements iteratlly*/
        if(val->encoding == XREDIS_ENCODING_LINKEDLIST){
            /*fetch and write adlist node values with an iterator*/
            listIter *li;
            listNode *ln;

            if(i == 0){
                /*only rewind iterator at the first iteration*/
                li = listGetIterator(val->ptr, AL_START_HEAD);
                i = 1;
            }

            while((ln = listNext(li)) && (len--)){
                if(xrioWriteBulkObject(x, listNodeValue(ln)) == 0){
                    return 0;
                }
            }
            if(items <= 0){
                /*only release the iterator at the last iteration*/
                listReleaseIterator(li);
            }
        }
        else if(val->encoding == XREDIS_ENCODING_ZIPLIST){
            /*fetch and write ziplist node values*/
            unsigned char *zp = ziplistIndex(val->ptr, 0);
            unsigned char *sstr;
            unsigned int slen;
            long long sval;

            if(i == 0){
                zp = ziplistIndex(val->ptr, 0);
                i = 1;
            }

            while((zp) && (len--)){
                if(ziplistGet(val->ptr, &sstr, &slen, &sval) == 0){
                    return 0
                }
                if(sstr){
                    if(xrioWriteBulkString(x, (char *)sstr, slen) == 0){
                        return 0;
                    }
                }
                else{
                    if(xrioWriteBulkLongLong(x, sval) == 0){
                        return 0;
                    }
                }
                zp = ziplistNext(val->ptr, zp);
            }
        }
        else{
            xredisPanic("Unknown list encoding");
        }
    }

    return 1;
}

static int xrioWriteHashIteratorCursor(xrio *x, hashTypeIterator *hi, int what){
    /*write key or value into given xrio, return 0 if fails*/
    /*what can be XREDIS_HASH_KEY or XREDIS_HASH_VALUE, let you choose write key or value into xrio by set it*/
    if(hi->encoding == XREDIS_ENCODING_HT){
        xrobj *xo;
        hashTypeCurrentFromHashTable(hi, what, &xo);
        return xrioWriteBulkObject(x, xo);
    }
    else if(hi->encoding == XREDIS_ENCODING_ZIPLIST){
        unsigned char *vstr = NULL;
        unsigned int vlen = UINT_MAX;
        long long vll = LLONG_MAX;

        hashTypeCurrentFromZiplist(hi, what, &vstr, &vlen, &vll);
        if(vstr){
            return xrioWriteBulkString(x, vstr, vlen);
        }
        else{
            return xrioWriteBulkLongLong(x, vll);
        }
    }
    else{
        xredisPanic("Unknown hash encoding");
    }
}

int rewriteHashObject(xrio *x, xrobj *key, xrobj *val){
    /*write the commands for rewriting HMSET hash object to x*/
    /*return 1 if successes, return 0 if fails*/
    long long items = hashTypeLength(val);       //set long long type to cover unsigned long type;
    hashTypeIterator *hi = hashTypeInitIterator(val);
    while(items > 0){
        int len;
        /*for hash, REWRITE_ITEMS_PER_CMD points to key-value pairs number*/
        if(items > XREDIS_AOF_REWRITE_ITEMS_PER_CMD){
            len = XREDIS_AOF_REWRITE_ITEMS_PER_CMD;
            items -= XREDIS_AOF_REWRITE_ITEMS_PER_CMD;
        }
        else{
            len = items;
            items -= XREDIS_AOF_REWRITE_ITEMS_PER_CMD;
        }
        /*write the *len\r\n part*/
        if(xrioWriteBulkCount(x, '*', len*2+2) == 0){
            return 0;
        }
        /*write the HMSET command*/
        if(xrioWriteBulkString(x, "HMSET", 5) == 0){
            return 0;
        }
        /*write the key*/
        if(xrioWriteBulkObject(x, key) == 0){
            return 0;
        }
        /*use hash iterator cursor defined before to write key and val iteratlly*/
        while((len--) && (hashTypeNext(hi) != XREDIS_ERR)){
            if(xrioWriteHashIteratorCursor(x, hi, XREDIS_HASH_KEY) == 0){
                return 0;
            }
            if(xrioWriteHashIteratorCursor(x, hi, XREDIS_HASH_VALUE) == 0){
                return 0;
            }
        }
    }
    hashTypeReleaseIterator(hi);
    return 1;
}

int rewriteSetObject(xrio *x, xrobj *key, xrobj *val){
    /*write the commands for rewritting SADD set object to x*/
    /*return 1 if successes, return 0 if fails*/
    long long items = setTypeSize(val), count = 0;

    if(val->encoding == XREDIS_ENCODING_INTSET){
        int pos = 0, len;
        long long setele;

        while(intsetGet(val->ptr, pos++, &setele)){
            if(count == 0){
                len = (items>XREDIS_AOF_REWRITE_ITEMS_PER_CMD) ? XREDIS_AOF_REWRITE_ITEMS_PER_CMD : items;
                items -= XREDIS_AOF_REWRITE_ITEMS_PER_CMD;

                if(xrioWriteBulkCount(x, '*', len+2) == 0){
                    return 0;
                }
                if(xrioWriteBulkString(x, "SADD", 4) == 0){
                    return 0;
                }
                if(xrioWriteBulkObject(x, key) == 0){
                    return 0;
                }
            }
            if(xrioWriteBulkLongLong(x, setele) == 0){
                return 0;
            }
            if((++count) == XREDIS_AOF_REWRITE_ITEMS_PER_CMD){
                count = 0;
            }
        }
    }
    else if(val->encoding == XREDIS_ENCODING_HT){
        dictIterator *di = dictGetIterator(val->ptr);
        dictEntry *de;
        xrobj *eleobj;
        int len

        while((de = dictNext(di)) != NULL){
            if(count == 0){
                len = (items>XREDIS_AOF_REWRITE_ITEMS_PER_CMD) ? XREDIS_AOF_REWRITE_ITEMS_PER_CMD : items;
                items -= XREDIS_AOF_REWRITE_ITEMS_PER_CMD;

                if(xrioWriteBulkCount(x, '*', len+2) == 0){
                    return 0;
                }
                if(xrioWriteBulkString(x, "SADD", 4) == 0){
                    return 0;
                }
                if(xrioWriteBulkObject(x, key) == 0){
                    return 0;
                }
            }
            eleobj = dictGetKey(de);
            if(xrioWriteBulkObject(x, eleobj) == 0){
                return 0;
            }
            if((++count) == XREDIS_AOF_REWRITE_ITEMS_PER_CMD){
                count = 0;
            }
        }
        dictReleaseIterator(di);
    }
    else{
        xredisPanic("Unknown set encoding");
    }

    return 1;
}

int rewriteSortedSetObject(xrio *x, xrobj *key, xrobj *val){
    /*write the commands for rewritting ZADD set object to x*/
    /*return 1 if successes, return 0 if fails*/
    long long items = zsetLength(val), count = 0;

    if(val->encoding == XREDIS_ENCODING_ZIPLIST){
        unsigned char *eptr, *sptr;
        unsigned char *vstr;
        unsigned int vlen;
        long long vll;
        double score;
        int len;

        eptr = ziplistIndex(val->ptr, 0);
        xredisAssert(eptr != NULL);
        sptr = ziplistNext(val->ptr, eptr);
        xredisAssert(sptr != NULL);

        while(eptr != NULL){
            xredisAssert(ziplistGet(eptr, &vstr, &vlen, &vll));
            score = zzlGetScore(sptr);

            if(count == 0){
                len = (items > XREDIS_AOF_REWRITE_ITEMS_PER_CMD) ? XREDIS_AOF_REWRITE_ITEMS_PER_CMD : items;
                items -= XREDIS_AOF_REWRITE_ITEMS_PER_CMD;

                if(xrioWriteBulkCount(x, '*', len*2+2) == 0){
                    return 0;
                }
                if(xrioWriteBulkString(x, "ZADD", 4) == 0){
                    return 0;
                }
                if(xrioWriteBulkObject(x, key) == 0){
                    return 0;
                }
            }
            if(xrioWriteBulkDouble(x, score) == 0){
                return 0;
            }
            if(vstr){
                if(xrioWriteBulkString(x, (char *)vstr, vlen) == 0){
                    return 0;
                }
            }
            else{
                if(xrioWriteBulkLongLong(x, vll) == 0){
                    return 0;
                }
            }
            zzlNext(val->ptr, &eptr, &sptr);
            if((++count) == XREDIS_AOF_REWRITE_ITEMS_PER_CMD){
                 count = 0;
            }
        }
    }
    else if(val->encoding == XREDIS_ENCODING_SKIPLIST){
        zset *zs = val->ptr;
        dictIterator *di = dictGetIterator(zs->dict);
        dictEntry *de;
        xrobj *eleobj;
        double *score;

        while(de = dictNext(di)){
            eleobj = dictGetKey(de);
            score = dictGetVal(de);

            if(count == 0){
                len = (items > XREDIS_AOF_REWRITE_ITEMS_PER_CMD) ? XREDIS_AOF_REWRITE_ITEMS_PER_CMD : items;
                items -= XREDIS_AOF_REWRITE_ITEMS_PER_CMD;

                if(xrioWriteBulkCount(x, '*', len*2+2) == 0){
                    return 0;
                }
                if(xrioWriteBulkString(x, "ZADD", 4) == 0){
                    return 0;
                }
                if(xrioWriteBulkObject(x, key) == 0){
                    return 0;
                }
            }
            if(xrioWriteBulkDouble(x, *score) == 0){
                return 0;
            }
            if(xrioWriteBulkObject(x, eleobj) == 0){
                return 0;
            }
            if((++count) == XREDIS_AOF_REWRITE_ITEMS_PER_CMD){
                count = 0;
            }
        }
        dictReleaseIterator(di);
    }
    else{
        xredisPanic("Unknown zset encoding");
    }

    return 1;
}

int rewriteAppendOnlyFile(char *filename){
    /*rewrite a new aof file for the whole dataset*/
    /*return XREDIS_OK if successes, return XREDIS_ERR if error*/
    dictIterator *di;
    dictEntry  *de;
    dict *d;
    xrio aof;
    FILE *fp;
    char tmpfile[256];
    int j;
    long long now = mstime();

    /*set a tmpfile name string which takes "temp-rewriteaof-<pid>.aof" form*/
    snprintf(tmpfile, 256, "temp-rewriteaof-%d.aof", (int)getpid());
    /*PART 1, create a new AOF file*/
    fp = fopen(tmpfile, "w");
    if(!fp){
        xredisLog(XREDIS_WARNING, "Opening the temp file for AOF rewrite in rewriteAppendOnlyFile(): %s", strerror(errno));
        return XREDIS_ERR;
    }
    /*initialize file flow*/
    xrioInitWithFile(&aof, fp);
    /*PART2, traverse the databases*/
    for(j=0; j<server.dbnum; j++){
        /*ignore empty db*/
        if(dictSize(server.db[j]) == 0){
            continue;
        }
        d = (server.db[j])->dict;
        /*write SELECT command*/
        char selectcmd[] = "*2\r\n$6\r\nSELECT\r\n";
        if(xrioWrite(x, selectcmd, strlen(selectcmd)) == 0){
            return 0;
        }
        if(xrioWriteBulkLongLong(x, j) == 0){
            return 0;
        }
        /*create dict iterator and traverse the database*/
        if(!(di = dictGetSafeIterator(d))){
            fclose(fp);
            return XREDIS_ERR;
        }
        while(de = dictNext(di)){
            sds keysds = dictGetKey(de);
            xrobj *val = dictGetVal(de), *key;
            initStaticStringObject(key, keysds);
            long long expiretime = getExpire(server.db[j], &key);
            /*ignore expired keys*/
            if((expiretime !=-1) && (expiretime < now)){
                continue;
            }
            /*rewrite the key-value pair*/
            if(val->type == XREDIS_STRING){
                if(rewriteStringObject(x, key, val) == 0){
                    goto werr;
                }
            }
            else if(val->type == XREDIS_LIST){
                if(rewriteListObject(x, key, val) == 0){
                    goto werr;
                }
            }
            else if(val->type == XREDIS_HASH){
                if(rewriteHashObject(x, key, val) == 0){
                    goto werr;
                }
            }
            else if(val->type == XREDIS_SET){
                if(rewriteSetObject(x, key, val) == 0){
                    goto werr;
                }
            }
            else if(val->type == XREDIS_ZSET){
                if(rewriteZsetObject(x, key, val) == 0){
                    goto werr;
                }
            }
            /*rewrite the PEXPIREAT KEY when commands if expiretime setting is needed*/
            if(expiretime != -1){
                if(xrioWrite(x, "PEXPIREAT", 9) == 0){
                    goto werr;
                }
                if(xrioWriteBulkObject(x, key) == 0){
                    goto werr;
                }
                if(xrioWriteBulkLongLong(x, expiretime) == 0){
                    goto werr;
                }
            }
        }
        dictReleaseIterator(di);
    }
    /*NOTE: flush buffer, to ensure no residual data stay in OS write buffer!*/
    if(fflush(fp) == EOF){
        goto werr;
    }
    /*fsync the file to disk*/
    if(aof_fsync(fileno(fp) == -1)){
        goto werr;
    }
    /*close fp*/
    if(fclose(fp) == -1){
        goto werr;
    }
    /*rename the temp file to given name*/
    if(rename(tmpfile, filename) == -1){
        xredisLog(XREDIS_WARNING, "Error moving temp append only file on the final destination: %s", strerror(errno));
        unlink(tmpfile);
        return XREDIS_ERR;
    }
    xredisLog(XREDIS_NOTICE, "SYNC append only file rewrite performed");
    return XREDIS_OK;
werr:
    fclose(fp);
    unlink(tmpfile);
    xredisLog(XREDIS_WARNING, "Write error writing append only file on disk: %s", strerror(errno));
    if(di){
        dictReleaseIterator(di);
    }
    return XREDIS_ERR;
}

int rewriteAppendOnlyFileBackground(void){
    /*execute AOF rewrite by a child in background*/
    /*return XREDIS_OK if successes, return XREDIS_ERR if fails*/
    pid_t childpid;
    long long start;
    /*return XREDIS_ERR if background rewritting is executing*/
    if(server.aof_child_pid != -1){
        return XREDIS_ERR;
    }
    start = ustime();
    if((childpid = fork()) == 0){
        /*child*/
        /*close networking connection*/
        if(server.ipfd > 0){
            close(server.ipfd);
        }
        if(server.sofd > 0){
            close(server.sofd);
        }
        /*create temp file and rewrite it*/
        snprintf(tmpfile, 256, "temp-rewriteaof-bg-%d.aof", (int)getpid());
        if(rewriteAppendOnlyFile(tmpfile) == XREDIS_OK){
            size_t private_dirty = zmalloc_get_private_dirty();

            if(private_dirty){
                xredisLog(XREDIS_NOTICE, "AOF rewrite: %lu MB of memory used by
                        copy-on-write", private_dirty/(1024*1024));
            }
            exitFromChild(0)
        }
        exitFromChild(1);
    }
    else{
        /*parent*/
        /*update start fork time*/
        server.stat_fork_time = ustime()-start;
        if(childpid == -1){
            xredisLog(XREDIS_WARNING, "Can't rewrite append only file in
                    background: fork: %s", strerror(errno));
            return XREDIS_ERR;
        }
        xredisLog(XREDIS_NOTICE, "Background append only file rewriting
                started by pid %d", childpid);
        /*update server state*/
        server.aof_rewrite_scheduled = 0;
        server.aof_rewrite_time_start = time(NULL);
        server.aof_child_pid = childpid;
        /*close the rehash in key space, avoid copy-on-write*/
        updateDictResizePolicy();
        /*set database id to -1, to force execute SELECT before feedAppendOnlyFile,
         * which could avoid database id wrong when mergering */
        server.aof_selected_db = -1;
        return XREDIS_OK;
    }
    return XREDIS_Ok;
}

void bgrewriteaofCommand(xredisClient *xc){
    /*command function for BGREWRITEAOF COMMAND*/
    if(server.aof_child_pid != -1){
        addReplyError(xc, "Background append only file rewritting already in progress");
    }
    /*In xredis,the priority of AOF is higher than RDB*/
    else if(server.rdb_child_pid != -1){
        server.aof_rewrite_scheduled = 1;
        addReplyStatus(xc, "Background append only file rewriting scheduled");
    }
    else if(rewriteAppendOnlyFileBackground() == XREDIS_OK){
        addReplyStatus(xc, "Background append only file rewriting started");
    }
    else{
        addReply(xc, shared.err);
    }
}

void aofRemoveTempFile(pid_t childpid){
    /* remove the temp file */
    snprintf(tmpfile, 256, "temp-rewriteaof-bg-%d.aof", (int)getpid());
    unlink(tmpfile);
}

void BackgroundRewriteDoneHandler(int exitcode, int bysignal){
    /*what should to do after childpid finishing background rewrtting*/
    /*corner cases*/
    if((exitcode == 1) && (bysignal == 0)){
        server.aof_lastbgrewrite_status = XREDIS_ERR;
        xredisLog(XREDIS_WARNING, "Background AOF rewrite terminated with error");
    }
    else if(bysignal){
        server.aof_lastbgrewrite_status = XREDIS_ERR;
        xredisLog(XREDIS_WARNING, "Background AOF rewrite terminated by signal %d", bysignal);
    }
    else if((exitcode == 0) && (bysignal == 0)){
        int newfd, oldfd;
        char tmpfile[256];
        long long now = ustime();

        xredisLog(XREDIS_NOTICE, "Background AOF rewrite terminated with successes");
        /*Add the commands received by rewrite buffer during child runing to new AOF file*/
        /*create tmp file name*/
        snprintf(tmpfile, 256, "temp-rewriteaof-bg-%d.aof", (int)server.aof_child_pid);
        if((newfd = open(tmpfile, O_WRONLY|O_APPEND)) == -1){
            xredisLog(XREDIS_WARNING, "Unable to open the temporary file created by the child :
                    %s", strerror(errno));
            goto cleanup;
        }
        /*write the contents in AOF rewrite buffer into newfd*/
        if(aofRewriteBufferWrite(newfd) == -1){
            xredisLog(XREDIS_WARNING, "Error trying to flush the parent diff to the rewritten AOF: %s", strerror(errno));
            close(newfd);
            goto cleanup;
        }
        xredisLog(XREDIS_NOTICE, "Parent diff successfully flushed to the rewritten AOF (%lu bytes)", aofRewriteBufferSize());
        /*atomic cover the old AOF file*/
        if(server.aof_fd == -1){
            /*AOF disabled*/
            oldfd = open(server.aof_filename, O_RDONLY|O_NONBLOCK);
        }
        else{
            oldfd = -1;
        }
        /* Rename the temporary file. This will not unlink the target file if it exists,
         * because we reference it with "oldfd". */
        if(rename(tmpfile, server.aof_filename) == -1){
            xredisLog(REDIS_WARNING,"Error trying to rename the temporary AOF file: %s", strerror(errno));
            close(newfd);
            if(oldfd != -1){
                close(oldfd);
            }
            goto cleanup;
        }

        if (server.aof_fd == -1) {
            /* AOF disabled, we don't need to set the AOF file descriptor to this new file, so we can close it. */
            close(newfd);
        } else {
            /* AOF enabled, replace the old fd with the new one. */
            oldfd = server.aof_fd;
            server.aof_fd = newfd;

            if (server.aof_fsync == AOF_FSYNC_ALWAYS){
                aof_fsync(newfd);
            }
            else if (server.aof_fsync == AOF_FSYNC_EVERYSEC){
                aof_background_fsync(newfd);
            }
            server.aof_selected_db = -1; /* Make sure SELECT is re-issued */
            aofUpdateCurrentSize();
            server.aof_rewrite_base_size = server.aof_current_size;

            /* Clear regular AOF buffer since its contents was just written to the new AOF from the background rewrite buffer. */
            sdsfree(server.aof_buf);
            server.aof_buf = sdsempty();
        }

        server.aof_lastbgrewrite_status = XREDIS_OK;

        xredisLog(XREDIS_NOTICE, "Background AOF rewrite finished successfully");
        /* Change state from WAIT_REWRITE to ON if needed */
        if (server.aof_state == XREDIS_AOF_WAIT_REWRITE){
            server.aof_state = XREDIS_AOF_ON;
        }
        /* Asynchronously close the overwritten AOF. */
        if (oldfd != -1){
            bioCreateBackgroundJob(REDIS_BIO_CLOSE_FILE,(void*)(long)oldfd,NULL,NULL);
        }

        xredisLog(XREDIS_VERBOSE,"Background AOF rewrite signal handler took %lldus", ustime()-now);
    }
cleanup:
    aofRewriteBufferReset();
    aofRemoveTempFile(server.aof_child_pid);
    server.aof_child_pid = -1;
    server.aof_rewrite_time_last = time(NULL)-server.aof_rewrite_time_start;
    server.aof_rewrite_time_start = -1;
    /* Schedule a new rewrite if we are waiting for it to switch the AOF ON. */
    if (server.aof_state == REDIS_AOF_WAIT_REWRITE){
        server.aof_rewrite_scheduled = 1;
    }
}
