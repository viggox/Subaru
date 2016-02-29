#include "xredis.h"
#include <signal.h>
#include <ctype.h>

/*----level1--Client level database APIs----------*/
/*-------------LOOK UP--------------------*/
xrobj *lookupKey(xredisDb *db, xrobj *key){
    //look up key's val in database db;
    dict *d;
    dictEntry *de;
    xrobj *value;

    d = db->dict;
    de = dictFind(d, key->ptr); //MODIFIED AFTER,原来key不是一字符串对象，key就是一字符串！

    if(de){
        value = dictGetVal(de);
        //ADD AFTER(REMEMBER update key's lru time after reading it!)
        if((server.rdb_child_pid == -1) && (server.aof_child_pid == -1)){
            value->lru = server.lruclock;
        }
        return value;
    }
    else{
        return NULL;
    }
}

xrobj *lookupKeyRead(xredisDb *db, xrobj *key){
    //look up key's val in db for the purpose of reading
    xrobj *value;
    expireIfNeeded(db, key);
    //All Read/Write database manipulation will tigger expireIfNeeded(inert deletion)
    value = lookupKey(db, key);
    if(value == NULL){
         server.stat_keyspace_misses++;
    }
    else{
         server.stat_keyspace_hits++;
    }
    return value;
}

xrobj *lookupKeyWrite(xredisDb *db, xrobj *key){
     //look up key's val in db for the purpose of writing
     xrobj *value;
     expireIfNeeded(db, key);

     value = lookupKey(db, key);
     return value;
}

xrobj *lookupKeyReadOrReply(xredisClient *xc, xrobj *key, xrobj *reply){
    //look for and read key, reply if not founded
    xrobj *value;

    value = lookupKeyRead(xc->db, key);
    if(value == NULL){
        addReply(xc, reply);
    }
    return value;
}

xrobj *lookupKeyWriteOrReply(xredisClient *xc, xrobj *key, xrobj *reply){
    //look up for writing key, reply if not founded;
    xrobj *value;

    value = lookupWrite(xc->db, key);
    if(value == NULL){
        addReply(xc, reply);
    }
    return value;
}

/*--------------------SET,non-key level----------------*/
/*since all DB COMMAND only use them as inner function, no need include lru & expire update*/
void dbAdd(xredisDb *db, xrobj *key, xrobj *val){
    //add the given key-value to the keysapce;
    //Add only when key is not existed
    sds sdskey = sdsdup(key->ptr);    //Key in keyspace is just a sds, not a sds object!
    int rtn = dictAdd(db->dict, sdskey, val);
    //if key already exists, dictAdd will return DICT_ERR
    xredisAssertWithInfo(NULL, key, rtn == DICT_OK);
}

void dbOverwrite(xredisDb *db, xrobj *key, xrobj *val){
    //update the given key
    //process only when key is already exists
    xredisAssertWithInfo(NULL, key, dictFind(db->dict, key->ptr) != NULL);

    sds sdskey = sdsdup(key->ptr);
    dictReplace(db->dict, sdskey, val);
}

void setKey(xredisDb *db, xrobj *key, xrobj *val){
     //SET key with val no matter key is exists or not
     sds sdskey = sdsdup(key->ptr);
     int rtn = dictReplace(db->dict, sdskey, val);
     //increment val's refCount;
     incrRefCount(val);
     //report to all the clients who WATCH this key that it's modified;
     signalModifiedKey(db, key);
     //remove key's old expire time(if exists), which makes it be persistent;
     removeExpire(db);
}

/*-----------------------CHECK-----------------------*/
/*---------------STILL NON COMMNAND LEVEL------------*/
int dbExists(xredisDb *db, xrobj *key){
    //check whether key is in keyspace
    //return 1 if do exists, else return 0;
    xrobj *value;
    value = lookupKey(db, key);
    if(value){
        return 1;
    }
    else{
        return 0;
    }
}

xrobj *dbRandomKey(xredisDb *db){
    //return a key from database with the form of xrobj randomly;
    dictEntry *de;
    sds sdskey;
    xrobj *key;

    while(1){
        de = dictGetRandomKey(db->dict);
        if(de == NULL) return NULL;
        sdskey = dictGetKey(de);
        key = createStringObject(sdskey, sdslen(sdskey));
        //Check whether key is expired (ADD AFTER)
        if(dictFind(db->expires, sdskey)){
            if(expireIfNeeded(db, key)){
                decrRefCount(key);
                //free the key "Object" JUST CREATED;
                continue;
            }
        }
        return key;
    }
}

int dbDelete(xredisDb *db, xrobj *key){
    //delete the key from keyspace
    //remove key from expires dict first;
    if(dictSize(db->expires) > 0){
        dictDelete(db->expires, key->ptr);
    } //delete key from expire dict doesn't free the key sds!
    if(dictDelete(db->dict, key->ptr) == DICT_OK){
        return 1;
    }
    return 0;
}

long long emptyDb(){
    //clear all the databases in server;
    //return the number of keys cleared;
     int i;
     long long removed;

     for(i=0; i<server.dbnum; i++){
         removed += dictSize(server.db[i]->dict);
         dictEmpty(server.db[i]->expires);
         dictEmpty(server.db[i]->dict);
     }

     return removed;
}

int selectDb(xredisClient *xc, int id){
    //SELECT a database by id
    if((id>0) || (id<server.dbnum-1)){
        xc->db = &server.db[id]; //MODIFIED AFTER, DON'T FORGET & BEFORE!
        return XREDIS_OK;
    }
    return XREDIS_ERR;
}

/*-----------hooks for key-space change---------*/
/*----TOUCH WATCHED KEYS(SEE DETAILS IN multi.c)----*/
void signalModifiedKey(xredisDb *db, xrobj *key){
    //report to all clients which is WATCHing that the key is modified;
    touchWatchedKeys(db, key);
}

void signalFlushedDb(int dbid){
    //report to all clients which is WATCHing that the database is flushed
    touchWatchedKeysOnFlush(dbid);
}

/*----------keyspace type AGNOSTIC COMMANDS----------*/
void flushdbCommand(xredisClient *xc){
    //FLUSHDB command, clear all key-val pairs in xc's database
    signalFlushedDb(xc->db->id);
    server.dirty += dictSize(xc->db->dict);
    dictEmpty(xc->db->dict);
    dictEmpty(xc->db->expires);
    addReply(xc, shared.ok);
}

void flushallCommand(xredisClient *xc){
    //FLUSHALL command, clear all databases in server
    signalFlushedDb(-1); //report to all clients!
    server.dirty += emptyDb(); //empty all databases!
    addReply(xc, shared.ok);
    //force quit if the database saving process is on;
    if(server.rdb_child_pid != -1){
        kill(server.rdb_child_pid, SIGKILL);
        rdbRemoveTmpFile(server.rdb_child_pid);
    }
    //save rdb file and backup server.dirty
    if(server.saveparasmlen > 0){
        int save.dirty = server.dirty;
        rdbSave(server.rdb_filename);
        server.dirty = save.dirty;
    }
    server.dirty++; //DON'T UNDERSTAND YET!
}

void delCommand(xredisClient *xc){
    //DEL command, delete the given key and it's value
    int i, removed=0;

    for(i=1; i<xc->argc; i++){
        if(dbDelete(xc->db, xc->argv[i])){
            signalModifiedKey(xc->db, xc->argv[i]);
            server.dirty++;
            removed++;
        }
    }
    //Reply removed keys number to client;
    addReplyLongLong(xc, removed);
}

void selectCommand(xredisClient *xc){
    //SELECT command, switch database;
    long long newid;
    //get newid from input, so input must could be converted to long long;
    if(getLongLongFromObejctOrReply(xc, xc->argv[1], &newid, "Invaid DB Index!") != XREDIS_OK){
        return;  //cannot extract a proper long long integer from input
    }
    if(selectDb(xc, newid) != XREDIS_OK){
        addReply(xc, "Invaid DB Index!")   //input number not in proper realm;
    }
    else{
        addReply(xc, shared.ok);
    }
}

void randomkeyCommand(xredisClient *xc){
    //RANDOMKEY command, return a key from database randomly;
    xrobj *rtnkey;

    if((rtnkey = dbRandomKey(xc->db)) == NULL){
        addReply(xc, "shared.nullbulk");
        return;
    }

    addReplyBulk(xc, rtnkey);
    decrRefCount(rtnkey); //ADDED AFTER, DON'T FORGET free the key OBJECT after reply it to client!
}

void keysCommand(xredisClient *xc){
    //KEYS command, return all keys match the supplied pattern;
    dictIterator *di;
    sds pattern, skey;
    int patternlen, skeylen;
    long long numkey=0;
    dictEntry *de;

    void *replylen = addDeferredMultiBulkLength(xc); //ADDED AFTER;

    pattern = xc->argv[1]->ptr;
    patternlen = sdslen(patternlen);
    di = dictGetSafeIterator(xc->db->dict);

    while(de = dictNext(di)){
         skey = dictGetKey(de);
         skeylen = sdslen(skey);

         if(stringmatchLen(pattern, patternlen, skey, skeylen, 0)){
             xrobj *key = createStringObject(skey, skeylen);
             if(expireIfNeeded(xc->db, key) == 0){
                  addReplyBulk(xc, key);
                  numkey++;
             }
             decrRefCount(key);
         }
    }
    dictReleaseIterator(di);

    setDeferredMultiBulkLength(xc,replylen,numkeys);  //ADDED AFTER; see you in networking.c~
}

void dbsizeCommand(xredisClient *xc){
     //DBSIZE command, return the number of key-value pairs in database;
     addReplyLongLong(xc, dictSize(xc->db->dict));
}

void existsCommand(xredisClient *xc){
     //EXISTS command, check whether a key is in database;
     expireIfNeeded(xc->db, xc->argv[1]);
     if(dbExists(xc->db, xc->argv[1])){
         addReply(xc, shared.cone);
     }
     else{
         addReply(xc, shared.czero;)
     }
}

void renameGenericCommand(xredisClient *xc, int nx){
    //Generic function for RENAME and RENAMENX command;
    xrobj *xo;
    long long expire;
    //Exception: input old and new key name are the same;
    if(sdscmp(xc->argv[1]->ptr,xc->argv[2]->ptr) == 0){
         addReply(shared.sameobjecterr);
         return;
    }
    //Exception: no key found by old name;
    if(!(xo = lookupKeyWriteOrReply(xc, xc->argv[1], shared.nokeyerr))){
         return;
    }
    incrRefCount(xo); //ADD AFTER;
    expire = getExpire(xc->db, xc->argv[1]);
    //Exception: the new key name conflicts with an old key name;
    if(lookupKeyWrite(xc->db, xc->argv[2]) != NULL){
        //if nx is on, the operation failed,return;
        if(nx){
            decrRefCount(xo);
            addReply(xc, shared.czero);
            return;
        }
        //if nx is not on, delete the conficted old key;
        dbDelete(xc->db, xc->argv[2]);
    }
    /*The way of replace old key name is:
     * first create a new key-val pair to key-value space,
     * then delete the old key-val pair*/
    dbAdd(xc->db, xc->argv[2], xo);
    if(expire){
        setExpire(xc->db, xc->argv[2], expire);
    }
    dbDelete(xc->db, xc->argv[1]);
    //ADD AFTER(总是想不起来！)
    signalModifiedKey(xc->db, xc->argv[1]);
    signalModifiedKey(xc->db, xc->argv[2]);
    server.dirty++;
    addReply(xc, nx ? shared.cone : shared.ok);
}

void renameCommand(xredisClient *xc){
     //RENAME command, rename an existing key;
     //Any exisited key whose name conflicts with new key name will be deleted;
     renameGenericCommand(xc, 0);
}

void renamenxCommand(xredisClient *xc){
     //RENAMENX command;
     //new key name cannot conflicts with existing keys;
    renameGenericCommand(xc, 1);
}

void lastsaveCommand(xredisClient *xc){
    //LASTSAVE command, return the lastsave time of database;
    addReply(xc, server.lastsave);
}

void typeCommand(xredisClient *xc){
    //TYPE command, return key object's string form;
    xrobj *xo;
    char *type;

    if(!(xo = lookupKeyReadOrReply(xc, xc->argv[1], shared.nokeyerr))){
         return;
    }
    swith(xo->type){
        case XREDIS_STRING:
            type = "string";
            break;
        case XREDIS_LIST:
            type = "list";
            break;
        case XREDIS_HASH:
            type = "hash";
            break;
        case XREDIS_SET:
            type = "set";
            break;
        case XREDIS_ZSET:
            type = "zset";
            break;
        default:
            type = "unknown";
            break;
    }
    addReplyStatus(xc, type);
}

void shutdownCommand(xredisClient *xc){
     //SHUTDOWN command, shut down the xredis server;
     //you can choose SHUTDOWN SAVE or SHUTDOWN NOSAVE;
     int flags = 0;

     if(xc->argc>2){
          addReply(xc, shared.syntaxerr);
          return;
     }
     else if(xc->argc == 2){
         if(strcasecmp(xc->argv[1]->ptr, "save") == 0){
             flags |= XREDIS_SHUTDOWN_SAVE;
         }
         else if(strcasecmp(xc->argv[1]->ptr, "nosave") == 0){
             flags |= XREDIS_SHUTDOWN_NOSAVE;
         }
         else{
             addReply(xc, shared.syntaxerr);
             return;
         }
     }
     if(prepareForShutdown(flags) == XREDIS_OK){
         exit(0);
     }
     addReplyError(xc, "Errors trying to SHUTDOWN!");
}

void moveCommand(xredisClient *xc){
    //MOVE command, move key from one database to another;
    int srcid, dstid;
    xredisDb *src, *dst;
    xrobj *value;

    src = xc->db; //backup original database;
    srcid = xc->db->id; //backup original database id;

    if(string2l(xc->argv[2]->ptr, sdslen(xc->argv[2]->ptr), &dstid) == 0){
         addReply(xc, shared.syntaxerr);
         return;
    }
    if(selectDb(xc, dstid) != XREDIS_OK){
         addReply(xc, shared.outofrangeerr);
         return;
    }
    if(srcid == dstid){
        addReply(xc, shared.sameobjecterr);
        return;
    }
    if(!(value=lookupKeyWrite(src, xc->argv[1]))){
        addReply(xc, shared.czero);
        return;
    }

    dst = &server.db[dstid];
    dbAdd(dst, xc->argv[1], value);
    incrRefCount(value);
    dbDelete(src, xc->argv[1]);

    server.dirty++;
    addReply(xc, shared.cone);
}

/*---------level1-----client level expire time APIs--------------*/
/*-----Used by both DB Commands and other APIs------*/
int removeExpire(xredisDb *db, xrobj *key){
    //remove key's expire time;
    xredisAssertWithInfo(NULL, key, dictFind(db->dict, key->ptr) != NULL);
    if(dictDelete(db->expires, key->ptr) == DICT_OK){
         return 1;
    }
    return 0;
}

void setExpire(xredisDb *db, xrobj *key, long long when){
    //Set expire time for the key;
    dictEntry *de;
    xredisAssertWithInfo(NULL, key, dictFind(db->dict, key->ptr) != NULL);
    de = dictReplaceRaw(db->expires, key->ptr);
    dictSetSignedIntegerVal(de, when);
}

long long getExpire(xredisDb *db, xrobj *key){
    //return the expire time of the specific key;
    //return -1 if no expire time is associated with this key;
    dictEntry *de;
    xredisAssertWithInfo(NULL, key, dictFind(db->dict, key->ptr) != NULL);
    if((dictSize(db->expires) == 0)||((de = dictFind(db->expires, key->ptr)) == NULL)){
         return -1;
    }
    return dictGetSignedInetegerVal(de);
}

void propagateExpire(xredisDb *db, xrobj *key){
    //propagte expires into slaves and the AOF file;
    //now we only realize the ability of propagating into AOF file(等到我做到分布式的时候再追加slaves部分);
    xrobj *argv[2];
    argv[0] = shared.del;
    argv[1] = key;

    incrRefCount(argv[0]);
    incrRefCount(argv[1]);

    if(server.aof_state != XREDIS_AOF_OFF){
        feedAppendOnlyFile(server.delCommand, db->id, argv,2);
    }

    decrRefCount(argv[0]);
    decrRefCount(argv[1]);
}

int expireIfNeeded(xredisDb *db, xrobj *key){
    //delete key if it has exceeded the expire time, else, do nothing;
    long long texpire;

    texpire = getExpire(db, key);
    //Don't expire anything while loading;
    if((texpire == -1) || (server.loading)){
         return 0;
    }
    if(mstime() <= texpire){
         return 0;
    }
    //update server's expired key counting
    server.stat_expiredkeys++;
    //append del command to AOF file;
    propagateExpire(db, key);
    //delete the key from keyspace;
    return dbDelete(db,key);
}

/*----------------expires COMMANDS---------------*/
void pexpireatGenericCommand(xredisClient *xc, long long basetime, int unit){
    //generic function for EXPIRE, PEXPIRE, EXPIREAT, PEXPIREAT command;
    //"basetime" is used to signal current time in non "AT" commands , set 0 in "AT" commands;
    //"unit" could be UNIT_SECONDS or UNIT_MILLISECONDS, basetime is always in milliseconds;
    xrobj *key;
    long long texpire;

    key=xc->argv[1];
    if(getLongLongFromObejctOrReply(xc, xc->argv[2], &texpire, NULL) != XREDIS_OK){
         return;
    }
    if(dictFind(xc->db->dict, key->ptr)==NULL){
        addReply(shared.czero);
        return;
    }
    if(unit == UNIT_SECONDS){
        texpire *= 1000;
    }
    texpire += basetime;
    /*-delete the expired keys-*/
    //which take instead of using expiredIfNeeded function;
    if((texpire<mstime()) && (!server.loading)){
         dbDelete(xc->db, key);
         server.dirty++;
         return;
    }
    else{
        setExpire(xc->db, key, texpire);
        addReply(xc, shared.cone);
        signalModifiedKey(xc->db, key);
        server.dirty++;
        return;
    }
}

void expireCommand(xredisClient *xc){
    //EXPIRE COMMAND;
    pexpireatGenericCommand(xc, mstime(), UNIT_SECONDS);
}

void pexpireCommand(xredisClient *xc){
    //PEXPIRE COMMAND;
    pexpireatGenericCommand(xc, mstime(), UNIT_MILLISECONDS);
}

void expireatCommand(xredisClient *xc){
    //EXPIREAT COMMAND;
    pexpireatGenericCommand(xc, 0, UNIT_SECONDS);
}

void pexpireatCommand(xredisClient *xc){
     //PEXPIREAT COMMAND;
     pexpireatGenericCommand(xc, 0, UNIT_MILLISECONDS);
}

void ttlGenericCommand(xredisClient *xc, int unit){
    //generic function for TTL and PTTL commands;
    xrobj *key;
    long long texpire, ttl;

    key = xc->argv[1];
    if(lookupKeyRead(xc->db, key) == NULL){
         addReplyLongLong(xc, -2);
         return;
    }
    if((texpire=getExpire(xc->db, key)) == -1){
         addReplyLongLong(xc, -1);
         return;
    }
    ttl = texpire-mstime();
    if(ttl < 0){
         addReplyLongLong(xc, -1);
         return;
    }
    else{
        addReplyLongLong(xc, (unit==UNIT_SECONDS)?(ttl+500)/1000:ttl);
        //the seconds return is "round-off"(ADD AFTER);
        return;
    }
}

void ttlCommand(xredisClient *xc){
    ttlGenericCommand(xc, UNIT_SECONDS);
}

void pttlCommand(xredisClient *xc){
    ttlGenericCommand(xc, UNIT_MILLISECONDS);
}

void persistCommand(xredisClient *xc){
    //PERSIST command;
    if(dictFind(xc->db->dict, xc->argv[1]->ptr) == NULL){
         addReply(xc, shared.czero);
         return;
    }
    if(removeExpire(xc->db, xc->argv[1])){
        addReply(xc, shared.cone);
        server.dirty++;
    }
    else{
        addReply(xc, shared.czero);
    }
    return;
}

/*------------APIs to get key arguments from COMMANDS---------*/
int *getKeysUsingCommandTable(struct xredisCommand *cmd,xrobj **argv, int argc, int *numkeys){
    //get keys positions from xredisCommand, and put them into an array and return;
    int i, j=0, last, *keys;

    if(cmd->firstkey == 0){
         *numkey = 0;
         return NULL;
    }
    last = cmd->lastkey;
    if(last<0){
         last += argc;
    }
    keys = zmalloc(sizeof(int)*(last-cmd->firstkey+1));
    for(j=cmd->firstkey; j<=last; j+=cmd->keystep){
        xredisAssert(j<argc);
        keys[i++] = j;
    }
    *numkeys = i;
    return keys;
}

int getKeysFromCommand(struct xredisCommand *cmd, xrobj **argv, int argc, int *numkeys){
    //Consider the getkeys_proc function in;
    if(cmd->getkeys_proc){
        return cmd->getkeys_proc(cmd, argv, argc, flags);
    }
    return getKeysUsingCommandTable(cmd, argv, argc, numkeys);
}

void getKeysFreeResult(int *result){
     zfree(result);
}

int *noPreloadGetKeys(struct xredisCommand *cmd, xrobj **argv, int argc, int *numkeys, int flags){
    //Consider PRELOAD case;
    if(flags && XREDIS_GETKEYS_PRELOAD){
         *numkeys = 0;
         return NULL;
    }
    else{
         return getKeysUsingCommandTable(cmd, argv, argc, numkeys);
    }
}

int *zunionInterGetKeys(struct xredisCommand *cmd, xrobj **argv, int argc, int *numkeys, int flags){
    //Consider PRELOAD case;
    if(flags && XREDIS_GETKEYS_PRELOAD){
         *numkeys = 1;
         keys[0] = 1;
         return NULL;
    }
    else{
        return getKeysUsingCommandTable(cmd, argv, argc, numkeys);
    }
}

int *zunionInterGetKeys(struct xredisCommand *cmd, xrobj **argv, int argc, int *numkeys, int flags){
    //......
    int i, num, *keys;
    num = atoi(argv[2]->ptr);
    if(num>(argc-3)){
         *numkeys = 0;
         return NULL;
    }
    keys = zmalloc(sizeof(int)*num);
    for(i=0; i<num; i++){
         keys[i] = 3+i;
    }
    *numkeys = num;
    return keys;
}

