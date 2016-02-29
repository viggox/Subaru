#include "xredis.h"
#include <math.h>
#include <ctype.h>

/*----------------------create new objects----------------*/
xrobj *createObject(int type, void *ptr){
    //create new object by given type and value
    //1.alloc space
    xrobj *xo = zmalloc(sizeof(xrobj));
    //2.initialize properties
    xo->type = type;
    xo->ptr = ptr;
    xo->encoding = XREDIS_ENCODING_RAW;
    xo->refcount = 1;
    //3.set LRU time to current lru clock
    xo->lru = server.lruclock;
    //4.return the created object
    return xo;
}

xrobj *createStringObject(char *ptr, size_t len){
    //create a string Object by the given char array
    return createObject(XREDIS_STRING, sdsnewlen(ptr,len));
}

xrobj *createStringObjectFromLongLong(long long value){
    //create a string object from given long long value
    //1.judge whether to use shared object
    if((value>=0) && (value<=XREDIS_SHARED_INTEGERS)){
        incrRefCount(shared.integers[value]);
        return shared.integers[value];
    }
    //2.if not, create new object, set encoding by value's range
    if((value>=LONG_MIN) && (value<=LONG_MAX)){
        xrobj *xo = createObject(XREDIS_STRING, NULL);
        xo->encoding = XREDIS_ENCODING_INT;
        xo->ptr = (void *)((long)value); //What the fuck?!!!!!! what is the fucking (void *) ?!!!!
    }
    else{
        xrobj *xo = createObject(XREDIS_STRING, sdsfromlonglong(value));
    }
    return xo;
}

xrobj *createStringObjectFromLongDouble(long double value){
     //create a string object from given long double value
     char buf[256];
     int slen;
     //1. use snprintf to change long double to string
     slen = snprintf(buf,sizeof(buf), "%.17Lf", value);
     //2. remove the tailing zeros and decimal point
     if(strchr(buf, '.')!=NULL){
         for(char *p=buf+slen-1; *p == '0'; p--){
             slen--;
         }
         if(*p == '.'){
              p--;
              slen--;
         }
     }
     //3. create string object
     xrobj *xo = createStringObject(buf, slen);
     return xo;
}

xrobj *dupStringObject(xrobj *xo){
    //duplicate a string object, return its transcripti
    redisAssertWithInfo(NULL, xo, xo->encoding == REDIS_ENCODING_RAW);
    return createStringObject(xo->ptr, sdslen(sds->ptr));
}

xrobj *createListObject(void){
    //create a adlist
    list *al = listcreate();
    //create a list object
    xrobj *xo = createObject(XREDIS_LIST, al);
    listSetFreeMethod(al,decrRefCount);
    xo->encoding = XREDIS_ENCODING_LINKEDLIST;
    return xo;
}

xrobj *createZiplistObject(void){
     //create a ziplist
     unsigned char *zl;
     zl = ziplistNew();
     //create a ziplist object
     xrobj *xo = createObject(XREDIS_LIST, zl);
     xo->encoding = XREDIS_ENCODING_ZIPLIST;
     return xo;
}

xrobj *createIntSetObject(void){
    //create a intset
    intset *ist;
    ist = intsetNew();
    //create a intset object
    xrobj *xo = createObject(XREDIS_SET, ist);
    xo->encoding = XREDIS_ENCODING_INTSET;
    return xo;
}

xrobj *createSetObject(void){
     //create a set(hashtable)
     dict *d;
     d = dictcreate(&setDictType, NULL);
     //create a dict object
     xrobj *xo = createObject(XREDIS_SET, d);
     xo->encoding = XREDIS_ENCODING_HT;
     return xo;
}

xrobj *createHashObject(void){
    unsigned char *zl = ziplistNew();
    xrobj *xo = createObject(XREDIS_HASH, zl);
    xo->encoding = XREDIS_ENCODING_ZIPLIST;
    return xo;
}

xrobj *createZsetObject(void){
    //zset use dict and ziplist at the same time
    zset *zs = zmalloc(sizeof(*zs));
    xrobj *xo;
    //initialize the zs
    zs->dict = dictcreate(&zsetDictType, NULL);
    zs->zsl = zslcreate();
    //create a new object with zs
    xo = createObject(XREDIS_ZSET, zs);
    xo->encoding = XREDIS_ENCODING_SKIPLIST;
    return xo;
}

xrobj *createZetZiplistObject(void){
    unsigned char *zl = ziplistNew();
    xrobj *xo = createObject(XREDIS_ZSET, zl);
    xo->encoding = XREDIS_ENCODING_ZIPLIST;
    return xo;
}

/*------------------------------Free old objects-----------------------*/
void freeStringObject(xrobj *xo){
     //free string object
    if(xo->encoding == XREDIS_ENCODING_RAW){
         sdsfree(xo->ptr);
    }
}

void freeListObject(xrobj *xo){
    if(xo->encoding == XREDIS_ENCODING_LINKEDLIST){
         listRelease((list *)(xo->ptr));
    }
    else if(xo->encoding == XREDIS_ENCODING_ZIPLIST){
         zfree(xo->ptr);
    }
    else{
        xredisPanic("Unknown list encoding type");
    }
}

void freeSetObject(xrobj *xo){
    if(xo->encoding == XREDIS_ENCODING_HT){
        dictRelease((dict *)(xo->ptr));
    }
    else if(xo->encoding == XREDIS_ENCODING_INTSET){
         zfree(xo->ptr);
    }
    else{
        xredisPanic("Unknown set encoding type");
    }
}

void freeZsetObject(xrobj *xo){
    if(xo->encoding == XREDIS_ENCODING_SKIPLIST){
         zslFree((zskiplist *)(xo->ptr));
    }
    else if(xo->encoding == XREDIS_ENCODING_ZIPLIST){
        zfree(xo->ptr);
    }
    else{
        xredisPanic("Unknown zset encoding type");
    }
}

void freeHashObject(xrobj *xo){
    if(xo->encoding == XREDIS_ENCODING_ZIPLIST){
        zfree(xo->ptr);
    }
    else if(xo->encoding == XREDIS_ENCODING_HT){
        dictRelease((dict *)(xo->ptr));
    }
    else{
        xredisPanic("Unknown hash encoding type");
    }
}

/*----------------Handle refcount -------------------*/
void incrRefCount(xrobj *xo){
     xo->refcount++;
}

void decrRefCount(xrobj *xo){
    //decline the refcount, and release the object when refcount reach 0
    if(xo->refcount > 1){
         xo->refcount--;
    }
    else if(xo->refcount == 1){
        switch(xo->type){
            case XREDIS_STRING :
                freeSetObject(xo);
                break;
            case XREDIS_LIST :
                freeListObject(xo);
                break;
            case XREDIS_SET :
                freeSetObject(xo);
                break;
            case XREDIS_HASH :
                freeHashObject(xo);
                break;
            case XREDIS_ZSET :
                freeZsetObject(xo);
                break;
            default :
                xredisPanic("Unknown Object Type");
                break;
        }
        zfree(xo);
    }
    else{
        xredisPanic("decrRefCount against refcount <= 0");//Professional Error Message~~~
    }
}

xrobj *resetRefCount(xrobj *xo){
     xo->refcount=0;
     return xo;
}

/*-----------------------Type Checking Part-----------------------------*/
int checkType(xredisClient *c, xrobj *xo, int type){
    //Check whether the xrobj xo's type is the given type
    if(xo->type != type){
        //if the type doesn't match, report to the client with wrong type error
        //and return 1
        addReply(c, shared.wrongtypeerr);
        return 1;
    }
    else{
        return 0;
    }
}

int isObjectRepresentableAsLongLong(xrobj *xo, long long *llval){
    //check whether the given string object could be set as long long value
    //return XREDIS_OK if successed, else, return XREDIS_ERR
    redisAssertWithInfo(NULL, xo, xo->type == XREDIS_STRING);
    if(xo->encoding == XREDIS_ENCODING_INT){
        if(llval) *llval = (long)(xo->ptr);
        return XREDIS_OK;
    }
    else{
        if(string2ll(xo->ptr, sdslen(xo->ptr), llval)){
            return XREDIS_OK;
        }
        else{
             return XREDIS_ERR;
        }
        //改成？：形式食用更佳！（编码风格！！）
    }
}

xrobj *tryObjectEncoding(xrobj *xo){
    //Try to encode a string object as INT to save space,and add to shared object if possible
    long value;
    if(xo->encoding != XREDIS_ENCODING_RAW){
        //special case which xo is already encoded
        return xo;
    }
    else if(xo->refcount > 1){
        //special case which xo is already been shared
        return xo;
    }
    //Only Try to encode the string object
    redisAssertWithInfo(NULL, xo, xo->type == XREDIS_STRING);
    else{
        if(string2l(xo->ptr, sdslen(xo->ptr), &value)){
            if((value>=0) && (value<=XREDIS_SHARED_INTEGERS) && (server.maxmemory == 0)){
                //Note that we also avoid using shared integers when maxmemory is used
                //because every object needs to have a private LRU field for the LRU
                decrRefCount(xo);//Free the old string object
                incrRefCount(shared.integers[value]);
            }
            else{
                freeStringObject(xo);
                xo->ptr = (void *)((long)value);
                xo->encoding = XREDIS_ENCODING_INT;
            }
            return xo;
        }
        else{
            return xo;
        }
    }
}

/*--------------------Get value from objects-------------------*/
xrobj *getDecodedObject(xrobj *xo){
    //return the decoded transcript of xo
    if(xo->encoding == XREDIS_ENCODING_RAW){
        incrRefCount(xo);
        return xo;
    }
    else if((xo->encoding == XREDIS_ENCODING_INT) && (xo->type == XREDIS_STRING)){
        char buf[32];
        xrobj *nxo
        ll2string(buf, 32, (long)(xo->ptr));
        nxo = createStringObject(buf, strlen(buf));
        return nxo;
    }
    else{
         xredisPanic("Unknown encoding type");
    }
}

int compareStringObjects(xrobj *a, xrobj *b){
     //Compare the strings preserved by two string objects
     redisAssertWithInfo(NULL, a, (a->type == XREDIS_STRING) && (b->type == XREDIS_STRING));
     //special case: a == b
    if(a == b){
        return 0;
    }
    //use sdscmp if a or b or encoded as string, else, must use strcmp
    int allsds = 1;
    char *astr, *bstr;

    if(a->encoding == XREDIS_ENCODING_INT){
         char abuf[128];
         ll2string(abuf, sizeof(abuf), (long)(a->ptr));
         allsds = 0;
         astr = abuf;
    }
    else{
        astr = a->ptr;
    }

    if(b->encoding == XREDIS_ENCODING_INT){
        char bbuf[128];
        ll2string(bbuf, sizeof(bbuf), (long)(b->ptr));
        allsds = 0;
        bstr = bbuf;
    }
    else{
        bstr = b->ptr;
    }
    return allsds == 1? sdscmp(astr, bstr) : strcmp(astr, bstr);
}

int equalStringObjects(xrobj *a, xrobj *b){
    //return 1 if string preserved by a & b is equal, else return 0
    //special case, when a&b all preserve INT
    if((a->encoding == XREDIS_ENCODING_INT) && (b->encoding == XREDIS_ENCODING_INT)){
        return a->ptr == b->ptr;
    }
    else{
        return compareStringObjects(a, b) == 0;
    }
}

size_t stringObjectLen(xrobj *xo){
    //return xo string's length
    redisAssertWithInfo(NULL, xo, xo->type == XREDIS_STRING);
    if(xo->encoding == XREDIS_ENCODING_RAW){
         return sdslen(xo->ptr);
    }
    else{
        char *buf[128];
        return ll2string(buf, sizeof(buf), (long)(xo->ptr));
    }
}

int getDoubleFromObject(xrobj *xo, double *target){
    //try to get double value from xo, and preserve it in *target
    double dvalue;
    char *endptr;
    if(xo == NULL){
        return 0;
    }
    redisAssertWithInfo(NULL, xo, xo->type == XREDIS_STRING);
    if(xo->encoding == XREDIS_ENCODING_RAW){
        errno = 0;
        dvalue = strtold(xo->ptr, &endptr);
        if((isspace(((char *)xo->ptr)[0])) || (endptr[0] != '\0') || (errno == ERANGE) || (isnan(dvalue))){
            return XREDIS_ERR;
        }
    }
    else if(xo->encoding == XREDIS_ENCODING_INT){
        dvalue = (long)(xo->ptr);
    }
    else{
        xredisPanic("Unknown String Encoding");
    }
    *target = dvalue;
    return XREDIS_OK;
}

int getDoubleFromObjectOrReply(xredisClient *c, xrobj *xo, double *target, const char *msg){
    //try to get double value from xo, and prserve it in *target
    //report to client if fail
    double dvalue;
    if(getDoubleFromObject(xo, &dvalue) != XREDIS_OK){
        if(msg == NULL){
            addReplyError(c, "value is not a valid float");
        }
        else{
            addReplyError(c, (char *)msg);
        }
        return XREDIS_ERR;
    }
    *target = dvalue;
    return XREDIS_OK;
}

int getLongDoubleFromObject(xrobj *xo, long double *target){
     long double dvalue;
     char *endptr;
     if(xo == NULL){
         return 0;
     }
     redisAssertWithInfo(NULL, xo, xo->type == XREDIS_STRING);
     if(xo->encoding == XREDIS_ENCODING_RAW){
         errno = 0;
         dvalue = strtold(xo->ptr, &endptr);
         if((isspace(((char *)xo->ptr)[0])) || (endptr[0] != '\0') || (errno == ERANGE) || (isnan(dvalue))){
             return XREDIS_ERR;
         }
     }
     else if(xo->encoding == XREDIS_ENCODING_INT){
         dvalue = (long)(xo->ptr);
     }
     else{
         xredisPanic("Unknown String Encoding");
     }
     *target = dvalue;
     return XREDIS_OK;
}

int getLongDoubleFromObjectOrReply(xredisClient *c, xrobj *xo, long double *target, const char *msg){
    long double dvalue;
    if(getLongDoubleFromObject(xo, &dvalue) != XREDIS_OK){
        if(msg == NULL){
            addReplyError(c, "value is not a valid float");
        }
        else{
            addReplyError(c, (char *)msg);
        }
        return XREDIS_ERR;
    }
    *target = dvalue;
    return XREDIS_OK;
}

int getLongLongFromObject(xrobj *xo, long long *target){
     long long dvalue;
     char *endptr;
     if(xo == NULL){
         return 0;
     }
     redisAssertWithInfo(NULL, xo, xo->type == XREDIS_STRING);
     if(xo->encoding == XREDIS_ENCODING_RAW){
         errno = 0;
         dvalue = strtold(xo->ptr, &endptr);
         if((isspace(((char *)xo->ptr)[0])) || (endptr[0] != '\0') || (errno == ERANGE) || (isnan(dvalue))){
             return XREDIS_ERR;
         }
     }
     else if(xo->encoding == XREDIS_ENCODING_INT){
         dvalue = (long)(xo->ptr);
     }
     else{
         xredisPanic("Unknown String Encoding");
     }
     *target = dvalue;
     return XREDIS_OK;
}

int getLongLongFromObjectOrReply(xredisClient *c, xrobj *xo, long long *target, const char *msg){
    long long dvalue;
    if(getLongLongFromObject(xo, &dvalue) != XREDIS_OK){
        if(msg == NULL){
            addReplyError(c, "value is not a valid float");
        }
        else{
            addReplyError(c, (char *)msg);
        }
        return XREDIS_ERR;
    }
    *target = dvalue;
    return XREDIS_OK;
}

int getLongFromObjectOrReply(xredisClient *c, xrobj *xo, long *target, const char *msg){
    //since we already have getlonglong function, so we don'r need to write getlong now
    long long dvalue;
    if(getLongLongFromObjectOrReply(c, xo, &dvalue, NULL) != XREDIS_OK){
         return XREDIS_ERR;
    }
    if((dvalue < LONG_MIN) || (dvalue > LONG_MAX)){
        if(msg == NULL){
            addReplyError(c, "value is not a valid float");
        }
        else{
            addReplyError(c, (char *)msg);
        }
        return XREDIS_ERR;
    }
    *target = dvalue;
    return XREDIS_OK;
}

/*-------------------------supporting independent functions-----------------*/
char *strEncoding(int encoding){
     //return the string form of encode
    switch(encoding){
        case XREDIS_ENCODING_RAW:
            return "RAW";
        case XREDIS_ENCODING_INT:
            return "INT";
        case XREDIS_ENCODING_HT:
            return "hashtable";
        case XREDIS_ENCODING_LINKEDLIST:
            return "linkedlist";
        case XREDIS_ENCODING_ZIPLIST:
            return "ziplist";
        case XREDIS_ENCODING_INTSET:
            return "intset";
        case XREDIS_ENCODING_SKIPLIST:
            return "skiplist";
        default:
            return "Unknown";
    }
}

unsigned long estimateObjectIdleTime(xrobj *xo){
     //Given an object returns the min number of seconds the object was never requested,
     //using an approximated LRU algorithm.
    if(server.lruclock >= xrobj->lru){
        return (server.lruclock-xo->lru)*XREDIS_LRU_CLOCK_RESOLUTION;
    }
    else{
        return ((XREDIS_LRU_CLOCK_MAX-xo->lru)+server.lruclock)*XREDIS_LRU_CLOCK_RESOLUTION;
    }
}

/*---------------------OBJECT command---------------*/
//Look up key's value object without change its lru time and other properties
//helper function for the DEBUG command
xrobj *objectCommandLookup(xredisClient *c, xrobj* key){
    if((de = dictFind(c->db->dict, key->ptr)) == NULL){
         return NULL;
    }
    return (xrobj *)dictGetVal(de);
}

xrobj *objectCommandLookupOrReply(xredisClient *c, xrobj *key, xrobj *reply){
    xrobj *xo;
    if((xo = objectCommandLookup(c, key)) == NULL){
        addReply(c, reply);
    }
    return xo;
}

void objectCommand(xredisClient *c){
     //The realization of OBJECT command
     //1. check the refcount
     xrobj *xo;
    if((strcasecmp(c->argv[1]->ptr, "refcount") == 0) && (c->argc == 3)){
        if((xo = objectCommandLookupOrReply(c, c->argv[2], shared.nullbulk)) == NULL){
             return;
        }
        addReplyLongLong(c, o->refcount);
    }
    //2. check the encoding
    else if((strcasecmp(c->argv[1]->ptr, "encoding") == 0) && (c->argc == 3)){
        if((xo = objectCommandLookupOrReply(c, c->argv[2], shared.nullbulk)) == NULL){
            return;
        }
        addReplyBulkCString(c, strEncoding(xo));
    }
    //3. check the lru-time
    else if((strcasecmp(c->argv[1]->ptr, "idletime") == 0) && (c->argc == 3)){
        if((xo = objectCommandLookupOrReply(c, c->argv[2], shared.nullbulk)) == NULL){
            return;
        }
        addReplyLongLong(c, estimateObjectIdleTime(xo));
    }
    else{
        addReplyError(c,"Syntax error. Try OBJECT (refcount|encoding|idletime)");
    }
}


