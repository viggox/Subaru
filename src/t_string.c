#include "xredis.h"
#include <math.h>

/*---------------private functions--------------*/
int checkStringLength(xredisClient *xc, long long len);

/*------------STRING KEY COMMANDS--------------*/
//SET command
void setGenericCommand(xredisClient *xc, int nx, xrobj *key, xrobj *val){
    //generic function for set command
    //if nx!=0, update a existed key is not allowed
    if(nx && lookupKeyReadOrReply(xc,key,NULL)){
        addReply(xc, shared.czero);
        return;//the key is already existed!
    }
    setKey(xc->db,key,val);//put the key-value pair into database
    server.dirty++;
    //report to the client
    addReply(xc, nx?shared.cone:shared.ok);
}

void setCommand(xredisClient *xc){
    xc->argv[2] = tryObejctEncoding(xc->argv[2]);   //ADD AFTER
    setGenericCommand(xc, 0, xc->argv[1], xc->argv[2]);
}

void setnxCommand(xredisClient *xc){
    xc->argv[2] = tryObjectEncoding(xc->argv[2]);    //ADD AFTER, Redis seems only try to do this to value object, not to key object.
    setGenericCommand(xc, 1, xc->argv[1], xc->argv[2]);
}

//MSET command
void msetGenericCommand(xredisClient *xc, int nx){
    //generic function for mset command
    int i=1;
    xrobj *key, *val;
    if(((xc->argc-1) % 2) != 0){
        addReplyError(xc,"Wrong number of arguments for MSET!");
        return;
    }
    while(i < xc->argc-1){
        key = xc->argv[i];
        val = xc->argc[i+1];
        if(nx && lookupKeyReadOrReply(xc,key,NULL)){
             addReply(xc, shared.czero);
             i+=2;
             continue;
        }
        val = tryObejctEncoding(val);     //ADD AFTER
        setKey(xc->db, key, val);
        server.dirty++;
        addReply(xc, nx?shared.cone:shared.ok);
        i+=2;
    }
}

void msetCommand(xredisClient *xc){
     msetGenericCommand(xc, 0);
}

void msetnxCommand(xredisClient *xc){
     msetGenericCommand(xc, 1);
}

//GET command
int getGenericCommand(xredisClient *xc){
    //generic function for get command
    xrobj *val;
    if((val=lookupKeyReadOrReply(xc,xc->argv[1],NULL)) != NULL){
        if(val->type == XREDIS_STRING){
            addReplyBulk(xc, val);
            return XREDIS_OK;
        }
        else{
            addReply(xc, shared.wrongtypeerr);
            return XREDIS_ERR;
        }
    }
    return XREDIS_OK;
}

void getCommand(xredisClient *xc){
     getGenericCommand(xc);
}

//MGET command
void mgetCommand(xredisClient *xc){
    //function for mget command
    int i=1;
    xrobj *val;
    while(i<=argc-1){
        if((val=lookupKeyReadOrReply(xc,xc->argv[i],NULL)) != NULL){
            if(val->type == XREDIS_STRING){
                 addReplyBulk(xc, val);
            }
            else{
                 addReply(xc, shared.nullbulk);
            }
        }
        addReply(xc, shared.nullbulk);
        i++;
    }
}

//APPEND command
void appendCommand(xredisClient *xc){
    //function for append command
    char *oldstr, *newstr;
    xrobj *old, *append;
    size_t totlen;

    if((old=lookupKeyWrite(xc->db, xc->argv[1])) == NULL){
        xc->argv[2] = tryObejctEncoding(xc->argv[2]);
        dbAdd(xc->db, xc->argv[1], xc->argv[2]);
        incrRefCount(xc->argv[2]);
        totlen = stringObjectLen(xc->argv[2]);
    }
    else if(checkType(xc, old, XREDIS_STRING)){
         return;
    }
    else{
        append = xc->argv[2];
        totlen = stringObjectLen(old) + stringObjectLen(append);
        //Check whether the appended string length is illegal(ADD AFTER)!
        if(checkStringLength(xc, totlen) != XREDIS_OK){
            return;
        }
        //If the old val is encoded as INT _or_ is shared object(refcount != 1),Create a copy of old val
        //ADD AFTER
        if((old->refcount > 1) || (old->encoding != XREDIS_ENCODING_RAW)){
            xrobj *oldcopy = getDecodedObject(old);
            old = createStringObject(oldcopy->ptr, sdslen(oldcopy->ptr));
            decrRefCount(oldcopy);
            dbOverwrite(xc->db, xc->argv[1], old);
        }//I really cannot think like this!(EDUCATED)
        //append the string!
        old->ptr = sdscatsds(old->ptr, append->ptr);
        totlen = stringObjectLen(old);
    }

    server.dirty++;//update dirty number,since the key is modified
    signalModifiedKey(xc->db, xc->argv[1]);//ADD AFTER
    addReplyLongLong(xc, totlen);
}

//INCRBYFLOAT command
void incrbyfloatCommand(xredisClient *xc){
    //function for INCRBYFLOAT command
    long double oldval, incr;
    xrobj *old, *new;
    if(getlLongDoubleFromObjectOrReply(xc, xc->argv[2], &incr, NULL) != XREDIS_OK){
         return;
    }

    old = lookupKeyReadOrReply(xc->db,xc->argv[1],NULL);

    if(checkType(xc,old,XREDIS_STRING) && old){
        return;
    }
    else{
        if(getLongDoubleFromObjectOrReply(xc, old, &oldval, NULL) != XREDIS_OK){
            return;
        }
        oldval += incr;
        if((isnan(oldval)) || (isinf(oldval))){
            addReplyError(xc, "Increment will produce NaN or Infinity");
            return;
        }
        new = createStringObjectFromLongDouble(oldval);
        if(old){
            dbOverwrite(xc->db, xc->argv[1], new);
        }
        else{
            dbAdd(xc->db, xc->argv[1], new);
        }
        server.dirty++;
        signalModifiedKey(xc->db, xc->argv[1]);
        addReplyBulk(c, new);
    }

    aux = createStringObject("SET",3);
    rewriteClientCommandArgument(c,0,aux);
    decrRefCount(aux);
    rewriteClientCommandArgument(c,2,new);//ADD AFTER & I don't understand yet....
}

// INCR, DECR, INCRBY, DECRBY command
void incrDecrGenericCommand(xredisClient *xc, long long incr){
     //generic function for these four commands
     long long oldval;
     xrobj *old, *new;

     old = lookupKeyReadOrReply(xc->db,xc->argv[1],NULL);
     if(checkType(xc,old,XREDIS_STRING) && old){
         return;
     }

     if(getLongLongFromObjectOrReply(xc, old, &oldval, NULL) != XREDIS_OK){
         return;
     }
     oldval += incr;
     //Check for value overflow
     if(((oldval>0) && (incr>0) && (incr<LLONG_MAX-oldval)) || ((oldval<0) && (incr<0) &&(incr>LLONG_MIN-oldval))){
         addReplyError(xc, "Increment or decrement will overflow");
         return;
     }
     new = createStringObjectFromLongLong(oldval);
     if(old){
         dbOverwrite(xc->db,xc->argv[1],new);
     }
     else{
         dbAdd(xc->db, xc->argv[1], new);
     }

     server.dirty++;
     signalModifiedKey(xc->db, xc->argv[1]);
     addReplyBulk(c, new);
}

void incrCommand(xredisClient *xc){
     incrDecrGenericCommand(xc, 1);
}

void decrCommand(xredisClient *xc){
    incrDecrGenericCommand(xc, -1);
}

void incrbyCommand(xredisClient *xc){
     long long incr;
     if((getLongLongFromObjectOrReply(xc, xc->argv[2], &incr, NULL)) != XREDIS_OK){
         return;
     }
     incrDecrGenericCommand(xc, incr);
}

void decrbyCommand(xredisClient *xc){
     long long decr;
     if((getLongLongFromObjectOrReply(xc, xc->argv[2], &decr, NULL)) != XREDIS_OK){
         return;
     }
     incrDecrGenericCommand(xc, -decr);
}

void strlenCommand(xredisClient *xc){
    xrobj *o;
    if(o=lookupKeyReadOrReply(xc,xc->argv[1],shared.czero) == NULL){
        return;
    }
    else if(checkType(xc, o, XREDIS_STRING)){
        return;
    }
    else{
        addReplyLongLong(xc, stringObjectLen(o));
    }
}

void setrangeCommand(xredisClient *xc){
    xrobj *old;
    int offset, oldlen, newlen;
    sds addval;

    addval = xc->argv[3]->ptr;

    if(getLongLongFromObjectOrReply(xc, xc->argv[2], &offset, NULL) != XREDIS_OK){
        return;
    }
    if(offset < 0){
        addReplyError(xc, "offset is out of range!");
        return;
    }

    old=lookupKeyWrite(xc,xc->argv[1]);

    if(old){
        if(checkType(xc, old, XREDIS_STRING)){
            addReply(xc, shared.wrongtypeerr);
            return;
        }
        oldlen = stringObjectLen(old);
        if(sdslen(addval) == 0){
            addReplyLongLong(xc, oldlen);
            return;
        }

        newlen = sdslen(addval)+offset;
        if(checkStringLength(xc, newlen) != XREDIS_OK){
             return;
        }

        if((old->refcount != 1) || (old->encoding != XREDIS_ENCODING_RAW)){
            xrobj *oldcopy = getDecodedObject(old);
            old = createStringObject(old->ptr, sdslen(old->ptr));
            decrRefCount(oldcopy);
            dbOverwrite(xc->db, xc->argv[1], old);
        }
    }
    else{
        if(sdslen(addval) == 0){
            addReply(xc, shared.czero);
            return;
        }
        newlen = sdslen(addval) + offset;
        if(checkStringLength(xc, newlen) != XREDIS_OK){
            return;
        }

        old = createStringObject("",0);
        dbAdd(xc->db, xc->argv[1], old);
    }

    if(sdslen(addval) > 0){
        old->ptr=sdsgrowzero(old->ptr, sdslen(addval)+offset);
        memcpy(old->ptr+offset, addval, sdslen(addval));
        signalModifiedKey(xc->db, xc->argv[1]);
        server.dirty++;
    }

    addReplyError(xc, stringObjectLen(old));
}

void getrangeCommand(xredisClient *xc){
    int start, end;
    size_t len;
    xrobj *xo;

    if((getLongLongFromObjectOrReply(xc,xc->argv[2],&start,NULL) != XREDIS_OK) &&
            (getLongLongFromObjectOrReply(xc, xc->argv[3], &end, NULL)) != XREDIS_OK){
        return;
    }

    if(((xo=lookupKeyReadOrReply(xc, xc->argv[1], shared.nullbulk)) == NULL) || (checkType(xc, xo, XREDIS_STRING))){
         return;
    }

    len = stringObjectLen(xo);
    if(start<0) start += len;
    if(end<0) end += len;
    if(start<0) start=0;
    if(end<0) end=0;
    if((unsigned)end > len) end = len-1;

    if(start > end){
        addReplyBulk(xc, shared.emptybulk);
    }
    else{
        addReplyBulkCBuffer(xc, (char *)(xo->ptr)+start, end-start+1);
    }
}

/*------------private functions------------------*/
int checkStringLength(xredisClient *xc, long long len){
    long long  limit = 512*1024*1024;
    if(len <= limit){
        return XREDIS_OK;
    }
    else{
        addReplyError(xc, "string exceeds maximum allowed size(512MB)");
        return XREDIS_ERR;
    }
}
