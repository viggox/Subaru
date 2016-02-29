#include "xredis.h"
/*--------------------------Marcos--------------------------------------*/
#define LIST_START  0
#define LIST_END    1
/*-----------------------Utilties----------------------------*/
void signalListAsReady(xredisClient *xc, xrobj *xo);
/*--------------------specific LIST type data polymorphic handler-----------------------*/
//listType polymorphic iterator
listTypeIterator *listTypeInitIterator(xrobj *subject, long index, int direction){
    //Create and initialize a iterator;
    lti = zmalloc(sizeof(*listTypeIterator));
    if(lti == NULL) return NULL;
    //initialization
    lti->subject = subject;

    if((lti->encoding = subject->encoding) == XREDIS_ENCODING_ZIPLIST){
        lti->zl = ziplistIndex(subject->ptr, index);
        lti->ln = NULL;
    }
    else if((lti->encoding = subject->encoding) == XREDIS_ENCODING_LINKEDLIST){
        lti->ln = listIndex(lti, index);
        lti->zl = NULL;
    }
    else{
        xredisPanic("Unknown list encoding");
    }

    lti->direction = direction;
    return lti;
}

void listTypeReleaseIterator(listTypeIterator *lti){
    //Release the iterator;
    zfree(lti);
}

int listTypeNext(listTypeIterator *lti, listTypeEntry *entry){
    //return the entry iterator now points, and move the iterator to the next node;
    //return 1 if successed, else return 0;
    entry->lti = lti;
    if((lti->zl) || (lti->ln)){
        if(lti->encoding == XREDIS_ENCODING_ZIPLIST){
            entry->zl = lti->zl;
            if(lti->direction == XREDIS_START_HEAD){
                lti->zl = ziplistNext(lti->subject->ptr, lti->zl);
            }
            else{
                lti->zl = ziplistPrev(lti->subject->ptr, lti->zl);
            }
            return 1;
        }
        else if(lti->encoding == XREDIS_ENCODING_ZIPLIST){
            entry->ln = lti->ln;
            if(lti->direction == XREDIS_START_START){
                lti->ln = lti->ln->next;
            }
            else{
                 lti->ln = lti->ln->prev;
            }
            return 1;
        }
        else{
            xredisPanic("Unknown list encoding!");
        }
    }
    return 0;
}

xrobj *listTypeGet(listTypeEntry *entry){
    //return the entry iterator now points;
    xrobj *xo;

    if(entry->lti->encoding == XREDIS_ENCODING_ZIPLIST){
        unsigned char *str;
        unsigned int len;
        long long val;

        if(ziplistGet(entry->zl, &str, &len, &val)){
            if(val){
                xo = createStringObjectFromLongLong(val);
            }
            else{
                xo = createStringObject((char*)str, len);
            }
        }
    }
    else if(entry->lti->encoding == XREDIS_ENCODING_LINKEDLIST){
        xo = listNodeValue(entry->ln);
        incrRefCount(xo);  //ADD AFTER
    }
    else{
         xredisPanic("Unknown list encoding")
    }

    return xo;
}

/*---------------encoding type conversion----------------------*/
int listTypeIfNeedConversion(xrobj *list, xrobj *value){
    //check whether the list needed to handled with type encoding when add value in
    //return 1 if need conversion, 0 if not
    xredisAssert(value->type == XREDIS_STRING);

    if(list->encoding == XREDIS_ENCODING_LINKEDLIST){
        return 0;
    }
    else if(list->encoding == XREDIS_ENCODING_ZIPLIST){
        unsigned int olen;

        olen = ziplistLen(list->ptr);
        if(olen+1 >= LIST_MAX_ZIPLIST_ENTRIES){
            return 1;
        }
        else{
            if(value->encoding == XREDIS_ENCODING_INT){
                //long type never exceeds 64 bits
                return 0;
            }
            else if(sdslen(value->ptr) > LIST_MAX_ZIPLIST_VALUE){
                return 1;
            }
            else{
                 return 0;
            }
        }
    }
    else{
         xredisPanic("Unknown list encoding");
    }
}

void listTypeConvert(xrobj *list){
    //convert the list's encoding from ziplist to adlist;
    xrobj *nlist, *node;
    listTypeIterator *lti;
    listTypeEntry entry;

    xredisAssert(list->encoding == XREDIS_ENCODING_ZIPLIST);

    nlist = createListObject();
    lti = listTypeInitIterator(list, 0, XREDIS_START_HEAD);

    while(listTypeNext(lti, &entry)){
        listAddNodeTail(nlist, listTypeGet(&entry));
    }
    listTypeReleaseIterator(lti);
    freeListObject(list);
}

/*-----------------specific LIST type manipulation function--------------------*/
void listTypePush(xrobj *list, xrobj *node, int where){
    //push function for specific LIST type;
    if(checkType(xc, list, XREDIS_LIST)){
        return;
    }

    if(list->encoding == XREDIS_ENCODING_ZIPLIST){
        //change the INT to RAW or memcopy cannot work in ziplistPush;
        node = getDecodedObject(node);
        //check and execute type conversion;
        if(listTypeIfNeedConversion(list, node)) listTypeConvert(list);

        if(where == LIST_START){
            list->ptr = ziplistPush(list->ptr, node->ptr, sdslen(node->ptr), ZIPLIST_ENTRY_HEAD);
        }
        else{
            list->ptr = ziplistPush(list->ptr, node->ptr, sdslen(node->ptr), ZIPLIST_ENTRY_END);
        }
        decrRefCount(node);
        //since the ziplist preserve the copy string which node->ptr points, free old object is okay;
    }
    else if(list->encoding == XREDIS_ENCODING_LINKEDLIST){
        if(where == LIST_START){
            list->ptr = listAddNodeHead(list->ptr, node->ptr);
            listTryConversion(list);
        }
        else{
             list->ptr = listAddNodeTail(list->ptr, node->ptr);
             listTryConversion(list);
        }
        incrRefCount(node); //Add since the node is used by adlist function, incr the refcount;
    }
    else{
         xredisPanic("Unknown list encoding!");
    }
}

xrobj *listTypePop(xrobj *list, int where){
    //pop function for specific LIST type;
    unsigned char *pzl, *str;
    xrobj *value, *pln;
    unsigned int slen;
    long long sval;

    if(list->encoding == XREDIS_ENCODING_ZIPLIST){
        if(where == LIST_START){
            pzl = ziplistIndex(list->ptr, 0);
        }
        else{
            pzl = ziplistIndex(list->ptr, -1);
        }
        if(ziplistGet(pzl, &str, &slen, &sval)){
            if(str){
                value = createStringObject((char *)str, slen);
            }
            else{
                value = createStringObjectFromLongLong(sval);
            }
            list->ptr = ziplistDelete(list->ptr, pzl, 1);
        }
    }
    else if(list->encoding == XREDIS_ENCODING_LINKEDLIST){
        if(where == LIST_START){
            pln = listFirst(list->ptr);
        }
        else{
            pln = listLast(list->ptr);
        }
        if(pln != NULL){
             value = pln->value;
             incrRefCount(value);
             listDelNode(list->ptr, pln);
        }
    }
    else{
        xredisPanic("Unknown list encoding");
    }

    return value;
}

unsigned long listTypeLength(xrobj *xo){
    if(xo->encoding == XREDIS_ENCODING_ZIPLIST){
        return ziplistLen(xo->ptr);
    }
    else if(xo->encoding == XREDIS_ENCODING_LINKEDLIST){
        return listLength(xo->ptr);
    }
    else{
         xredisPanic("Unknown encoding type");
    }
}

/*--------------------LIST commands realization---------------------------*/
void pushGenericCommand(xredisClient *xc, int where){
    //generic command for LPUSH and RPUSH command
    xrobj *val;
    int i, pushed=0;
    if((val = lookupKeyWrite(xc->db,key)) && checkType(xc, val, XREDIS_LIST)){
        return;
    }
    if(val == NULL){
        signalListAsReady(xc, xc->argv[1]);
        //check whether the key is in blocking keys dict, if so, create readyList and add it to server.ready_keys
        val = createZiplistObject();
        dbAdd(xc->db, xc->argv[1], val);
    }
    //push the key into LIST
    for(i=2; i<argc; i++){
        xc->argv[i] = tryObjectEncoding(xc->argv[i]); //ADD AFTER, for saving space!
        listTypePush(val, xc->argv[i], where);
        pushed++;
    }
    addReplyLongLong(xc, listTypeLength(val)); //Report to client the length of modified LIST
    if(pushed){
         signalModifiedKey(xc->db, xc->argv[1]);
    }
    server.dirty+=pushed;
}

void lpushCommand(xredisClient *xc, int where){
    //LPUSH command;
    pushGenericCommand(xc, LIST_START);
}

void rpushCommand(xredisClient *xc, int where){
    //RPUSH command;
    pushGenericCommand(xc, LIST_END);
}

void popGenericCommand(xredisClient *xc, int where){
     //generic function for LPOP, RPOP command
     xrobj *xo, *value;
     //Find the key's value or Reply nullbulk;
     xo = lookupKeyWriteOrReply(xc, xc->argv[1], shared.nullbulk);
     if(xo == NULL || checkType(xc, xo, XREDIS_LIST)){
          return;
     }

     value = listTypePop(xo, where);
     if(value == NULL){
          addReply(xc, shred.nullbulk);
     }
     else{
          addReplyBulk(xc, value);
          decrRefCount(value);
          //delete the key-val pair from database if the LIST is null;
          if(listTypeLength(xo) == 0){
              dbDelete(xc->db, xc->argv[1]);
          }
          //common manipulations after modifing a key;
          signalModifiedKey(xc->db, xc->argv[1]);
          server.dirty++;
     }
}

void lpopCommand(xredisClient *xc){
    popGenericCommand(xc, LIST_START);
}

void rpopCommand(xredisClient *xc){
    popGenericCommand(xc, LIST_END);
}

void llenCommand(xredisClient *xc){
     xrobj *xo;
     unsigned long len;

     xo = lookupKeyReadOrReply(xc, xc->argv[1], shared.czero);
     if(xo == NULL || checkType(xc, xo, XREDIS_LIST)){
          return;
     }

     len = listTypeLength(xo);
     addReplyLongLong(xc, len);
}
