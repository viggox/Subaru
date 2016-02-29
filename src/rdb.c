#include "redis.h"
#include "zipmap.h"
#include "endianconv.h"

#include <math.h>
#include <sys/types.h>
#include <sys/time.h>
#include <sys/resource.h>
#include <sys/wait.h>
#include <arpa/inet.h>
#include <sys/stat.h>

static int rdbWriteRaw(xrio *rdb, void *p, size_t len){
    /*write len size content at p to rdb, return the bytes written if success*/
    if(rdb && xrioWrite(rdb, p, len)){
        return -1;
    }
    else{
        return len;
    }
}

int rdbSaveType(xrio *rdb, unsigned char type){
    /*write TYPE into rdb file*/
    return rdbWriteRaw(rdb, &type, 1);
}

int rdbLoadType(xrio *rdb){
    /*read TYPE from rdb file*/
    unsigned char type;
    if((xrioRead(rdb, &type, 1)) == 0){
        return -1;
    }
    return type;
}

time_t rdbLoadTime(xrio *rdb){
    /*read 4 bytes TIME from rdb file*/
    int32_t t;
    if((xrioRead(rdb, &t, 4)) == 0){
        return -1;
    }
    return t;
}

int rdbSaveMillisecondTime(xrio *rdb, long long t){
    /*write 8 bytes ms_time with milliseconds unit*/
    return rdbWriteRaw(rdb &t, 8);
}

long long rdbLoadMillisecondTime(xrio *rdb){
    /*read 8 bytes ms_time with milliseconds unit from rdb file*/
    long long t;
    if((xrioRead(rdb, &t, 8)) == 0){
         return -1;
    }
    return t;
}

int rdbSaveLen(xrio *rdb, uint32_t len){
    /*preserve an encoded length,the first and second bit are used to record type of encoding*/
    /*return the length of the encode*/
    unsigned char buf[2];

    if(len < (1<<6)){
        buf[0] = (len & 0xFF)|(XREDIS_RDB_6BITLEN << 6);
        if(rdbWriteRaw(rdb, buf, 1) == -1){
            return -1; //Don't forget the judgement after Writing or Reading!
        }
        return 1;
    }
    else if(len < (1<<14)){
        buf[0] = ((len>>8) & 0xFF)|(XREDIS_RDB_14BITLEN << 6);
        buf[1] = len & 0xFF;
        if(rdbWriteRaw(rdb, buf, 2) == -1){
             return -1;
        }
        return 2;
    }
    else{
        buf[0] = (XREDIS_RDB_32BITLEN << 6);
        if(rdbWriteRaw(rdb, buf, 1) == -1){
             return -1;
        }
        /*the convertion of endian from host to net*/
        len = htonl(len);

        if(rdbWrite(rdb, &len, 4) == -1){
            return -1;
        }
        return 5;
    }
}

uint32_t rdbLoadLen(xrio *rdb, int *isencoded){
    /*read out an encoded length, set isencoded as 1 if integer case*/
    char buf[2];
    unsigned int len;
    int type, inttype;

    if((xrioRead(rdb, buf, 1)) == 0){
         return XREDIS_RDB_LENERR;
    }
    type = (buf[0] && 0xC0)>>6;
    if(type == XREDIS_RDB_ENCVAL){
        /*INT case, return encode type directly*/
        if(isencoded){
            *isencoded = 1; //Don't forget to check the pointer is NULL or not before * it!!!!
        }
        inttype = buf[0] && 0x3F;

        return inttype;
    }
    else if(type == XREDIS_RDB_6BITLEN){
         len = buf[0] && 0x3F;
    }
    else if(type == XREDIS_RDB_14BITLEN){
        if((xrioRead(rdb, buf+1, 1)) == 0){
             return XREDIS_RDB_LENERR;
        }
        len = ((buf[0] && 0x3F)<<8)|(buf[1]);
    }
    else if(type == XREDIS_RDB_32BITLEN){
        if((xrioRead(rdb, &len, 4)) == 0){
            return XREDIS_RDB_LENERR;
        }
        len = ntol(len);
    }
    /*RAW case*/
    if(isencoded){
         *isencoded = 0;
    }

    return len;
}

int rdbEncodeInteger(long long value, unsigned char *enc){
    /*encode an integer value, write the encode into enc*/
    /*return bytes needed for encoding if succeed, else return 0*/
    if((value >= -(1<<7)) && (value < (1<<7))){
        enc[0] = (XREDIS_RDB_ENCVAL <<6)|XREDIS_RDB_ENC_INT8;
        enc[1] = value & 0xFF;
        return 2;
    }
    else if((value >= -(1<<15)) && (value < (1<<15))){
        enc[0] = (XREDIS_RDB_ENCVAL <<6)|XREDIS_RDB_ENC_INT16;
        enc[1] = value & 0xFF;
        enc[2] = (value>>8) & 0xFF;
        return 3;
    }
    else if((value >= -((long long)1<<31)) && (value < ((long long)1<<31))){
        enc[0] = (XREDIS_RDB_ENCVAL <<6)|XREDIS_RDB_ENC_INT32;
        enc[1] = value & 0xFF;
        enc[2] = (value>>8) & 0xFF;
        enc[3] = (value>>16) & 0xFF;
        enc[4] = (value>>24) & 0xFF;
        return 5;
    }
    else{
         return 0;
    }
}

xrobj *rdbLoadIntegerObject(xrio *rdb, int enctype, int encode){
    /*load the integer from RDB and turn it into integer object*/
    char enc[4];
    long long val;

    if(enctype == XREDIS_RDB_ENC_INT8){
        if((xrioRead(rdb, enc, 1)) == 0){
             return NULL;
        }
        val = (int8_t)enc[0];
    }
    else if(enctype == XREDIS_RDB_ENC_INT16){
        if((xrioRead(rdb, enc, 2)) == 0){
            return NULL;
        }
        val = (int16_t)(enc[0]|(enc[1]<<8));
    }
    else if(enctype == XREDIS_RDB_ENC_INT32){
        if((xrioRead(rdb, enc, 4)) == 0){
            return NULL;
        }
        val = (int32_t)(enc[0]|(enc[1]<<8)|(enc[2]<<16)|(enc[3]<<24));
    }
    else{
        val = 0      //Avoid warning!
        xredisPanic("Unknown RDB integer encoding type!")
    }

    if(encode){
        return createStringObjectFromLongLong(val);
    }
    else{
        return createObject(XREDIS_STRING, sdsfromlonglong(val));
    }

}

int rdbTryIntegerEncoding(char *s, size_t len, unsigned char *enc){
    /*Try to preserve a string with the form of integer*/
    /*return bytes needed if success, return 0 if failed*/
    long long value;
    char *endptr, *svalue;

    value = strtoll(s, &endptr, 10);
    if(endptr != '\0'){
        return 0;
    }
    ll2string(svalue, len, value);

    if(strcmp(s, svalue) != 0){
        return 0;
    }

    return rdbEncodeInteger(value, enc);
}

int rdbSaveRawString(xrio *rdb, unsigned char *s, size_t len){
    /*save the string to rdb file with the form of [len][string]*/
    /*if the string could be saved with the form of integer, then do it*/
    /*return bytes used to encode and save it*/
    char buf[5];
    int savelen;
    /*special case which string could be parsed and save as integer*/
    if((savelen = rdbTryIntegerEncoding(s, len, buf)) > 0){
        if((rdbWriteRaw(rdb, buf, savelen)) == -1){
             return -1;
        }
        return savelen;
    }
    /*the case of ordianry string, encode and save with [len][string] form*/
    if((savelen = rdbSaveLen(rdb, len)) == -1){
        return -1;
    }
    /*the len has been encoded in, now write the string itself to rdb!*/
    if((rdbWriteRaw(rdb, s, len)) == -1){
        return -1;
    }
    savelen += len;

    return savelen;
}

int rdbSaveLongLongAsStringObject(xrio *rdb, long long value){
    /*preserve a long long value as string, or encode it directly as long long*/
    unsigned char buf[32];
    int intlen;
    unsigned int slen, savelen=0;

    if(intlen = rdbEncodeInteger(value, buf)){
        return rdbWriteRaw(rdb, buf, intlen);
    }
    else{
        slen = ll2string((char *)buf, 32, value);
        if((savelen = rdbSaveLen(rdb, slen)) == -1){
             return -1;
        }
        if((rdbWriteRaw(rdb, buf, slen)) == -1){
             return -1;
        }
        savelen += slen;

        return savelen;
    }
}

int rdbSaveStringObject(xrio *rdb, xrobj *xo){
    /*write a string of redis object into rdb file*/
    /*consider the case of object with INT encoding or RAW encoding*/
    if(xo->encoding == XREDIS_ENCODING_INT){
        return rdbSaveLongLongAsStringObject(rdb, (long long)(xo->ptr));
    }
    else if(xo->encoding == XREDIS_ENCODING_RAW){
         return rdbSaveRawString(rdb, xo->ptr, sdslen(xo->ptr));
    }
    else{
         xredisPanic("Unknown String Object Encoding!")
    }
}

xrobj *rdbGenericLoadStringObject(xrio *rdb, int encode){
    /*read string from rdb file, and return xrobj which hold that string*/
    uint32_t len;
    int isencoded;

    if((len = rdbLoadLen(rdb, &isencoded)) == XREDIS_RDB_LENERR){
         return NULL;
    }
    if(isencode){
        return rdbLoadIntegerObject(rdb, len, encode);
    }
    else{
        sds s = sdsnewlen(NULL, len);
        if((len && xrioRead(rdb, s, len)) == 0){   //ADDED AFTER: check len == 0;
            sdsfree(s);            //ADDED AFTER
            return NULL;
        }
        return createObject(XREDIS_STRING, s);
    }
}

xrobj *rdbLoadStringObject(xrio *rdb){
    /*possible integer value turned string before saving*/
    return rdbGenericLoadStringObject(rdb, 0);
}

xrobj *rdbLoadEncodedStringObject(xrio *rdb){
    /*possible string turned into integer before saving*/
    return rdbGenericLoadStringObject(rdb, 1);
}

int rdbSaveDoubleValue(xrio *rdb, double val){
    /*preserve double with the form of [8bits len][double]*/
    /*253 means NaN, 254 means +infinity, 255 means -infinity*/
    unsigned char buf[128];

    if(isnan(val)){
        buf[0] = 253;
    }
    else if(!isinfinite(val)){
        len = 1;
        if(val<0){
            buf[0] = 255;
        }
        else{
            buf[0] = 254;
        }
    }
    else{
#if (DBL_MANT_DIG >= 52) && (LLONG_MAX == 0x7fffffffffffffffLL)
        double min = -4503599627370495; /* (2^52)-1 */
        double max = 4503599627370496; /* -(2^52) */
        if((val>min) && (val<max) && (val == (double)((long long)val))){
            ll2string((char *)buf+1, sizeof(buf), (long long)val);
        }
        else
#endif
            snprintf((char *)buf+1, sizeof(buf)-1, "%.17g", val);
        buf[0] = strlen((char *)buf+1);
        len = buf[0]+1;
    }
    return rdbWriteRaw(rdb, buf, len);
}


int rdbLoadDoubleValue(xrio *rdb, double val){
    /*read the double value out from rdb and preserve it in val*/
    unsigned char buf[128];
    unsigned char len;

    if((xrioRead(rdb, &len, 1)) == 0){
        return -1;
    }
    if(len == 253){
        *val = R_Nan;
        return 0;
    }
    if(len == 254){
        *val = R_PosInf;
        return 0;
    }
    if(len == 253){
        *val = R_NegInf;
        return 0;
    }
    if((xrioRead(rdb, buf, len)) == 0){
        return -1;
    }
    buf[len] = '\0';
    sscanf(buf, "%lg", val);
    return 0;
}

int rdbSaveObjectType(xrio *rdb, xrobj *xo){
    /*write xredis object type into rdb file*/
    if(xo->type == XREDIS_STRING){
        return rdbSaveType(rdb, XREDIS_RDB_TYPE_STRING);
    }
    else if(xo->type == XREDIS_LIST){
        if(xo->encoding == XREDIS_ENCODING_LINKEDLIST){
            return rdbSaveType(rdb, XREDIS_RDB_TYPE_LIST);
        }
        else if(xo->encoding == XREDIS_ENCODING_ZIPLIST){
            return rdbSaveType(rdb, XREDIS_RDB_TYPE_LIST_ZIPLIST);
        }
        else{
            xredisPanic("UnKnown list encoding");
        }
    }
    else if(xo->type == XREDIS_SET){
        if(xo->encoding == XREDIS_ENCODING_HT){
            return rdbSaveType(rdb, XREDIS_RDB_TYPE_SET);
        }
        else if(xo->encoding == XREDIS_ENCODING_INTSET){
             return rdbSaveType(rdb, XREDIS_RDB_TYPE_SET_INTSET);
        }
        else{
            xredisPanic("Unknown set encoding");
        }
    }
    else if(xo->type == XREDIS_ZSET){
        if(xo->encoding == XREDIS_ENCODING_ZIPLIST){
            return rdbSaveType(rdb, XREDIS_RDB_TYPE_ZSET_ZIPLIST);
        }
        else if(xo->encoding == XREDIS_ENCODING_HT){
            return rdbSaveType(rdb, XREDIS_RDB_TYPE_ZSET);
        }
        else{
             xredisPanic("Unknown zset encoding");
        }
    }
    else if(xo->type == XREDIS_HASH){
        if(xo->encoding == XREDIS_ENCODING_HT){
            return rdbSaveType(rdb, XREDIS_RDB_TYPE_HASH);
        }
        else if(xo->encoding == XREDIS_ENCODING_ZIPLIST){
            return rdbSaveType(rdb, XREDIS_RDB_TYPE_HASH_ZIPLIST);
        }
        else{
             xredisPanic("Unknown hash encoding");
        }
    }
    else{
        xredisPanic("Unknown Object Type");
    }
}

int rdbLoadObjectType(xrio *rdb){
    /*load the TYPE from rdb file, and return it
     * if the TYPE is illegal, return -1*/
    int type;

    if((type = rdbLoadType(rdb)) == -1){
        return -1;
    }
    if(!rdbIsObjectType(type)){
        return -1;
    }
    return type;
}

int rdbSaveObject(xrio *rdb, xrobj *xo){
    /*write given xredis Object into rdb,return 0 if success
     * return -1 if fail */
    int n, nwritten = 0;
    size_t len;

    if(xo->type == XREDIS_STRING){
        if((nwritten = rdbSaveStringObject(rdb, xo)) == -1){
            return -1;
        }
    }
    else if(xo->type == XREDIS_LIST){
        if(xo->encoding == XREDIS_ENCODING_LINKEDLIST){
            /*write the length of linkedlist into rdb*/
            if((n = rdbSaveLen(rdb, listLength(xo->ptr))) == -1){
                return -1;
            }
            nwritten = n;
            /*create linked list iterator, traverse the list, save all nodes*/
            list *l = xo->ptr;
            listNode *ln;
            listIter *li = listGetIterator(l, AL_START_HEAD);
            xrobj *val;

            /*start to traverse*/
            while((ln = listNext(li)) != NULL){
                val = listNodeValue(ln);
                if((n = rdbSaveStringObject(rdb, val)) == -1){
                    return -1;
                }
                nwritten += n;
            }
            listReleaseIterator(li);
        }
        else if(xo->encoding == XREDIS_ENCODING_ZIPLIST){
            /*get the bytes the ziplist takes*/
            len = ziplistBlobLen((unsigned char *)xo->ptr);
            /*save the ziplist with the form of a large string*/
            if((n = rdbSaveRawString(rdb, xo->ptr, len)) == -1){
                 return -1;
            }
            nwritten += n;
        }
        else{
             xredisPanic("Unknown LIST encoding");
        }
    }
    else if(xo->type == XREDIS_SET){
        if(xo->encoding == XREDIS_ENCODING_HT){
            dict *d = xo->ptr;
            dictEntry *de;
            dictIterator *di;
            xrobj *key;

            /*write the size of dict into rdb*/
            if((n = rdbSaveLen(rdb, dictSize(d))) ==-1){
                 return -1;
            }
            nwritten = n;
            /*create dict iterator, traverse the dict, save all keys*/
            di = dictGetIterator(d);
            /*start to traverse*/
            while((de = dictNext(di)) != NULL){
               key = dictGetKey(de);
               if((n = rdbSaveStringObject(rdb, key)) ==-1){
                    return -1;
                }
                nwritten += n;
            }
            dictReleaseIterator(di);
        }
        else if(xo->encoding == XREDIS_ENCODING_INTSET){
            /*get the bytes the intset takes*/
            len = intsetBlobLen((unsigned char *)xo->ptr);
            /*save the intset with the form of a large string*/
            if((n = rdbSaveRawString(rdb, xo->ptr, len)) == -1){
                 return -1;
            }
            nwritten += n;
        }
        else{
            xredisPanic("Unknown SET encoding");
        }
    }
    else if(xo->type == XREDIS_ENCODING_HASH){
        if(xo->encoding == XREDIS_ENCODING_HT){
            dict *d = xo->ptr;
            dictEntry *de;
            dictIterator *di;
            xrobj *key, *val;

            /*write the size of dict into rdb*/
            if((n = rdbSaveLen(rdb, dictSize(d))) == -1){
                 return -1;
            }
            nwritten = n;
            /*create dict iterator, traverse the dict, save all key-value pairs*/
            di = dictGetIterator(d);
            /*start to traverse*/
            while((de = dictNext(di)) != NULL){
                 key = dictGetKey(de);
                 val = dictGetVal(de);
                 if((n=rdbSaveStringObject(rdb, key)) == -1){
                     return -1;
                 }
                 nwritten += n;
                 if((n=rdbSaveStringObject(rdb, val)) == -1){
                     return -1;
                 }
                 nwritten += n;
            }
            dictReleaseIterator(di);
        }
        else if(xo->encoding == XREDIS_ENCODING_ZIPLIST){
            /*get the bytes the ziplist takes*/
            len = ziplistBlobLen((unsigned char *)xo->ptr);
            /*save the ziplist with the form of a large string*/
            if((n = rdbSaveRawString(rdb, xo->ptr, len)) == -1){
                 return -1;
            }
            nwritten += n;
        }
        else{
            xredisPanic("Unknown HASH encoding");
        }
    }
    else if(xo->type == XREDIS_ZSET){
        if(xo->encoding == XREDIS_ENCODING_SKIPLIST){
            zset *zs = xo->ptr;
            dictEntry *de;
            dictIterator *di;
            xrobj *key;
            double *score;

            if((n = rdbSaveLen(rdb, dictSize(zs->dict))) == -1){
                return -1;
            }
            nwritten = n;
            di = dictGetIterator(zs->dict);
            /*start to traverse*/
            while((de = dictNext(di)) != NULL){
                key = dictGetKey(de);
                score = dictGetVal(de);
                /*save member*/
                if((n=rdbSaveStringObject(rdb, key)) == -1){
                     return -1;
                }
                nwritten += n;
                if((n=rdbSaveDoubleValue(rdb, score)) == -1){
                    return -1;
                }
                nwritten += n;
            }
            dictReleaseIterator(di);
        }
        else if(xo->encoding == XREDIS_ENCODING_ZIPLIST){
            /*get the bytes the ziplist takes*/
            len = ziplistBlobLen((unsigned char *)xo->ptr);
            /*save the ziplist with the form of a large string*/
            if((n = rdbSaveRawString(rdb, xo->ptr, len)) == -1){
                 return -1;
            }
            nwritten += n;
        }
        else{
            xredisPanic("Unknown zset encoding");
        }
    }
    else{
        xredisPanic("Unknown Object Type");
    }

    return nwritten;
}

off_t rdbSavedObjectLen(xrobj *xo){
    /*return the size a xredis object taken on disk after saved*/
    int len = rdbSaveObject(NULL, o);
    xredisAssertWithInfo(NULL, o, len!=-1);
    return len;
}

int rdbSaveKeyValuePair(xrio *rdb, xrio* key, xrobj *val, long long expiretime, long long now){
    /*save key-value pair, type, and its expiretime*/
    /*return 1 if succeed, return 0 if key is expired, return -1 if fail*/
    if(expiretime != -1){
        if(expiretime < now){
            /*the key is expired*/
            return 0;
        }
        if(rdbSaveType(rdb, XREDIS_RDB_OPCODE_EXPIRETIME_MS) == -1){
             return -1;
        }
        if(rdbSaveMillisecondTime(rdb, expiretime) == -1){
            return -1;
        }
    }
    /*the expiretime is saved(if it has)*/
    if(rdbSaveObjectType(rdb, val) == -1){
         return -1;
    }
    if(rdbSaveStringObject(rdb, key) == -1){
        return -1;
    }
    if(rdbSaveObject(rdb, val) == -1){
         return -1;
    }
    /*saving succeed*/
    return 1;
}

int rdbSave(char *filename){
    /*save databases on disk*/
    /*return XREDIS_OK if succeed, return XREDIS_ERR if fail*/
    xredisDb *db;
    dictIterator *di = NULL;
    dictEntry *de;
    sds keystr;
    xrobj *key, *val;
    char tmpfile[256];
    char magic[10];
    int i;
    long long texpire, now = mstime();
    FILE *fp;
    xrio rdb;
    uint64_t cksum;

    /*create tmp-file name with form of "temp-<pid>.rdb"*/
    snprintf(tmpfile, 256, "temp-%d.rdb",(int)getpid());
    /*initialize rdb file*/
    fp = fopen(tmpfile, "w");
    if(fp == NULL){
        xredisLog(XREDIS_WARNING, "Failed opening .rdb for saving: %s", strerror(errno));
        return XREDIS_ERR;
    }
    xrioInitWithFile(&rdb, fp);
    /*write file header and version into rdb with the form of "XREDIS<VERSION>"*/
    snprintf(magic, 10, "XREDIS%04d", XREDIS_RDB_VERSION);
    if(rdbWriteRaw(&rdb, magic, 9) == -1){
         /*delete the tmpfile*/
         fclose(fp);
         unlink(tmpfile);
         /*send warning*/
         xredisLog(XREDIS_WARNING, "Write error saving DB on disk:%s", strerror(errorno));
         return XREDIS_ERR;
    }
    for(i=0; i<server.dbnum; i++){
        db = server.db+i;
        if(dictSize(db->dict) == 0){
            continue;
        }
        if((rdbSaveType(&rdb, XREDIS_RDB_OPCODE_SELECTDB) == -1)||
                (rdbSaveLen(&rdb, i) == -1)){
         /*delete the tmpfile*/
         fclose(fp);
         unlink(tmpfile);
         /*send warning*/
         xredisLog(XREDIS_WARNING, "Write error saving DB on disk:%s", strerror(errorno));
         return XREDIS_ERR;
        }
        /*create the iterator*/
        di = dictGetIterator(db->dict);
        /*traverse and save the key-value pairs*/
        while((de = dictNext(di)) != NULL){
            keystr = dictGetKey(de);
            key = initStaticStringObject(keystr);
            val = dictGetVal(de);
            texpire = getExpire(db, key);

            if(rdbSaveKeyValuePair(&rdb, key, val, texpire) == -1){
                /*delete the tmpfile*/
                fclose(fp);
                unlink(tmpfile);
                /*send warning*/
                xredisLog(XREDIS_WARNING, "Write error saving DB on disk:%s", strerror(errorno));
                /*release the iterator*/
                if(di){
                     dictReleaseIterator(di);
                }
                return XREDIS_ERR;
            }
        }
        dictReleaseIterator(di);
    }
    /*write EOF byte into RDB file*/
    if(rdbSaveType(&rdb, XREDIS_RDB_OPCODE_EOF) == -1){
        /*delete the tmpfile*/
        fclose(fp);
        unlink(tmpfile);
        /*send warning*/
        xredisLog(XREDIS_WARNING, "Write error saving DB on disk:%s", strerror(errorno));
        return XREDIS_ERR;
    }
    /*write check_sum into rdb file*/
    cksum = rdb.cksum; //cksum is a 8bytes unsigned int;
    memrev64ifbe(&cksum);
    xrioWrite(&rdb, &cksum, 8);
    /*Make sure data will not remain on the OS's output buffers*/
    fflush(fp);
    fsync(fileno(fp));
    fclose(fp);
    /*rename the tmpfile with input "filename"*/
    if(rename(tmpfile , filename) == -1){
        xredisLog(XREDIS_WARNING, "Error making RDB file finally a mature woman:%s", strerror(errno));
        unlink(tmpfile);
        return XREDIS_ERR;
    }
    xredisLog(XREDIS_NOTICE, "RDB file saved on disk");
    /*initialize database related server properties*/
    server.dirty = 0;
    server.lastsave = time(NULL);
    server.lastbgsave_status = XREDIS_OK;
    /*finally :)*/
    return XREDIS_OK;
}

int rdbSaveBackground(char *filename){
    /*use a forked child process to save rdb file, parent process continue handling commands*/
    pid_t child_pid;
    long long start;
    size_t private_dirty;

    /*exception:child pid already exists*/
    if(server.rdb_child_pid != -1){
         return XREDIS_ERR;
    }
    /*backup status before bg_save*/
    server.dirty_before_bgsave = server.dirty;
    /*preserve the current time*/
    start = ustime(NULL);
    if((child_pid = fork()) == 0){
        /* Child */
        int rtnval;
        /* the child doesn't accept network data */
        if(server.ipfd > 0){
            close(server.ipfd);
        }
        if(server.sofd > 0){
            close(server.sofd);
        }
        /* report the space save data taken */
        if(rtnval == XREDIS_OK){
            private_dirty = zmalloc_get_private_dirty();
            if(private_dirty){
                 xredisLog(XREDIS_NOTICE, "RDB: %lu MB of memory used by copy-on-write",
                         private_dirty/(1024*1024));
            }
        }
        exitFromChild((rtnval == XREDIS_OK) ? 0: 1);
    }
    else{
        /* Parent */
        server.stat_fork_time = ustime - start;
        if(child_pid == -1){
            /*child create failed*/
            xredisLog(XREDIS_WARNING, "Can't save in background: fork: %s",
                    strerror(errno));
            return XREDIS_ERR;
        }
        xredisLog(XREDIS_NOTICE, "Background saving started by pid %d", childpid);
        /*record the start time*/
        server.rdb_save_time_start = time(NULL);
        /*save child process id*/
        server.rdb_child_pid = child_pid;
        /*close rehashing, avoid copy-on-write*/
        updateDictResizePolicy();

        return XREDIS_OK;
    }
    return XREDIS_OK;
}

void rdbRemoveTempFile(pid_t child_pid){
    /*delete tmpfile*/
    char tmpfile[256];
    /*get tmpfile's name*/
    snprintf(tmpfile, 256, "temp-%d.rdb", (int)child_pid);
    unlink(tmpfile);
}

xrobj *rdbLoadObject(int rdbtype, xrio *rdb){
    /*load and return object from rdb file, type given rdbtype*/
    xredisLog(XREDIS_DEBUG, "LOADING OBJECT %d (at %d)\n", rdbtype, xrioTell(rdb));

    if(rdbtype == XREDIS_RDB_TYPE_STRING){
        xrobj *xo = rdbLoadEncodedStringObject(rdb);
        xo = tryObjectEncoding(xo);
    }
    else if(rdbtype == XREDIS_RDB_TYPE_LIST){
        unsigned int len;
        xrobj *xo, *ele;

        if((len = rdbLoadLen(rdb, NULL)) == XREDIS_RDB_LENERR){
            return NULL;
        }
        if(len > server.list_max_ziplist_entries){
            xo = createListObject();
        }
        else{
            xo = createZiplistObject();
        }
        while(len--){
            if((ele = rdbLoadEncodedStringObject(rdb)) == NULL){
                return NULL;
            }
            ele = tryObjectEncoding(ele);
            if((xo->encoding == XREDIS_ENCODING_ZIPLIST) &&
                    (ele->encoding == XREDIS_ENCODING_RAW) &&
                    (sdslen(ele->ptr) > server.list_max_ziplist_value)){
                listTypeConvert(xo, XREDIS_ENCODING_LINKEDLIST);
            }

            if(xo->encoding == XREDIS_ENCODING_ZIPLIST){
                dec = getDecodedObject(ele);
                xo->ptr = ziplistPush(xo->ptr, dec->ptr, sdslen(dec->ptr), XREDIS_TAIL);
                decrRefCount(dec);
                decrRefCount(ele);
            }
            else{
                 ele = tryObjectEncoding(ele);
                 listAddNodeTail(xo->ptr, ele);
            }
        }
    }
    else if(rdbtype == XREDIS_RDB_TYPE_HASH){
        size_t len;
        int ret;

        if((len = rdbLoadLen(rdb, NULL)) == XREDIS_RDB_LENERR){
            return NULL;
        }
        xo = createHashObject();
        /*type convert*/
        if(len > server.hash_max_ziplist_entries){
             hashTypeConvert(xo, XREDIS_ENCODING_HT);
        }

        while((xo->encoding == XREDIS_ENCODING_ZIPLIST) && (len>0)){
            xrobj *field, *value;

            len--;
            field = rdbLoadEncodedStringObject(rdb);
            if(field == NULL){
                 return NULL;
            }
            xredisAssert(field->encoding == XREDIS_ENCODING_RAW);
            value = rdbLoadStringObject(rdb);
            if(value == NULL){
                return NULL;
            }
            xredisAssert(value->encoding == XREDIS_ENCODING_RAW);
            /* Add pair to ziplist */
            xo->ptr = ziplistPush(xo->ptr, field->ptr, sdslen(field->ptr), ZIPLIST_TAIL);
            xo->ptr = ziplistPush(xo->ptr, value->ptr, sdslen(value->ptr), ZIPLIST_TAIL);
            /* Convert to hash table if size threshold is exceeded */
            if (sdslen(field->ptr) > server.hash_max_ziplist_value ||
                sdslen(value->ptr) > server.hash_max_ziplist_value)
            {
                decrRefCount(field);
                decrRefCount(value);
                hashTypeConvert(xo, REDIS_ENCODING_HT);
                break;
            }
            decrRefCount(field);
            decrRefCount(value);
        }
    }
    else if(rdbtype == XREDIS_RDB_TYPE_SET){
        /* Read list/set value */
        if ((len = rdbLoadLen(rdb,NULL)) == XREDIS_RDB_LENERR){
            return NULL;
        }

        /* Use a regular set when there are too many entries. */
        if (len > server.set_max_intset_entries){
            xo = createSetObject();
            /* It's faster to expand the dict to the right size asap in order
             * to avoid rehashing */
            if (len > DICT_HT_INITIAL_SIZE){
                dictExpand(xo->ptr,len);
            }
            else{
                xo = createIntsetObject();
            }
        }
        /* Load every single element of the list/set */
        for (i = 0; i < len; i++){
            long long llval;
            if ((ele = rdbLoadEncodedStringObject(rdb)) == NULL){
                return NULL;
            }
            ele = tryObjectEncoding(ele);
            if (xo->encoding == XREDIS_ENCODING_INTSET) {
                /* Fetch integer value from element */
                if (isObjectRepresentableAsLongLong(ele,&llval) == XREDIS_OK) {
                    xo->ptr = intsetAdd(xo->ptr,llval,NULL);
                } else {
                    setTypeConvert(xo,XREDIS_ENCODING_HT);
                    dictExpand(xo->ptr,len);
                }
            }
            /* This will also be called when the set was just converted
             * to a regular hash table encoded set */
            if (xo->encoding == REDIS_ENCODING_HT) {
                dictAdd((dict*)xo->ptr,ele,NULL);
            } else {
                decrRefCount(ele);
            }
        }
    }
    else if(rdbtype == XREDIS_RDB_TYPE_ZSET){
        /* Read list/set value */
        size_t zsetlen;
        size_t maxelelen = 0;
        zset *zs;

        if ((zsetlen = rdbLoadLen(rdb,NULL)) == XREDIS_RDB_LENERR) return NULL;
        xo = createZsetObject();
        zs = xo->ptr;

        /* Load every single element of the list/set */
        while(zsetlen--) {
            robj *ele;
            double score;
            zskiplistNode *znode;

            if ((ele = rdbLoadEncodedStringObject(rdb)) == NULL) return NULL;
            ele = tryObjectEncoding(ele);
            if (rdbLoadDoubleValue(rdb,&score) == -1) return NULL;

            /* Don't care about integer-encoded strings. */
            if (ele->encoding == XREDIS_ENCODING_RAW &&
                sdslen(ele->ptr) > maxelelen)
                    maxelelen = sdslen(ele->ptr);

            znode = zslInsert(zs->zsl,score,ele);
            dictAdd(zs->dict,ele,&znode->score);
            incrRefCount(ele); /* added to skiplist */
        }

        /* Convert *after* loading, since sorted sets are not stored ordered. */
        if (zsetLength(xo) <= server.zset_max_ziplist_entries &&
                maxelelen <= server.zset_max_ziplist_value){
            zsetConvert(xo,XREDIS_ENCODING_ZIPLIST);
        }
    }
    else if (  rdbtype == XREDIS_RDB_TYPE_LIST_ZIPLIST ||
               rdbtype == XREDIS_RDB_TYPE_SET_INTSET   ||
               rdbtype == XREDIS_RDB_TYPE_ZSET_ZIPLIST ||
               rdbtype == XREDIS_RDB_TYPE_HASH_ZIPLIST)
    {
        xrobj *aux = rdbLoadStringObject(rdb);

        if (aux == NULL) return NULL;

        xo = createObject(XREDIS_STRING,NULL);
        xo->ptr = zmalloc(sdslen(aux->ptr));
        memcpy(xo->ptr,aux->ptr,sdslen(aux->ptr));
        decrRefCount(aux);

        switch(rdbtype) {
            case XREDIS_RDB_TYPE_LIST_ZIPLIST:
                xo->type = XREDIS_LIST;
                xo->encoding = XREDIS_ENCODING_ZIPLIST;
                if (ziplistLen(xo->ptr) > server.list_max_ziplist_entries)
                    listTypeConvert(xo,XREDIS_ENCODING_LINKEDLIST);
                break;
            case XREDIS_RDB_TYPE_SET_INTSET:
                xo->type = XREDIS_SET;
                xo->encoding = XREDIS_ENCODING_INTSET;
                if (intsetLen(xo->ptr) > server.set_max_intset_entries)
                    setTypeConvert(xo,XREDIS_ENCODING_HT);
                break;
            case XREDIS_RDB_TYPE_ZSET_ZIPLIST:
                xo->type = XREDIS_ZSET;
                xo->encoding = XREDIS_ENCODING_ZIPLIST;
                if (zsetLength(xo) > server.zset_max_ziplist_entries)
                    zsetConvert(xo,XREDIS_ENCODING_SKIPLIST);
                break;
            case XREDIS_RDB_TYPE_HASH_ZIPLIST:
                xo->type = XREDIS_HASH;
                xo->encoding = XREDIS_ENCODING_ZIPLIST;
                if (hashTypeLength(xo) > server.hash_max_ziplist_entries)
                    hashTypeConvert(xo, XREDIS_ENCODING_HT);
                break;
            default:
                xredisPanic("Unknown encoding");
                break;
        }
    } else {
        xredisPanic("Unknown object type");
    }
    return o;
}

void startLoading(FILE *fp){
    /*update loading status*/
    struct stat sb;

    /*loading database*/
    server.loading = 1;
    server.loading_start_time = time(NULL);
    if(fstat(fileno(fp), &sb) == -1){
        server.loading_total_bytes = 1;
    }
    else{
         server.loading_total_bytes = sb.st_size;
    }
}

void loadingProgress(off_t pos){
    /*refresh loading progress info*/
    server.loading_loaded_bytes = pos;
    if (server.stat_peak_memory < zmalloc_used_memory()){
        server.stat_peak_memory = zmalloc_used_memory();
    }
}

void stop_loading(){
    /*set loading status as finished*/
    server.loading = 0;
}

int rdbLoad(char *filename){
    /*read rdb file and load object in it into memory*/
    uint32_t dbid;
    int type, rdbver;
    xredisDb *db = server.db;
    char buf[1024];
    long long texpire, now = mstime();
    long loops = 0;
    FILE *fp;
    xrio rdb;

    /*open file*/
    fp = fopen(filename, "r");
    if(!fp){
        errno = ENONENT;
        return XREDIS_ERR;
    }
    /*initialize rdb file*/
    xrioInitWithFile(&rdb, fp);
    if(server.rdb_checksum){
        rdb.update_cksum = xrioGenericUpdateChecksum(rdb, buf, len);
    }
    if(xrioRead(&rdb, buf, 9) == 0){
        xredisLog(XREDIS_WARNING,"Short read or OOM loading DB. Unrecoverable error, aborting now.");
        exit(1);
    }
    buf[9] = '\0';
    if(memcmp(buf, "XREDIS", 6) != 0){
        fclose(fp);
        xredisLog(XREDIS_WARNING,"Can't handle RDB format version %d",rdbver);
        errno = EINVAL;
        return XREDIS_ERR;
    }

    startLoading(fp);
    while(1) {
        robj *key, *val;
        expiretime = -1;

        if (!(loops++ % 1000)) {
            loadingProgress(xrioTell(&rdb));
            aeProcessEvents(server.el, AE_FILE_EVENTS|AE_DONT_WAIT);
        }

        /* Read type. */
        if ((type = rdbLoadType(&rdb)) == -1) goto eoferr;

        if (type == XREDIS_RDB_OPCODE_EXPIRETIME) {
            if ((expiretime = rdbLoadTime(&rdb)) == -1) goto eoferr;
            /* We read the time so we need to read the object type again. */
            if ((type = rdbLoadType(&rdb)) == -1) goto eoferr;
            /* the EXPIRETIME opcode specifies time in seconds, so convert
             * into milliesconds. */
            expiretime *= 1000;
        } else if (type == XREDIS_RDB_OPCODE_EXPIRETIME_MS) {
            /* Milliseconds precision expire times introduced with RDB
             * version 3. */
            if ((expiretime = rdbLoadMillisecondTime(&rdb)) == -1) goto eoferr;
            /* We read the time so we need to read the object type again. */
            if ((type = rdbLoadType(&rdb)) == -1) goto eoferr;
        }

        if (type == XREDIS_RDB_OPCODE_EOF)
            break;

        /* Handle SELECT DB opcode as a special case */
        if (type == XREDIS_RDB_OPCODE_SELECTDB) {
            if ((dbid = rdbLoadLen(&rdb,NULL)) == XREDIS_RDB_LENERR)
                goto eoferr;
            if (dbid >= (unsigned)server.dbnum) {
                xredisLog(REDIS_WARNING,"FATAL: Data file was created with a Redis server configured to handle more than %d databases. Exiting\n", server.dbnum);
                exit(1);
            }
            db = server.db+dbid;
            continue;
        }

        /* Read key */
        if ((key = rdbLoadStringObject(&rdb)) == NULL) goto eoferr;

        /* Read value */
        if ((val = rdbLoadObject(type,&rdb)) == NULL) goto eoferr;

        if (server.masterhost == NULL && expiretime != -1 && expiretime < now) {
            decrRefCount(key);
            decrRefCount(val);
            continue;
        }

        /* Add the new object in the hash table */
        dbAdd(db,key,val);

        /* Set the expire time if needed */
        if (expiretime != -1) setExpire(db,key,expiretime);

        decrRefCount(key);
    }

    /* Verify the checksum if RDB version is >= 5 */
    if (rdbver >= 5 && server.rdb_checksum) {
        uint64_t cksum, expected = rdb.cksum;

        if (xrioRead(&rdb,&cksum,8) == 0) goto eoferr;
        memrev64ifbe(&cksum);
        if (cksum == 0) {
            xredisLog(REDIS_WARNING,"RDB file was saved with checksum disabled: no check performed.");
        } else if (cksum != expected) {
            xredisLog(REDIS_WARNING,"Wrong RDB checksum. Aborting now.");
            exit(1);
        }

    fclose(fp);
    stopLoading();
    return XREDIS_OK;

eoferr: /* unexpected end of file is handled here with a fatal exit */
    xredisLog(XREDIS_WARNING,"Short read or OOM loading DB. Unrecoverable error, aborting now.");
    exit(1);
    return XREDIS_ERR; /* Just to avoid warning */
}

void backgroundSaveDoneHandler(int exitcode, int bysignal) {
    /*update server status by bgsave returns*/
    if (!bysignal && exitcode == 0) {
        xredisLog(XREDIS_NOTICE,"Background saving terminated with success");
        server.dirty = server.dirty - server.dirty_before_bgsave;
        server.lastsave = time(NULL);
        server.lastbgsave_status = XREDIS_OK;
    } else if (!bysignal && exitcode != 0) {
        xredisLog(REDIS_WARNING, "Background saving error");
        server.lastbgsave_status = XREDIS_ERR;
    } else {
        redisLog(XREDIS_WARNING,"Background saving terminated by signal %d", bysignal);
        rdbRemoveTempFile(server.rdb_child_pid);
        server.lastbgsave_status = XREDIS_ERR;
    }

    server.rdb_child_pid = -1;
    server.rdb_save_time_last = time(NULL)-server.rdb_save_time_start;
    server.rdb_save_time_start = -1;

    /* Possibly there are slaves waiting for a BGSAVE in order to be served
     * (the first stage of SYNC is a bulk transfer of dump.rdb) */
    updateSlavesWaitingBgsave(exitcode == 0 ? XREDIS_OK : XREDIS_ERR);
}

void saveCommand(xredisClient *xc) {
    if (server.rdb_child_pid != -1) {
        addReplyError(xc,"Background save already in progress");
        return;
    }

    if (rdbSave(server.rdb_filename) == XREDIS_OK) {
        addReply(xc,shared.ok);
    } else {
        addReply(xc,shared.err);
    }
}

void bgsaveCommand(xredisClient *xc) {
    if (server.rdb_child_pid != -1) {
        addReplyError(xc,"Background save already in progress");
    } else if (server.aof_child_pid != -1) {
        addReplyError(xc,"Can't BGSAVE while AOF log rewriting is in progress");
    } else if (rdbSaveBackground(server.rdb_filename) == XREDIS_OK) {
        addReplyStatus(xc,"Background saving started");
    } else {
        addReply(xc,shared.err);
    }
}

