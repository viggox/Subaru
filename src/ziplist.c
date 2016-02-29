#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <stdint.h>
#include <assert.h>
#include <limits.h>
#include "zmalloc.h"
#include "util.h"
#include "ziplist.h"
#include "endianconv.h"

#define ZIP_END 255     //the end of ziplist, zlend (0xFF is 255)
#define ZIP_BIGLEN 254  //limit for preventrylength

/* Different encoding/length possibilities */
#define ZIP_STR_MASK 0xc0   // 1100, 0000  mask code for string type
#define ZIP_INT_MASK 0x30   // 0011, 0000  mask code for integer type
#define ZIP_STR_06B (0 << 6)   //encoding for 1byte char
#define ZIP_STR_14B (1 << 6)   //encoding for 2byte char
#define ZIP_STR_32B (2 << 6)   //encoding for 5byte char
#define ZIP_INT_16B (0xc0 | 0<<4)  //encoding for int16_t type
#define ZIP_INT_32B (0xc0 | 1<<4)  //encoding for int32_t type
#define ZIP_INT_64B (0xc0 | 2<<4)  //encoding for int64_t type
#define ZIP_INT_24B (0xc0 | 3<<4)  //encoding for int24_t type
#define ZIP_INT_8B 0xfe       //encoding for int8_t type
/* 4 bit integer immediate encoding */
#define ZIP_INT_IMM_MASK 0x0f   /* 00001111 */
#define ZIP_INT_IMM_MIN 0xf1    /* 11110001 */
#define ZIP_INT_IMM_MAX 0xfd    /* 11111101 */
#define ZIP_INT_IMM_VAL(v) (v & ZIP_INT_IMM_MASK)
#define INT24_MAX 0x7fffff
#define INT24_MIN (-INT24_MAX - 1)
/* Macro to determine type */
#define ZIP_IS_STR(enc) (((enc) & ZIP_STR_MASK) < ZIP_STR_MASK)

/* Macros to get fields from ziplist */
#define ZIPLIST_BYTES(zl)       (*((uint32_t*)(zl)))
#define ZIPLIST_TAIL_OFFSET(zl) (*((uint32_t*)((zl)+sizeof(uint32_t))))
#define ZIPLIST_LENGTH(zl)      (*((uint16_t*)((zl)+sizeof(uint32_t)*2)))
#define ZIPLIST_HEADER_SIZE     (sizeof(uint32_t)*2+sizeof(uint16_t))   // 32*2 bit + 16 bit
#define ZIPLIST_ENTRY_HEAD(zl)  ((zl)+ZIPLIST_HEADER_SIZE)
#define ZIPLIST_ENTRY_TAIL(zl)  ((zl)+intrev32ifbe(ZIPLIST_TAIL_OFFSET(zl)))
#define ZIPLIST_ENTRY_END(zl)   ((zl)+intrev32ifbe(ZIPLIST_BYTES(zl))-1)

//the structure to store entry of ziplist
typedef struct zlentry {
    unsigned int prevrawlensize,    // size needed to store the length of previous entry
                 prevrawlen;        // the length of previous entry
    unsigned int lensize,           // size needed to store the length of current entry
                 len;               // the length of current entry
    unsigned int headersize;        // length of header
    unsigned char encoding;         // content type encoding
    unsigned char *p;               // content array
} zlentry;

/* Macros serve as functions */
//Macro to add the ZIPLIST_LENGTH property of ziplist with incr
#define ZIPLIST_INCR_LENGTH(zl,incr) {                                            \
    if (ZIPLIST_LENGTH(zl) < UINT16_MAX)                                          \
        ZIPLIST_LENGTH(zl) = intrev16ifbe(intrev16ifbe(ZIPLIST_LENGTH(zl))+incr); \
}
//Macro to take the encoding out from the ziplist entry, and then "pure" it
#define ZIP_ENTRY_ENCODING(ptr, encoding) do {                                    \
    (encoding) = (ptr[0]);                                                        \
    if ((encoding) < ZIP_STR_MASK) (encoding) &= ZIP_STR_MASK;                    \
} while(0)
//Macro to take the encoding, length, and size needed to store the length out from the entry
#define ZIP_DECODE_LENGTH(ptr, encoding, lensize, len) do {                       \
    ZIP_ENTRY_ENCODING((ptr), (encoding));                                        \
    if((encoding) < ZIP_STR_MASK) {                                               \
         /* the entry stores char array/string */                                 \
        if((encoding) == ZIP_STR_06B) {                                           \
            (lensize) = 1;                                                        \
            (len) = (((ptr)[0]) & 0x3f);                                          \
        }                                                                         \
        else if((encoding) == ZIP_STR_14B) {                                      \
             (lensize) = 2;                                                       \
             (len) = ((((ptr)[0]) & 0x3f) << 8 ) | ((ptr)[1]);                    \
        }                                                                         \
        else if((encoding) == ZIP_STR_32B) {                                      \
             (lensize) = 5;                                                       \
             (len) = ((ptr)[1] << 24)|((ptr)[2] << 16)|((ptr)[3] << 8)|((ptr)[4]);\
        }                                                                         \
        else{                                                                     \
             assert(NULL);                                                        \
        }                                                                         \
    }                                                                             \
    else{                                                                         \
         /* What stores in the entry is an integer */                             \
        (lensize) = 1;                                                            \
        (len) = zipIntSize(encoding);                                             \
    }                                                                             \
} while(0);                                                                       \
//Macro to return size needed to store prev_entry_length of entry
#define ZIP_DECODE_PREVLENSIZE(ptr, prevlensize) do {                             \
    if((ptr)[0] < ZIP_BIGLEN) {                                                   \
        (prevlensize) = 1;                                                        \
    }                                                                             \
    else{                                                                         \
        (prevlensize) = 5;                                                        \
    }                                                                             \
} while(0);                                                                       \
//Macro to return the prev_entry_length of pointed entry
#define ZIP_DECODE_PREVLEN(ptr, prevlensize, prevlen) do {                        \
     ZIP_DECODE_PREVLENSIZE(ptr,prevlensize);                                     \
    if((prevlensize) == 1){                                                       \
         (prevlen) = (ptr)[0];                                                    \
    }                                                                             \
    else if((prevlensize) == 5){                                                  \
         memcpy(&(prevlen),((char *)(ptr)) +1, 4);                                \
         memrev32ifbe(&prevlen);                                                  \
    }                                                                             \
} while(0);                                                                       \

/*---------------------------------------------------APIs-----------------------------------------*/
//------------ziplistNew---Create a new ziplist----------------O(1)
unsigned char *ziplistNew(void){
    unsigned char *pzl = zmalloc(ZIPLIST_HEADER_SIZE +1);
    //Set ziplist field-macros
    ZIPLIST_TAIL_OFFSET(pzl) = intrev32ifbe(ZIPLIST_HEADER_SIZE);
    ZIPLIST_BYTES(pzl) = intrev32ifbe(ZIPLIST_HEADER_SIZE+1);
    ZIPLIST_LENGTH(pzl) = 0;
    //Set the end of ziplist
    pzl[ZIPLIST_HEADER_SIZE] = 255;
    return pzl;
}

//---ziplistPush-----Create a new entry with given value and add it to the header or tail of ziplist----O(N) in average,O(N^2) worst
static zlentry zipEntry(unsigned char *p){
    //take out the contents and properties of entry pointed by p, stored in returning
    zlentry zle;
    ZIP_DECODE_PREVLEN(p, zle.prevrawlensize, zle.prevrawlen);
    ZIP_DECODE_LENGTH(p+zle.prevrawlensize, zle.encoding, zle.lensize, zle.len);
    zle.headersize = zle.lensize + zle.prevrawlensize;
    zle.p = p;
    return zle;
}

static unsigned int zipRawEntryLength(unsigned char *p){
     //return the total length/size of entry pointed by p
    unsigned int prevlensize, lensize, len;
    unsigned char enc;

    ZIP_DECODE_PREVLENSIZE(p,prevlensize);
    ZIP_DECODE_LENGTH(p+prevlensize, enc, lensize, len);

    return prevlensize+lensize+len;
}

static int zipTryEncoding(unsigned char *entry, unsigned int entrylen, long long *v, unsigned char *encoding){
    /* check the content preserved in entry to see whether it can be encoded as int, if could, return 1, and preserve the int in
     *v, encode preserved in encoding, else return 0 */
    long long val;
    if((entrylen >= 32) || (entrylen == 0)){
         //the string is too long
         return 0;
    }
    if(string2ll((char*)entry, entrylen, &val)){
        if((val>=0) && (val<=12)){
            *encoding = ZIP_INT_IMM_MIN+val;
        }
        else if((val>=INT8_MIN) && (val<=INT8_MAX)){
            *encoding = ZIP_INT_8B;
        }
        else if((val>=INT16_MIN) && (val<=INT16_MAX)){
            *encoding = ZIP_INT_16B;
        }
        else if((val>=INT32_MIN) && (val<=INT32_MAX)){
             *encoding = ZIP_INT_32B;
        }
        else {
             *encoding = ZIP_INT_64B;
        }
        *v = val;
        return 1;
    }
    else{
         return 0;
    }
}

static unsigned int zipIntSize(unsigned char encoding){
    //return the size of the integer which encoding encodes
    switch(encoding){
        case ZIP_INT_8B:
            return 1;
        case ZIP_INT_16B:
            return 2;
        case ZIP_INT_24B:
            return 3;
        case ZIP_INT_32B:
            return 4;
        case ZIP_INT_64B:
            return 8;
        default:
            return 0;
    }
    assert(NULL);//find why
    return 0;
}

static unsigned int zipPrevEncodeLength(unsignied char *p, unsigned int len){
    //Encode the prevlen, write it into p, if p is NULL, return bytes needed to encode it.
    if(p == NULL){
        if(len < ZIP_BIGLEN){
            return 1;
        }
        else{
            return 1+sizeof(len);
        }
    }
    else{
        if(len < ZIP_BIGLEN){
            p[0] = len;
            return 1;
        }
        else{
            p[0] = ZIP_BIGLEN;
            memcpy(p+1, &len, sizeof(len));
            memrev32ifbe(p+1);
            return 1+sizeof(len);
        }
    }
}

static void zipPrevEncodeLengthForceLarge(unsigned char *p, unsigned int len){
    //encode len into p, but the storage needed is actually smaller than given
     if(p == NULL) return;
     p[0] = ZIP_BIGLEN;
     memcpy(p+1, &len, sizeof(len));
     memrev32ifbe(p+1);
}

static unsigned int zipEncodeLength(unsigned char *p, unsigned char encoding, unsigned int rawlen){
    //encode the "encoding+length" part, and write it into p, return the bytes needed to encode them
    unsigned int len = 1;
    unsigned char benc[5];
    if(ZIP_IS_STR(encoding)){
        if(rawlen <= 0x3f){
             if(!p) return len;
             benc[0] = ZIP_STR_06B | rawlen;
        }
        else if(rawlen <= 0x3fff){
            len += 1;
             if(!p) return len;
             benc[0] = ZIP_STR_14B | ((rawlen>>8) & 0x3f);
             benc[1] = rawlen & 0xff;
        }
        else {
             len += 4;
             if(!p) return len;
             benc[0] = ZIP_STR_32B;
             benc[1] = (rawlen >> 24) & 0xff;
             benc[2] = (rawlen >> 16) & 0xff;
             benc[3] = (rawlen >> 8) & 0xff;
             benc[4] = rawlen & 0xff;
        }
    }
    else{
        //what been encoded is integer
         if(!p) return len;
         benc[0]=encoding;
    }
    //put the "encoding+length" into p
    memcpy(p, benc, len);
    return len;
}

static int zipPrevLenByteDiff(unsigned char *p, unsigned int len){
    //return the differences between current lensize and previous lensize, lensize, not len!
    unsigned int prevsize, currsize;

    ZIP_DECODE_PREVLENSIZE(p, prevsize);
    currsize = zipPrevEncodeLength(NULL, len);

    return currsize-prevsize;
}

static unsigned char *ziplistResize(unsigned char *zl, unsigned int len){
    //realloc ziplist's space, update its relative properties, return the updated ziplist
    zl = zrealloc(zl, len);
    ZIPLIST_BYTES(zl) = len;
    zl[len-1] = ZIP_END;

    return zl;
}

static unsigned char *_ziplistCascadeUpdate(unsigned char *zl, unsigned char *p){
    //cascaded update function after insert a new entry
    size_t curlen, rawlen, rawlensize, offset, noffset, extra;
    unsigned char *np;
    zlentry cur, next;

    curlen = intrev32ifbe(ZIPLIST_BYTES(zl));
    while(p[0]!=NULL){
        cur = zipEntry(p);//cur stores the current entry
        rawlen = cur.headersize + cur.len;//rawlen stores the length of current entry
        rawlensize = zipPrevEncodeLength(NULL, rawlen); //rawlensize stores the size needed to stores the current entry's length
        //if p is the tail entry, break
        if(p[rawlen]==ZIP_END) break;
        next = zipEntry(p+rawlen);
        //break if next's prev_entry_length same as storage needed to store cur's length
        if(rawlen == next.prevrawlen){
             break;
        }
        else if(rawlensize > next.prevrawlensize){
            offset = p-zl;
            extra = rawlensize-next.prevrawlensize;//back up offset, because realloc may change the ziplist's address
            zl = ziplistResize(zl, curlen+extra);
            p = zl+offset;
            // Current pointer and offset for next element.
            np = p+rawlen;
            noffset = offset+rawlen;
            if(np[next.headersize+next.len]!=ZIP_END){
                ZIPLIST_TAIL_OFFSET(zl) = intrev32ifbe(extra+intrev32ifbe(ZIPLIST_TAIL_OFFSET));
            }
            memmove(np+rawlensize, np+next.prevrawlensize, curlen-noffset-next.prevrawlensize-1);
            zipPrevEncodeLength(np, rawlen);
            p = p+rawlen;
            curlen += extra;
        }
        else if(rawlensize == next.prevrawlensize){
            zipPrevEncodeLength(p+rawlen, rawlen);
            break;
        }
        else{
             zipPrevEncodeLengthForceLarge(p+rawlen,rawlen);
             break;
        }
    }
    return zl;
}

static void zipSaveInteger(unsigned char *p, int64_t value, unsigned char encoding){
    //save the integer value into p, and put encoding into p;
    int8_t i8;
    int16_t i16;
    int32_t i32;
    int64_t i64;
    if(encoding == ZIP_INT_8B){
         i8 = value;
         memcpy(p, &i8, sizeof(i8));
    }
    else if(encoding == ZIP_INT_16B){
         i16 = value;
         memcpy(p, &i16, sizeof(i16));
         memrev16ifbe(p);
    }
    else if(encoding == ZIP_INT_24B){
        i32 = value << 8;
        memrev32ifbe(i32);
        memcpy(p, ((uint8_t *)(&i32))+1, sizeof(i32)-sizeof(uint8_t));
    }
    else if(encoding == ZIP_INT_32B){
         i32 = value;
         memcpy(p, &i32, sizeof(i32));
         memrev32ifbe(p);
    }
    else if(encoding ==  ZIP_INT_64B){
        i64 = value;
        memcpy(p, &i64, sizeof(i64));
        memrev64ifbe(p);
    }
    else if(encoding >= ZIP_INT_IMM_MIN && encoding <= ZIP_INT_IMM_MAX){
         // do nothing, 1111xxxx;
    }
    assert(NULL);
}

static unsigned char *_ziplistInsert(unsigned char *zl, unsigned char *p, unsigned char *s, unsigned int slen){
    //add new entry with content s to p
    size_t curlen = intrev32ifbe(ZIPLIST_BYTES(zl)), newlen, prevlen, offset;
    int nextdiff;
    unsigned char encoding;
    long long value;
    zlentry entry, tail;
    //Consider whether the new entry will be inserted to the end of ziplist;
    if(p[0]!=ZIP_END){
        entry = zipEntry(p);
        prevlen = entry.prevrawlen;
    }
    else{
        unsigned char *ptail = ZIPLIST_ENTRY_TAIL(zl);
        if(ptail[0]!=ZIP_END){
            prevlen = zipRawEntryLength(p);
        }
    }
    //assign newlen the value in case of integer or string
    if(zipTryEncoding(s, slen, &value, &encoding)){
         newlen = zipIntSize(encoding);
    }
    else{
        newlen = slen;
    }
    //add newlen the size needed to encode previous length and current length;
    newlen += zipPrevEncodeLength(NULL, prevlen);
    newlen += zipEncodeLength(NULL, encoding, slen);
    //preserve the differences between new prevrawlensize and old prevrawlensize;
    nextdiff = (p[0]!=ZIP_END) ? zipPrevLenByteDiff(p, newlen): 0;
    //space realloc;
    offset = p-zl;
    zl = ziplistResize(zl, curlen+nextdiff+newlen);
    p = zl+offset;
    //move entries behind the added entry into new positions;
    if(p[0]!=ZIP_END){
        //The case which doesn't insert at the end;
        memmove(p+newlen, p-nextdiff, curlen-offset+nextdiff-1);//debug!
        zipPrevEncodeLength(p+newlen, newlen);
        tail = zipEntry(p+newlen);
        if(p[newlen+tail.headersize+tail.len] != ZIP_END){
             ZIPLIST_TAIL_OFFSET(zl) = intrev32ifbe(ZIPLIST_TAIL_OFFSET(zl)+newlen+nextdiff);
        }
        else{
            ZIPLIST_TAIL_OFFSET(zl) = intrev32ifbe(ZIPLIST_TAIL_OFFSET(zl)+newlen);
        }

    }
    else{
        //Case which insert at the end;
         ZIPLIST_TAIL_OFFSET = intrev32ifbe(p-zl);
    }
    if(nextdiff!=0){
        //The case of cascade update may happen;
         offset = p-zl;
         zl = _ziplistCascadeUpdate(zl, p+newlen);
         p = zl+offset;
    }
    //update new entry properties
    p += zipPrevEncodeLength(p,prevlen);
    p += zipEncodeLength(p,encoding,slen);
    if(ZIP_IS_STR(encoding)){
        memcpy(p, s, slen);
    }
    else{
        zipSaveInteger(p, value, encoding);
    }
    ZIPLIST_INCR_LENGTH(zl, 1);

    return zl;
}

unsigned char *ziplistPush(unsigned char *zl, unsigned char *s, unsigned int slen, int where){
     //add new element to the header or tail of ziplist;
     unsigned char *p;
     p = (where == ZIPLIST_HEAD) ? ZIPLIST_ENTRY_HEAD : ZIPLIST_ENTRY_END;
     return _ziplistInsert(zl, p, s, slen);
}

//------ziplistInsert--------add the new entry with given value after the setted entry---O(N) on average, O(N^2) worst;
unsigned char *ziplistInsert(unsigned char *zl, unsigned char *p, unsigned char *s, unsigned int slen){
     return _ziplistInsert(zl, p, s, slen);
}

//-----ziplistIndex------return the entry from ziplist with given index-----O(N);
unsigned char *ziplistIndex(unsigned char *zl, int index){
     //header entry's index is 0, tail entry's index is -1;
     unsigned char *p;
     unsigned int prevrawlen, prevrawlensize;
     if(index<0){
         index = -index-1;//turn negative index into positive;
         p = ZIPLIST_ENTRY_TAIL(zl);
         if(p[0]!=ZIP_END){
             ZIP_DECODE_PREVLEN(p, prevrawlensize, prevrawlen);
             while((prevrawlen>0) && (index--)){
                  p-=prevrawlen;
                  ZIP_DECODE_PREVLEN(p, prevlensize, prevrawlen);
             }
         }

     }
     else{
         p = ZIPLIST_ENTRY_HEAD(zl);
         while((p[0]!=ZIP_END) && (index--)){
             p += zipRawEntryLength(p);
         }
     }

     return ((p[0]==ZIP_END) || (index>0)) ? NULL : p;
}

//-----ziplistFind------Find and return the entry with given value from the ziplist---O(N)
static int64_t zipLoadInteger(unsigned char *p, unsigned char *encoding){
    //get the integer's value from p by encoding
    int16_t i16;
    int32_t i32;
    int64_t i64, rtn;
    if(encoding == ZIP_INT_8B){
         rtn = ((int8_t *)p)[0];
    }
    else if(encoding == ZIP_INT_16B){
        memcpy(&i16, p, sizeof(i16));
        memrev16ifbe(&i16);
        rtn = i16;
    }
    else if(encoding == ZIP_INT_24B){
        memcpy(((uint8_t *)&i32)+1, sizeof(int32_t)-sizeof(int8_t));
        memrev32ifbe(&i32);
        rtn = i32>>8;
    }
    else if(encoding == ZIP_INT_32B){
         memcpy(&i32, p, sizeof(i32));
         memrev32ifbe(&i32);
         rtn = i32;
    }
    else if(encoding == ZIP_INT_64B){
         memcpy(&i64, p, sizeof(i64));
         memrev64ifbe(&i64);
         rtn = i64;
    }
    else if((encoding >= ZIP_INT_IMM_MIN) && (encoding <= ZIP_INT_IMM_MAX)){
        rtn = (encoding && ZIP_INT_IMM_MASK)-1;
    }
    else{
         assert(NULL);
    }

    return rtn;
}

unsigned char *ziplistFind(unsigned char *p, unsigned char *vstr,unsigned int vlen, unsigned int skip){
    //Find and return the entry with value vstr and length vlen, return NULL if not found;
    int skipsteps=0;
    unsigned char vencoding = 0;
    long long vll;
    unsigned int prevlensize, encoding, lensize, len;
    unsigned char *q;

    while(p[0]!=ZIP_END){
         ZIP_DECODE_PREVLENSIZE(p, prevlensize);
         ZIP_DECODE_LENGTH(p+prevlensize, encoding, lensize, len);
         q = p+prevlensize+lensize;

         if(skipsteps == 0){
             if(ZIP_IS_STR(encoding)){
                 if((vlen == len) && memcpy(q, vstr, vlen)){
                      return p;
                 }
             }
             else{
                 if(vencoding != UCHAR_MAX){
                     if(zipTryEncoding(vstr, vlen, &vll, encoding)){
                         if(zipLoadInteger(q, encoding) == vll){
                             return p;
                         }
                     }
                     else{
                         vencoding = UCHAR_MAX;
                     }
                 }
             }
             skipsteps = skip;
         }
         else{
              skipsteps--;
         }

         p = q+len;
    }
    return NULL;
}

/*-----------ziplistNext-----Return the next entry of given entry from ziplist---O(1)-------*/
unsigned char *ziplistNext(unsignied char *zl, unsigned char *p){
    ((void) zl);
    //return the entry after p;
    if(p[0] == ZIP_END){
        return NULL;
    }
    p += zipRawEntryLength(p);
    if(p[0] == ZIP_END){
        return NULL;
    }
    return p;
}

/*-----------ziplistPrev----return the prior entry of given entry from ziplist----O(1)------*/
unsi char *ziplistPrev(unsigned char *zl, unsigned char *p){
     //return the previous entry of p from ziplist;
     zlentry pentry;

    if(p == ZIPLIST_ENTRY_END(zl)){
        if(p == ZIPLIST_ENTRY_TAIL(zl)){
             return NULL;
        }
        else{
            return ZIPLIST_ENTRY_TAIL(zl);
        }
    }
    else if(p == ZIPLIST_ENTRY_HEAD(zl)){
         return NULL;
    }
    else{
        pentry = zipEntry(p);
        assert(pentry.prevrawlen>0);
        p -= pentry.prevrawlen;
        return p;
    }
}

/*---------ziplistGet----return the value/content preserved in given value --- O(1)-----*/
unsigned int ziplistGet(unsigned char *p, unsigned char **sstr, unsigned int *slen, long long *sval){
    //get the value from given entry, if the value is string, preserve it in *sstr, its length in *slen;
    //if the value is integer, preserve it in sval;
    zlentry pentry;

    if((p==NULL) || (p[0]==ZIP_END)){
        return 0;
    }
    if(sstr) *sstr = NULL;
    if(sval) *sval = -1;

    pentry = zipEntry(p);
    if(ZIP_IS_STR(p)){
        if((sstr) && (slen)){
            *sstr = pentry.p + pentry.headersize;
            *slen = pentry.len;
        }
    }
    else{
        if(sval){
            *sval = zipLoadInteger(p+pentry.headersize, pentry.encoding);
        }
    }
    return 1;
}

/*--------ziplistDelete----Delete the given entries from the ziplist----O(N) in average, O(N^2) worst----*/
static unsigned char *_ziplistDelete(unsigned char *zl, unsigned char *p, unsigned int num){
     //Delete num entries from entry pointed by p, return the updated ziplist
     unsignied int i, totlen, deleted;
     size_t offset;
     int nextdiff;
     zlentry first, tail;

     first = zipEntry(p);
     deleted = 0;
     for(i=0; i<num; i++){
          p += zipRawEntryLength(p);
          deleted++;
     }

     totlen = p-first.p;
     //cover the entries to delete
     if(totlen > 0){
         if(p[0]!=ZIP_END){
             nextdiff = zipPrevLenByteDiff(p, first.prevrawlen);
             p -= nextdiff;
             zipPrevEncodeLength(p,first.prevrawlen);

             if(p+nextdiff == ZIPLIST_ENTRY_TAIL(zl)){
                  ZIPLIST_TAIL_OFFSET = intrev32ifbe(intrev32ifbe(ZIPLIST_ENTRY_TAIL)-totlen);
             }
             else{
                  ZIPLIST_TAIL_OFFSET = intrev32ifbe(intrev32ifbe(ZIPLIST_ENTRY_TAIL)-totlen+nextdiff);
             }

             memmove(first.p, p, intrev32ifbe(ZIPLIST_BYTES(zl))-1-(p-zl));
         }
         else{
             ZIPLIST_TAIL_OFFSET = intrev32ifbe(first.p-zl-first.prevrawlen);
         }
        //realloc the ziplist;
         offset = first.p-zl;
         zl = ziplistResize(zl, intrev32ifbe(ZIPLIST_BYTES)-totlen+nextdiff);
         ZIPLIST_INCR_LENGTH(zl,-deleted);
         p = zl+offset;
         //cascaded update;
         if(nextdiff!=0){//no matter nextdiff greater or less than 0,
             zl = _ziplistCascadeUpdate(zl, p);
         }
     }
     return zl;
}

unsigned char *ziplistDelete(unsigned char *zl, unsigned char **p){
     //delete the given entry from ziplist;
     size_t offset;
     offset = *p-zl;
     zl = _ziplistDelete(zl, *p, 1);
     *p = zl+offset;
     return zl;
}

/*---------ziplistDeleteRange----delete the continus entries from ziplist----average O(N), O(N^2) worst---*/
unsigned char *ziplistDeleteRange(unsigned char *zl, int index, unsigned int num){
    unsigned char *p;
    p = ziplistIndex(zl, index);
    return (p==NULL) ? zl : _ziplistDelete(zl, p, num);
}

/*-------ziplistBlobLen----return the bytes the ziplist takes-----O(1)---*/
unsigned int ziplistBlobLen(unsigned char *zl){
     return ZIPLIST_BYTES(zl);
}

/*--ziplistLen--return the number of entries ziplist holds--O(1) when entry number less than 65535, O(N) when greater than 65535-*/
unsigned int ziplistLen(unsigned char *zl){
    unsigned int num=0;
    unsigned char *p;
    if(intrev16ifbe(ZIPLIST_LENGTH(zl)) < UINT16_MAX){
         return intrev16ifbe(ZIPLIST_LENGTH(zl));
    }
    else{
        p = ZIPLIST_ENTRY_TAIL(zl);
        if(p[0]!=ZIP_END){
            while(p[0]!=ZIP_END){
                p += zipRawEntryLength(p);
                num++;
            }
            return num+UINT16_MAX-1;
        }
        else{
            assert(NULL);
        }
    }
}
