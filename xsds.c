#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <ctype.h>
#include <assert.h>
#include "xsds.h"
#include "zmalloc.h"

/*-----------------------------APIs-------------------------*/
sds sdsnewlen(const void *init, size_t initlen){
    //return the sds with initlen and init as initial buf
    sdshdr *sh;

    sh = zcalloc(sizeof(sdshdr)+initlen+1);
    if(sh == NULL){
        return NULL;
    }

    sh->len = initlen;
    sh->free = 0;
    if(init && initlen){
        memcpy(sh->buf, init, initlen);
    }
    sh->buf[initlen] = '\0';
    return (char *)sh->buf;
}

sds sdsempty(){
    //Create a sds with no content;
    return sdsnewlen(NULL,0);
}

sds sdsnew(const char *init){
    //Create a sds with given string
    return init == NULL ? sdsempty() : sdsnewlen(init, strlen(init));
}

size_t sdslen(const sds str){
    sdshdr *sh = SDSGETHDR(str);
    return sh->len;
}

size_t sdsavail(const sds str){
    sdshdr *sh = SDSGETHDR(str);
    return sh->free;
}

sds sdsdup(const sds str){
    //Create a copy of sds
    return sdsnewlen(str, sdslen(str));
}
//
void sdsfree(sds str){
     //Free the given sds
    if(str){
        zfree(str-sizeof(sdshdr));
    }
    return;
}

void sdsupdatelen(sds s){
    //update the given sds's corresponding sdshdr's free and len
    sdshdr *sh = SDSGETHDR(s);
    unsigned int reallen = strlen(s);
    sh->free += (sh->len-reallen);
    sh->len = reallen;
}

void sdsclear(sds str){
    //Clear the buf of given str to NULL
    sdshdr *sh = SDSGETHDR(str), *newsh;

    str[0] = '\0';
    sh->free += sh->len;
    sh->len =0;
}

sds sdsMakeRoomFor(sds str, size_t addlen){
     //Enlarge the storage of str->buf
    sdshdr *sh = SDSGETHDR(str), *newsh;
    unsigned int olen, nlen;
    //Prealloc space is enough, no need for enlarge
    if(addlen <= sh->free){
        return str;
    }

    olen = sh->len;
    nlen = sh->len+addlen;

    if(nlen < SDS_MAX_PREALLOC){
        nlen *= 2;
    }
    else{
        nlen += SDS_MAX_PREALLOC;
    }

    newsh = zrealloc(sh, sizeof(sdshdr)+nlen+1);
    if(newsh == NULL){
        return NULL;
    }
    newsh->free = nlen-olen;
    return newsh->buf;
}

sds sdsRemoveFreeSpace(sds str){
    //Free the free spaces in buf without changing it
    sdshdr *sh = SDSGETHDR(str);
    sh = zrealloc(sh, sizeof(sdshdr)+sh->len+1);
    sh->free = 0;
    return sh->buf;
}

size_t sdsAllocSize(sds str){
    //Compute the spces taken by given sds'buf
    sdshdr *sh = SDSGETHDR(str);
    return (sizeof(*sh) + sh->len + sh->free +1);
}

void sdsIncrLen(sds str, int incr){
    //expand or trim sds->buf's right end
    sdshdr *sh = SDSGETHDR(str);

    if(incr >= 0){
        assert(sh->free >= (unsigned int)incr);
    }
    else{
        assert(sh->len >= (unsigned int)(-incr));
    }
    sh->len += incr;
    sh->free -= incr;
    sh->buf[sh->len] = '\0';
}

sds sdsgrowzero(sds str, size_t nlen){
    /*expand the sds to given length with '\0'*/
    sdshdr *sh = SDSGETHDR(str);
    int olen = sh->len, totlen;
    //No need for enlarge if old length >= new length
    if(olen >= nlen){
        return str;
    }
    str = sdsMakeRoomFor(str, nlen-olen);
    if(str == NULL) return NULL;
    memset(str+olen, 0, nlen-olen+1);
    //put '\0' in places with no content
    totlen = sh->len + sh->free;
    sh->len = nlen;
    sh->free = totlen-sh->len;

    return sh->buf;
}

sds sdscatlen(sds str, size_t addlen, char *adds){
    //enlarge the given sds, and add a sds at the end of it;
    sdshdr *sh;
    //1. enlarge the space;
    str = sdsMakeRoomFor(str, addlen);
    if(str == NULL) return NULL;
    sh = SDSGETHDR(str);
    //2. add string after the buf;
    memcpy(str+sh->len, adds, addlen);
    sh->buf[sh->len+addlen] = '\0';
    //3. update the properties;
    sh->len += addlen;
    sh->free -= addlen;

    return str;
}

sds sdscat(sds str, char *adds){
     //add a C string to the end of str
    return sdscatlen(str, strlen(adds), adds);
}

sds sdscatsds(sds str, sds addsds){
     //add addsds to the end of sds;
    return sdscatlen(str, sdslen(addsds), addsds);
}

sds sdscpylen(sds str, size_t slen, char *cpys){
    //copy part of a C string to the sds
    sdshdr *sh = SDSGETHDR(str);
    size_t totlen = sh->len + sh->free;

    //Check the need of enlarging room
    if(slen > totlen){
        str = sdsMakeRoomFor(str, slen-totlen);
        if(str == NULL) return NULL;
        sh = SDSGETHDR(str);
    }

    memcpy(str, cpys, slen);
    sh->buf[slen] = '\0';

    totlen = sh->len + sh->free;
    sh->len = slen;
    sh->free = totlen-sh->len;

    return str;
}

sds sdscpy(sds str, char *cpys){
    return sdscpylen(str, strlen(cpys), cpys);
}

sds sdsrange(sds str, int start, int end){
    //Preserve sds in given range, start and end denotes array index
    sdshdr *sh = SDSGETHDR(str);
    int nlen, totlen = sh->len+sh->free;
    //1. Consider special cases
    if(start < 0){
       start += sh->len;
    }
    if(end < 0){
        end += sh->len;
    }
    if((start>end) || (start>=sh->len) || (end<0)){
        return str;
    }
    if(start < 0){
        start = 0;
    }
    if(end >= sh->len){
        end = sh->len-1;
    }
    nlen = end-start+1;
    if(nlen > 0){
        memmove(sh->buf, sh->buf+start, nlen);
        sh->buf[nlen] = '\0';
        sh->len = nlen;
        sh->free = totlen-nlen;
    }

    return str;
}

sds sdstrim(sds str, const char *cset){
    char *pstart, *pend;
    sdshdr *sh = SDSGETHDR(str);
    int nlen, totlen = sh->len+sh->free;

    pstart = sh->buf;
    pend = sh->buf+sh->len-1;
    while((pstart <= (sh->buf+sh->len-1)) && (strchr(cset, *pstart))){
        pstart++;
    }
    while((pend >= sh->buf) && (strchr(cset, *pend))){
        pend--;
    }

    nlen = pend-pstart+1;
    if(nlen<=0) nlen=0;

    if((nlen>0) && (pstart > sh->buf)){
        memmove(sh->buf, pstart, nlen);
    }
    sh->buf[nlen] = '\0';
    sh->len = nlen;
    sh->free = totlen-nlen;

    return sh->buf;
}

int sdscmp(sds str1, sds str2){
    //corresponding to strcmp for C strin
    //memcpy is used in source code
    int len1 = sdslen(str1), len2 = sdslen(str2);
    int slen = (len1>len2 ? len2 :len1), cmp;

    cmp = memcmp(str1,str2,slen);
    if(cmp == 0){
        return len1-len2;
    }
    return cmp;
}

int sdsll2str(char *s, long long value){
    /*turn long long value into string*/
    char *p, aux;
    unsigned long long v;
    size_t l;
    /* Generate the string representation, this method produces an reversed string. */
    v = (value < 0) ? -value : value;
    p = s;
    do {
        *p++ = '0'+(v%10);
        v /= 10;
    } while(v);
    if(value < 0){
        *p++ = '-';
    }
    /* Compute length and add null term. */
    l = p-s;
    *p = '\0';
    /* Reverse the string. */
    p--;
    /*exchange head and end*/
    while(s < p){
        aux = *s;
        *s = *p;
        *p = aux;
        s++;
        p--;
    }
    return l;
}

sds sdsfromlonglong(long long value){
    /*turn value into sds form*/
    char buf[SDS_LLSTR_SIZE];
    int len = sdsll2str(buf,value);

    return sdsnewlen(buf,len);
}

sds sdscatvprintf(sds s, const char *fmt, va_list ap){
    /*concat varargs with fmt to sds*/
    va_list cpy;
    char staticbuf[1024], *buf = staticbuf, *t;
    size_t buflen = strlen(fmt)*2;
    /* We try to start using a static buffer for speed.If not possible we revert to heap allocation. */
    if(buflen > sizeof(staticbuf)){
        buf = zmalloc(buflen);
        if(buf == NULL){
            return NULL;
        }
    }
    else{
        buflen = sizeof(staticbuf);
    }
    /* Try with buffers two times bigger every time we fail to fit the string in the current buffer size. */
    while(1){
        buf[buflen-2] = '\0';
        va_copy(cpy,ap);
        vsnprintf(buf, buflen, fmt, cpy);
        va_end(cpy);
        if(buf[buflen-2] != '\0'){
            if(buf != staticbuf){
                zfree(buf);
            }
            buflen *= 2;
            buf = zmalloc(buflen);
            if(buf == NULL){
                return NULL;
            }
            continue;
        }
        break;
    }
    /* Finally concat the obtained string to the SDS string and return it. */
    t = sdscat(s, buf);
    if (buf != staticbuf){
        zfree(buf);
    }
    return t;
}

sds sdscatprintf(sds s, const char *fmt, ...){
    /*like snprintf for str*/
    va_list ap;
    char *t;
    va_start(ap, fmt);
    t = sdscatvprintf(s,fmt,ap);
    va_end(ap);
    return t;
}

sds *sdssplitlen(const char *s, int len, const char *sep, int seplen, int *count){
    /*split the sds into array of small sdses by seperator sep*/
    int elements = 0, slots = 5, start = 0, j;
    sds *tokens;
    /*handle corner case*/
    if(seplen < 1 || len < 0){
        return NULL;
    }
    /*set 5 tokens at first*/
    tokens = zmalloc(sizeof(sds)*slots);
    if(tokens == NULL){
        return NULL;
    }
    /*zero length string, null tokens is returned*/
    if(len == 0){
        *count = 0;
        return tokens;
    }
    for(j = 0; j < (len-(seplen-1)); j++){
        /* make sure there is room for the next element and the final one */
        if(slots < elements+2){
            sds *newtokens;

            slots *= 2;
            newtokens = zrealloc(tokens,sizeof(sds)*slots);
            if(newtokens == NULL){
                goto cleanup;
            }
            tokens = newtokens;
        }
        /* search the separator */
        if((seplen == 1 && *(s+j) == sep[0]) || (memcmp(s+j,sep,seplen) == 0)){
            tokens[elements] = sdsnewlen(s+start,j-start);
            if(tokens[elements] == NULL){
                /*this may happen when memory is not enough*/
                goto cleanup;
            }
            elements++;
            start = j+seplen;
            j = j+seplen-1; /* skip the separator */
        }
    }
    /* Add the final element. We are sure there is room in the tokens array. */
    tokens[elements] = sdsnewlen(s+start,len-start);
    if(tokens[elements] == NULL){
        goto cleanup;
    }
    elements++;
    *count = elements;         //count saves size of tokens;
    return tokens;

cleanup:
    {
        int i;
        for (i = 0; i < elements; i++) sdsfree(tokens[i]);
        zfree(tokens);
        *count = 0;
        return NULL;
    }
}

/* Free the result returned by sdssplitlen(), or do nothing if 'tokens' is NULL. */
void sdsfreesplitres(sds *tokens, int count) {
    if(!tokens){
        return;
    }
    while(count--){
        sdsfree(tokens[count]);
    }
    zfree(tokens);
}

