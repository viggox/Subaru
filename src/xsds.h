#ifndef SDS_INCLUDED
#define SDS_INCLUDED

#include <sys/types.h>
#include <stdarg.h>

#define SDS_MAX_PREALLOC (1024*1024)
#define SDS_LLSTR_SIZE 21

typedef char *sds;

typedef struct sdshdr{
    unsigned int len;
    unsigned int free;
    char buf[];
} sdshdr;

#define SDSGETHDR(sds) ((sdshdr *)((sds)-(sizeof(sdshdr))))

sds sdsnewlen(const void *init, size_t initlen);
sds sdsnew(const char *init);
sds sdsempty();
size_t sdslen(const sds s);
sds sdsdup(const sds s);
void sdsfree(sds s);
size_t sdsavail(const sds s);
void sdsupdatelen(sds s);
sds sdsgrowzero(sds s, size_t len);
sds sdscatlen(sds s, size_t len, char *t);
sds sdscat(sds s, char *t);
sds sdscatsds(sds s, sds t);
sds sdscpylen(sds s, size_t len, char *str);
sds sdscpy(sds s, char *str);
sds sdstrim(sds s, const char *cset);
sds sdsrange(sds s, int start, int end);
void sdsclear(sds s);
int sdscmp(const sds s1, const sds s2);
sds sdsfromlonglong(long long value);

sds sdscatvprintf(sds s, const char *fmt, va_list ap);
sds sdscatprintf(sds s, const char *fmt, ...);

sds *sdssplitlen(const char *s, int len, const char *sep, int seplen, int *count);
void sdsfreesplitres(sds *tokens, int count);

//Low level functions exposed to the user API
sds sdsMakeRoomFor(sds s, size_t addlen);
void sdsIncrLen(sds s, int incr);
sds sdsRemoveFreeSpace(sds s);
size_t sdsAllocSize(sds s);

#endif

