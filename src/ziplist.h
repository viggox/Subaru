#define ZIPLIST_HEAD 0
#define ZIPLIST_TAIL 1

extern unsigned char *ziplistNew(void);
extern unsigned char *ziplistPush(unsigned char *zl, unsigned char *s, unsigned int slen, int where);
extern unsigned char *ziplistIndex(unsigned char *zl, int index);
extern unsigned char *ziplistNext(unsigned char *zl, unsigned char *p);
extern unsigned char *ziplistPrev(unsigned char *zl, unsigned char *p);
extern unsigned int ziplistGet(unsigned char *p, unsigned char **sval, unsigned int *slen, long long *lval);
extern unsigned char *ziplistInsert(unsigned char *zl, unsigned char *p, unsigned char *s, unsigned int slen);
extern unsigned char *ziplistDelete(unsigned char *zl, unsigned char **p);
extern unsigned char *ziplistDeleteRange(unsigned char *zl, unsigned int index, unsigned int num);
extern unsigned int ziplistCompare(unsigned char *p, unsigned char *s, unsigned int slen);
extern unsigned char *ziplistFind(unsigned char *p, unsigned char *vstr, unsigned int vlen, unsigned int skip);
extern unsigned int ziplistLen(unsigned char *zl);
extern size_t ziplistBlobLen(unsigned char *zl);
