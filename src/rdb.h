#ifndef RDB_INCLUDED
#define RDB_INCLUDED

#include <stdio.h>
#include "xrio.h"
#include "redis.h"

#define XREDIS_RDB_VERSION 1
/*For avoiding storing 32 bits lengths for short keys(serious space wasting!)*/
/*So we check the most significant 2 bits of the first byte to interpreter the length-*/

#define XREDIS_RDB_6BITLEN 0
#define XREDIS_RDB_14BITLEN 1
#define XREDIS_RDB_32BITLEN 2
#define XREDIS_RDB_ENCVAL  3
#define XREDIS_RDB_LENERR UINT_MAX

/*the remaining two bits specify a special encoding for the object */
#define XREDIS_RDB_ENC_INT8   0
#define XREDIS_RDB_ENC_INT16  1
#define XREDIS_RDB_ENC_INT32  2

/*the RDB key_value_pairs part TYPE values*/
#define XREDIS_RDB_TYPE_STRING  0
#define XREDIS_RDB_TYPE_LIST    1
#define XREDIS_RDB_TYPE_SET     2
#define XREDIS_RDB_TYPE_ZSET     3
#define XREDIS_RDB_TYPE_HASH    4
#define XREDIS_RDB_TYPE_LIST_ZIPLIST  10
#define XREDIS_RDB_TYPE_SET_INTSET    11
#define XREDIS_RDB_TYPE_ZSET_ZIPLIST  12
#define XREDIS_RDB_TYPE_HASH_ZIPLIST  13

//Test if a type is an object type;
#define rdbIsObjectType(t) ((t>=0 && t<=4)||(t>=10 && t<=13))

/* Special RDB opcodes */
#define XREDIS_RDB_OPCODE_EXPIRETIME_MS  252  //EXPIRETIME_MS;
#define XREDIS_RDB_OPCODE_EXPIRETIME  253    //EXPIRETIME;
#define XREDIS_RDB_OPCODE_SELECTDB   254  //SELECTDB;
#define XREDIS_RDB_OPCODE_EOF     255     //EOF;

int rdbSaveType(xrio *rdb, unsigned char type);
int rdbLoadType(xrio *rdb);
int rdbSaveTime(xrio *rdb, time_t t);
time_t rdbLoadTime(xrio *rdb);
int rdbSaveLen(xrio *rdb);
uint32_t rdbLoadLen(xrio *rdb);
int rdbSaveObjectType(xrio *rdb);
int rdbLoadObjectType(xrio *rdb);
int rdbLoad(char *filename);
int rdbSaveBackground(char *filename);
void rdbRemoveTempFile(pid_t childpid);
int rdbSave(char *filename);
int rdbSaveObject(xrio *rdb, xrobj *xo);
off_t rdbSaveObjectLen(xrobj *xo);
off_t rdbSaveObjectPages(xrobj *xo);
xrobj *rdbLoadObject(int type, xrio *rdb);
void backgroundSaveDoneHandler(int exitcode, int bysignal);
int rdbSaveKeyValuePair(xrio *rdb, xrobj *key, xrobj *val, long long expiretime, long long now);
xrobj *rdbLoadStringObject(xrio *rdb);

#endif
