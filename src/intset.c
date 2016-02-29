#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include "intset.h"
#include "zmalloc.h"
#include "endianconv.h"

/*----------------------------------------------------Marcos---------------------------------------*/
#define INTSET_ENC_INT16 (sizeof(int16_t))
#define INTSET_ENC_INT32 (sizeof(int32_t))
#define INTSET_ENC_INT64 (sizeof(int64_t))

#define intset  IST

/*-----------------------------------------------APIs---------------------------------------------*/
/*             intSetNew-------------Create a new integer set-------------------------O(1)              */
IST *intsetNew(void){
    IS  *pset = zmalloc(sizeof(IS));
    pset->encoding = intrev32ifbe(INTSET_ENC_INT16);
    pset->length = 0;

    return pset;
}

/*            intsetAdd-------------Add a given element to the integer set----------------------------O(N)    */
static uint8_t _intsetValueEncoding(int64_t v){
     //judge v's encoding by compare its value with type limits
    if(v>INT32_MAX || v<INT32_MIN) {
        return INTSET_ENC_INT64;
    }
    else if(v>INT16_MAX || v<INT16_MIN) {
         return INTSET_ENC_INT32;
    }
    else {
         return INTSET_ENC_INT16;
    }
}

static int64_t _intsetGetEncoded(IST *is, int pos, uint8_t enc){
    //return the value on given position by given encoding value from the intset
    int64_t rv64;
    int32_t rv32;
    int16_t rv16;

    if(enc == INTSET_ENC_INT64){
        memcpy(&rv64, ((int64_t *)is->contents) + pos, sizeof(int64_t));
        memrev64ifbe(&rv64);
        return rv64;
    }
    else if(enc == INTSET_ENC_INT32){
         memcpy(&rv32, ((int32_t *)is->contents) + pos, sizeof(int32_t));
         memrev32ifbe(&rv32);
         return rv32;
    }
    else {
         memcpy(&rv16, ((int16_t *)is->contents) + pos, sizeof(int16_t));
         memrev16ifbe(&rv16);
         return rv16;
    }
}

static int64_t _intsetGet(IST *is, int pos){
     //return the value on given position from the intset
    return _intsetGetEncoded(is, pos, intrev32ifbe(is->encoding));
}


static void _intsetSet(IST *is, int pos, int64_t value){
    //set the element on position pos of intset is with value
    uint32_t enc = intrev32ifbe(is->encoding);

    if(enc == INTSET_ENC_INT64){
        ((int64_t *)is->contents)[pos] = value;
        memrev64ifbe(((int64_t *)is->contents)+pos);
    }
    else if(enc == INTSET_ENC_INT32){
         ((int32_t *)is->contents)[pos] = value;
         memrev32ifbe(((int32_t *)is->contents)+pos);
    }
    else{
        ((int16_t *)is->contents)[pos] = value;
        memrev16ifbe(((int16_t *)is->contents)+pos);
    }
}


static IST  *intsetResize(IST* is, uint32_t len){
    //expand the storage of is to hold elements with number len
    uint32_t new_size;// set as uint32_t because both encoding and len are uint32_t, so the new_size's type is uint32_t
    new_size = len*intrev32ifbe(is->encoding);
    is = zrealloc(is, new_size);
    return is;
}


static uint8_t intsetSearch(IST *is, int64_t value, uint32_t *pos){
    /*search the value's position in intset, if value is in intset,return 1 and preserve position in pos,
     * else,return 0, and preserve the place that value can be inserted */
    int min, mid, max;
    int64_t vget;

    if(intrev32ifbe(is->length) == 0){
        if(pos) *pos = 0;
        return 0;
    }
    else if(value > _intsetGet(is,intrev32ifbe(is->length)-1)){
         if(pos) *pos = intrev32ifbe(is->length);
         return 0; //why not use intrev32ifbe now?!!!!!
    }
    else if(value < _intsetGet(is,0)){
         if(pos) *pos = 0;
         return 0;
    }
    //now, let's do binary search!!!
    min = 0;
    max = intrev32ifbe(is->length)-1;
    mid = -1;

    while(min<=max){
         mid = (min + max)/2;
         vget = _intsetGet(is, mid);

         if(vget < value){
             min = mid;
         }
         else if(vget > value){
             max = mid;
         }
         else{
              break;
         }
    }

    if(vget == value){
        if(pos) *pos = intrev32ifbe(mid);
        return 1;
    }
    else{
         if(pos) *pos = intrev32ifbe(min);
         return 0;
    }
    return 0;
}


static IST *intsetUpgradeAndAdd(IST *is, int64_t value){
    //upgrade the intset and add the new value to it
    uint8_t oldenc, newenc;
    int hdortl, lengths;

    lengths = intrev32ifbe(is->length);
    oldenc = intrev32ifbe(is->encoding);
    newenc = _intsetValueEncoding(value);//who can tell me why the fuck don't use intrev32ifbe before it!
    //use hdortl to judge put on intset's head or intset's tail, hdortl=0 is tail, hdortl=1 is head
    if(value > 0){
        hdortl = 0;
    }
    else{
        hdortl = 1;
    }
    //expand the storage
    is->encoding = intrev32ifbe(newenc);
    is = intsetResize(is, intrev32ifbe(is->length)+1);
    //move the elements one by one
    while(lengths--){
        _intsetSet(is, lengths+hdortl, _intsetGetEncoded(is, lengths, oldenc));//use functions before!!!!
    }
    //put new value on its place
    if(hdortl){
        _intsetSet(is, 0, value);
    }
    else{
        _intsetSet(is, intrev32ifbe(is->length), value);
    }
    //update the length
    is->length = intrev32ifbe(intrev32ifbe(is->length)+1);

    return is;
}


static void intsetMoveTail(IST *is, uint32_t from, uint32_t to){
    //move the elements starts at "from" into position starts at "to", cover the aborigines at "to"
    int moves;
    int32_t enc = intrev32ifbe(is->encoding);

    if(enc == INTSET_ENC_INT64){
        int64_t *dst = ((int64_t *)is->contents) + from;
        int64_t *src = ((int64_t *)is->contents) + to;
        moves = enc*(intrev32ifbe(is->length)-from);
    }
    else if(enc == INTSET_ENC_INT32){
        int32_t *dst = ((int32_t *)is->contents) + from;
        int32_t *src = ((int32_t *)is->contents) + to;
        moves = enc*(intrev32ifbe(is->length)-from);
    }
    else{
        int16_t *dst = ((int16_t *)is->contents) + from;
        int16_t *src = ((int16_t *)is->contents) + to;
        moves = enc*(intrev32ifbe(is->length)-from);
    }

    memmove(dst, src, moves);
}


IST *intsetAdd(IST *is, int64_t value, uint8_t *success){
    //add the given integer into the intset, if the integer alrealy exists, *success=0, else *success=1
    uint8_t newenc;
    uint32_t pos;

    newenc = _intsetValueEncoding(value);
    if(newenc > intrev32ifbe(is->encoding)){
         is = intsetUpgradeAndAdd(is, value);
         if(success) *success = 1;//Don't forget the if before it!
         return is;
    }
    else{
        if(intsetSearch(is, value, &pos)){
             if(success) *success = 0;
             return is;
        }
        else{
            is = intsetResize(is, intrev32ifbe(intretv32ifbe(is->length)+1));
            if(pos < intrev32ifbe(length)){
                 intsetMoveTail(is, pos, pos+1);
            }
            _intsetSet(is, pos, value);

            is->length = intrev32ifbe(intrev32ifbe(is->length)+1);
            if(success) *success = 1;

            return is;
        }
    }
}

/*   intsetRemove------------remove the given element from the intset -----------O(N)   */
IST *intsetRemove(IST *is, int64_t value, int *success){
    int8_t enc = _intsetValueEncoding(value);
    uint32_t pos;

    if((enc > intrev32ifbe(is->encoding)) || (intsetSearch(is, value, &pos) == 0))
    {
         if(success) *success=0;
    }
    else{
        if(pos <intrev32ifbe(length)-1) intsetMoveTail(is, pos+1, pos);
        intsetResize(is, intrev32ifbe(length)-1);
        is->length = intrev32ifbe(intrev32ifbe(is->length)-1);

        if(success) *success=1;
    }

    return is;
}

/*    intsetFind---------------------check wether the given value is in intset-------O(logN) */
uint8_t intsetFind(IST *is, int64_t value){
    return (_intsetValueEncoding(value) <= intrev32ifbe(is->encoding)) && (intsetSearch(is, value, NULL));
}

/*   intsetRandom--------------------return a element from the intset randomly-------O(1)  */
int64_t intsetRandom(IST *is){
    return _intsetGet(is, rand() % intrev32ifbe(is->length));
}

/*   intsetGet----------------------return the element by the given index------------O(1)   */
uint8_t intsetGet(IST *is, uint32_t pos, int64_t *value){
    if(pos < intrev32ifbe(is->length)){
        *value = _intsetGet(is, pos);
        return 1;
    }
    else{
        return 0;
    }
}

/*   intsetLen----------------------return the number of integer elements the intset holds ---------O(1)   */
uint32_t intsetLen(IST *is){
     return intrev32ifbe(is->length);
}

/*   intsetBlobLen-----------------return the bytes the intset takes -----------------------------O(1)    */
size_t intsetBlobLen(IST *is){
    return sizeof(IST) + intrev32ifbe(is->length)*intrev32ifbe(is->encoding);
}
