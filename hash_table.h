//#ifndef HASH_TABLE_GUARD
//#define HASH_TABLE_GUARD

#include <stdio.h>
#include <stddef.h>
#include <stdlib.h>

#define MAX 10

typedef int keyType;
typedef int valType;

typedef struct node
{
    struct node *prev;
    valType element;
    struct node *next;
}node;

typedef struct hashtable
{
    struct node *ptr[MAX];
// define the components of the hash table here (e.g. the array, bookkeeping for number of elements, et)
}hashtable;

void init(hashtable *ht);
void put(hashtable* ht, keyType key, valType value);
//int get(hashtable* ht, keyType key, valType *values, int num_values);
//void erase(hashtable* ht, keyType key);
int gen_Hash(keyType);

//#endif
