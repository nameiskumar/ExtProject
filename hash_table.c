#include "hash_table.h"

// initialize the components of the hashtable
void init(hashtable *ht) {
    // This line tells the compiler that we know we haven't used the variable
    // yet so don't issue a warning. You should remove this line once you use
    // the parameter.
    ht = (hashtable *)(malloc(sizeof(hashtable)));
    int i;

    for (i=0 ; i<10 ; i++)
    {
	ht->ptr[i] = NULL;
    }

}

int gen_Hash(keyType key)
{
    return key%7;
}

// insert a key-value pair into the hash table
void put(hashtable *ht, keyType key, valType value) {
    
    printf(" I m in put");
    printf("key is %d",key);
    printf("value is %d",value);

    int hvalue = gen_Hash(key);

    printf("hash value is %d",value);

    node *n = (node *)(malloc(sizeof(node)));
    
    n->element = value;
    n->prev = ht->ptr[hvalue];
    //(ht->ptr[hvalue])->next = n;

    if (ht->ptr[hvalue] != NULL)
    {
	(ht->ptr[hvalue])->next = n;
    }

    n->next = NULL;
    ht->ptr[hvalue] = n;
}

/*int gen_hash(keyType key)
{
    return key%7;
}*/

// get entries with a matching key and stores the corresponding values in the
// values array. The size of the values array is given by the parameter
// num_values. If there are more matching entries than num_values, they are not
// stored in the values array to avoid a buffer overflow. The function returns
// the number of matching entries. If the return value is greater than
// num_values, the caller can invoke this function again (with a larger buffer)
// to get values that it missed during the first call. 
/*int get(hashtable* ht, keyType key, valType *values, int num_values) {
    (void) ht;
    (void) key;
    (void) values;
    (void) num_values;
    return 0;
}

// erase a key-value pair from the hash talbe
void erase(hashtable* ht, keyType key) {
    (void) ht;
    (void) key;
}*/

int main(void) 
{
    hashtable ht;
    keyType key;
    valType value;

    init(&ht);

    scanf("%d", &key);
    scanf("%d", &value);

    put(&ht,key,value);    


    return 0;
}

