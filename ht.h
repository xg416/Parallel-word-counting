// Simple hash table implemented in C @ https://github.com/benhoyt/ht/blob/master/ht.h

#ifndef _HT_H
#define _HT_H

#include <stddef.h>
#include <stdlib.h>
#include <string.h>
#include <stdint.h>

#define FNV_OFFSET 14695981039346656037UL
#define FNV_PRIME 1099511628211UL

// Item of hashtable is a chained list
struct item{
    char *key;
    int count;
};

typedef struct item item;

struct ht
{
    item **entries; // one * for list, one * for struct item
    size_t itemcount;
    size_t capacity;
};

// Hash table structure: create with ht_create, free with ht_destroy.
typedef struct ht ht;

// Create hash table and return pointer to it, or NULL if out of memory.
ht* ht_create(int size);

// update the hashtable with the input key and count
item *ht_update(struct ht* table, char *key, int count);

// Free memory allocated for hash table, including allocated keys.
void ht_destroy(ht* table);

// Get item with given key (NUL-terminated) from hash table. Return
// value (which was set with ht_set), or NULL if key not found.
void* ht_get(ht* table, const char* key);

// Move iterator to next item in hash table, update iterator's key
// and value to current item, and return true. If there are no more
// items, return false. Don't call ht_set during iteration.
// bool ht_next(ht* it);

// hash code 
uint64_t hashcode(const char* key);

// print the key and count pairs in the hashtable
void printTable(ht *table);


ht *ht_create(int size)
{
    int i;
    ht *table = (ht *)malloc(sizeof(struct ht));
    table->itemcount = 0;
    table->capacity = size;

    table->entries = (item **)malloc(sizeof(struct item *) * size);
    if (table->entries == NULL) {
        free(table); // error, free table before we return!
        return NULL;
    }
    for (i = 0; i < size; i++) {table->entries[i] = NULL;}
    return table;
}

void freeHT(struct ht* table){
    // First free allocated keys.
    int i;
    struct item *entry;
    for (i = 0; i < table->capacity; i++) {
        entry = table->entries[i];
        if (entry != NULL){
            free(entry->key);
            free(entry);
        }
    }

    // Then free entries array and table itself.
    free(table->entries);
    free(table);
}


// Return 64-bit FNV-1a hash for key (NUL-terminated). See description:
// https://en.wikipedia.org/wiki/Fowler–Noll–Vo_hash_function
uint64_t hashcode(const char* key) {
    uint64_t hash = FNV_OFFSET;
    const char* p;
    for (p = key; *p; p++) {
        hash ^= (uint64_t)(unsigned char)(*p);
        hash *= FNV_PRIME;
    }
    return hash;
}


item *ht_update(struct ht* table, char *key, int count)
{
    // char checkWord[] = "smart";
    struct item *my_item;
    int index = hashcode(key) % table->capacity;
    my_item = table->entries[index];
    /* Search for duplicate value */
    while (my_item != NULL)
    {
        index++;
        if (strcmp(key, my_item->key) == 0){
            my_item->count += count;
            return my_item;
        }
        if (index == table->capacity) index=0;
        my_item = table->entries[index];
    }
    /* Create new node if no duplicate is found */
    my_item = (item *)malloc(sizeof(struct item));
    // strcpy(my_item->key, key); can use this if we explicitly allocate memory: my_item->key = malloc(strlen(key)+1);
    // strdup function dynamically allocate memory on the heap. Need to free manually
    my_item->key = strdup(key);
    my_item->count = count;
    table->entries[index] = my_item;
    table->itemcount = table->itemcount + 1;
    return my_item;
}


void printTable(ht *table)
{
    int i;
    item *current;

    for (i = 0; i < table->capacity; i++)
    {
        current = table->entries[i];
        if (current == NULL)
            continue;
        else{
            printf("i: %d, key: %s, count: %d\n", i, current->key, current->count);
        }
    }
}

#endif // _HT_H