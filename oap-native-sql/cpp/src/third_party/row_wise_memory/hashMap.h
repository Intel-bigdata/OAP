#pragma once

#include <stdlib.h>
#include <string.h>

#include "third_party/row_wise_memory/unsafe_row.h"

#define MAX_HASH_MAP_CAPACITY (1 << 29)  // must be power of 2

#define HASH_NEW_KEY -1
#define HASH_FOUND_MATCH -2
#define HASH_FULL -3

#define loadFactor 0.5

/** HashMap Layout
 *
 * keyArray: Fixed Size Array to map hash key to payloads
 * each item has 8 bytes, 4 for a hash32 key and 4 for location in bytesMap
 * | key-hash(4 bytes) | bytesMap offset(4 bytes) |
 *
 * BytesMap: map to store key and value data
 * each item has format as below, same key items will be linked (Min size is 8 bytes when
 * key and value both 0)
 * | total-length(2 bytes) | key-length(2 bytes) | key data(variable-size) | value
 *data(variable-size) | next value ptr(4 bytes) |
 *
 **/

typedef struct {
  int arrayCapacity;  // The size of the keyArray
  uint8_t bytesInKeyArray;
  size_t mapSize;  // The size of the bytesMap
  int cursor;
  int numKeys;
  bool needSpill;
  char* keyArray;  //<32-bit key hash,32-bit offset>, hash slot itself.
  char* bytesMap;  // use to save the  key-row, and value row.
} unsafeHashMap;   /*general purpose hash structure*/

static inline void dump(unsafeHashMap* hm) {
  printf("=================== HashMap DUMP =======================\n");
  printf("keyarray capacity is %d\n", hm->arrayCapacity);
  printf("bytemap capacity is %d\n", hm->mapSize);
  printf("bytesInKey is %d\n", hm->bytesInKeyArray);
  printf("cursor is %d\n", hm->cursor);
  printf("numKeys is %d\n", hm->numKeys);
  printf("keyArray[offset_in_bytesMap, hashVal] is\n");
  for (int i = 0; i < hm->arrayCapacity * hm->bytesInKeyArray;
       i = i + hm->bytesInKeyArray) {
    char* pos = hm->keyArray + i;
    if (*((int*)pos) == -1) continue;
    printf("%d: ", i / hm->bytesInKeyArray);
    auto numFields = (hm->bytesInKeyArray % 4) > 0 ? (hm->bytesInKeyArray / 4 + 1)
                                                   : (hm->bytesInKeyArray / 4);
    for (int j = 0; j < numFields; j++) {
      if ((hm->bytesInKeyArray - j * 4) < 4) {
        int tmp = 0;
        memcpy(&tmp, (pos + j * 4), (hm->bytesInKeyArray - j * 4));
        printf("%04x  ", tmp);  // value_data
      } else {
        printf("%04x    ", *((int*)(pos + j * 4)));
      }
    }
    printf("\n");
  }
  printf("bytesMap is\n");
  int pos = 0;
  int idx = 0;
  while (pos < hm->cursor) {
    printf("%d: ", idx++);
    auto first_4 = *(int*)(hm->bytesMap + pos);
    auto total_length = first_4 >> 16;
    auto key_length = first_4 & 0x00ff;
    auto value_length =
        total_length - key_length - 8;  // 12 includes first 4 bytes, last 4 bytes
    printf("[%04x, %d, %d, %d]", pos, total_length, key_length, value_length);
    printf("%04x  ", first_4);  // total_length + key_length
    int i = 0;
    while (i < key_length) {
      if ((key_length - i) < 4) {
        int tmp = 0;
        memcpy(&tmp, (hm->bytesMap + pos + 4 + i), (key_length - i));
        printf("%04x  ", tmp);  // value_data
        i = key_length;
      } else {
        printf("%04x  ", *(int*)(hm->bytesMap + pos + 4 + i));  // key_data
        i += 4;
      }
    }
    i = 0;
    while (i < value_length) {
      if ((value_length - i) < 4) {
        int tmp = 0;
        memcpy(&tmp, (hm->bytesMap + pos + 4 + key_length + i), (value_length - i));
        printf("%04x  ", tmp);  // value_data
        i = value_length;
      } else {
        printf("%04x  ", *(int*)(hm->bytesMap + pos + 4 + key_length + i));  // value_data
        i += 4;
      }
    }
    printf("%04x  ",
           *(int*)(hm->bytesMap + pos + 4 + key_length + value_length));  // next_ptr
    printf("\n");
    pos += total_length;
  }
}

static inline int getTotalLength(char* base) { return *((int*)base) >> 16; }

static inline int getKeyLength(char* base) { return *((int*)(base)) & 0x00ff; }

/* If keySize > 0, we should put raw key also in keyArray */
/* Other wise we put key in bytesMap */
static inline unsafeHashMap* createUnsafeHashMap(int initArrayCapacity,
                                                 int initialHashCapacity,
                                                 int keySize = -1) {
  unsafeHashMap* hashMap =
      (unsafeHashMap*)nativeMalloc(sizeof(unsafeHashMap), MEMTYPE_HASHMAP);
  uint8_t bytesInKeyArray = (keySize == -1) ? 8 : 8 + keySize;
  hashMap->bytesInKeyArray = bytesInKeyArray;
  hashMap->keyArray =
      (char*)nativeMalloc(initArrayCapacity * bytesInKeyArray, MEMTYPE_HASHMAP);
  hashMap->arrayCapacity = initArrayCapacity;
  memset(hashMap->keyArray, -1, initArrayCapacity * bytesInKeyArray);

  hashMap->bytesMap = (char*)nativeMalloc(initialHashCapacity, MEMTYPE_HASHMAP);
  hashMap->mapSize = initialHashCapacity;

  hashMap->cursor = 0;
  hashMap->numKeys = 0;
  hashMap->needSpill = false;
  return hashMap;
}

static inline void destroyHashMap(unsafeHashMap* hm) {
  if (hm != NULL) {
    if (hm->keyArray != NULL) nativeFree(hm->keyArray);
    if (hm->bytesMap != NULL) nativeFree(hm->bytesMap);

    nativeFree(hm);
  }
}

static inline int getRecordLengthFromBytesMap(char* record) {
  return *((int*)record) >> 16;
}

static inline int getkLenFromBytesMap(char* record) {
  int klen = *((int*)(record)) & 0x00ff;
  return klen;
}

static inline int getvLenFromBytesMap(char* record) {
  int totalLengh = *((int*)record) >> 16;
  int klen = *((int*)(record)) & 0x00ff;
  return (totalLengh - 8 - klen);
}

static inline char* getKeyFromBytesMap(char* record) { return (record + 4); }

static inline char* getValueFromBytesMap(char* record) {
  int klen = *((int*)(record)) & 0x00ff;
  return (record + 4 + klen);
}

static inline int getNextOffsetFromBytesMap(char* record) {
  int totalLengh = *((int*)record) >> 16;
  return *((int*)(record + totalLengh - 4));
}

static inline int getvLenFromBytesMap(unsafeHashMap* hashMap, int KeyAddressOffset) {
  char* record = hashMap->bytesMap + KeyAddressOffset;
  return getvLenFromBytesMap(record);
}

static inline int getNextOffsetFromBytesMap(unsafeHashMap* hashMap,
                                            int KeyAddressOffset) {
  char* record = hashMap->bytesMap + KeyAddressOffset;
  return getNextOffsetFromBytesMap(record);
}

static inline int getValueFromBytesMapByOffset(unsafeHashMap* hashMap,
                                               int KeyAddressOffset, char* output) {
  char* record = hashMap->bytesMap + KeyAddressOffset;
  memcpy(output, getValueFromBytesMap(record), getvLenFromBytesMap(record));
  return KeyAddressOffset;
}

static inline bool growHashBytesMap(unsafeHashMap* hashMap) {
  int oldSize = hashMap->mapSize;
  int newSize = oldSize << 1;
  char* newBytesMap = (char*)nativeRealloc(hashMap->bytesMap, newSize, MEMTYPE_HASHMAP);
  if (newBytesMap == NULL) return false;

  hashMap->bytesMap = newBytesMap;
  hashMap->mapSize = newSize;
  return true;
}

static inline bool growAndRehashKeyArray(unsafeHashMap* hashMap) {
  assert(hashMap->keyArray != NULL);

  int oldCapacity = hashMap->arrayCapacity;
  int newCapacity = (oldCapacity << 1);
  newCapacity =
      (newCapacity >= MAX_HASH_MAP_CAPACITY) ? MAX_HASH_MAP_CAPACITY : newCapacity;
  char* oldKeyArray = hashMap->keyArray;

  // Allocate the new keyArray and zero it
  char* newKeyArray =
      (char*)nativeMalloc(newCapacity * hashMap->bytesInKeyArray, MEMTYPE_HASHMAP);
  if (newKeyArray == NULL) return false;

  memset(newKeyArray, -1, newCapacity * hashMap->bytesInKeyArray);
  int mask = newCapacity - 1;

  int keySizeInBytes = hashMap->bytesInKeyArray;
  if (keySizeInBytes > 8) {
    // Rehash the map
    for (int pos = 0; pos < oldCapacity; pos++) {
      int keyOffset = *(int*)(oldKeyArray + pos * keySizeInBytes);
      int hashcode = *(int*)(oldKeyArray + pos * keySizeInBytes + 4);

      if (keyOffset < 0) continue;

      int newPos = hashcode & mask;
      int step = 1;
      while (*(int*)(newKeyArray + newPos * keySizeInBytes) >= 0) {
        newPos = (newPos + step) & mask;
        step++;
      }
      memcpy(newKeyArray + newPos * keySizeInBytes, oldKeyArray + pos * keySizeInBytes,
             keySizeInBytes);
    }
  } else {
    // Rehash the map
    for (int pos = 0; pos < oldCapacity; pos++) {
      int keyOffset = *(int*)(oldKeyArray + pos * keySizeInBytes);
      int hashcode = *(int*)(oldKeyArray + pos * keySizeInBytes + 4);

      if (keyOffset < 0) continue;

      int newPos = hashcode & mask;
      int step = 1;
      while (*(int*)(newKeyArray + newPos * keySizeInBytes) >= 0) {
        newPos = (newPos + step) & mask;
        step++;
      }
      *(int*)(newKeyArray + newPos * keySizeInBytes) = keyOffset;
      *(int*)(newKeyArray + newPos * keySizeInBytes + 4) = hashcode;
    }
  }

  hashMap->keyArray = newKeyArray;
  hashMap->arrayCapacity = newCapacity;

  nativeFree(oldKeyArray);
  return true;
}
/*
 * return:
 *   0 if exists
 *   -1 if not exists
 */
template <typename CType>
static inline int safeLookup(unsafeHashMap* hashMap, CType keyRow, int hashVal) {
  assert(hashMap->keyArray != NULL);
  int mask = hashMap->arrayCapacity - 1;
  int pos = hashVal & mask;
  int step = 1;
  int keyLength = sizeof(keyRow);
  char* base = hashMap->bytesMap;
  int keySizeInBytes = hashMap->bytesInKeyArray;
  char* keyArrayBase = hashMap->keyArray;

  while (true) {
    int KeyAddressOffset = *(int*)(keyArrayBase + pos * keySizeInBytes);
    int keyHashCode = *(int*)(keyArrayBase + pos * keySizeInBytes + 4);

    if (KeyAddressOffset < 0) {
      // This is a new key.
      return HASH_NEW_KEY;
    } else {
      if ((int)keyHashCode == hashVal) {
        if (keySizeInBytes > 8) {
          if (keyRow == *(CType*)(keyArrayBase + pos * keySizeInBytes + 8)) {
            return 0;
          }
        } else {
          // Full hash code matches.  Let's compare the keys for equality.
          char* record = base + KeyAddressOffset;
          if (keyRow == *((CType*)getKeyFromBytesMap(record))) {
            return 0;
          }
        }
      }
    }

    pos = (pos + step) & mask;
    step++;
  }

  // Cannot reach here
  assert(0);
}

static inline int safeLookup(unsafeHashMap* hashMap, const char* keyRow, size_t keyRowLen,
                             int hashVal) {
  assert(hashMap->keyArray != NULL);
  int mask = hashMap->arrayCapacity - 1;
  int pos = hashVal & mask;
  int step = 1;
  int keyLength = keyRowLen;
  char* base = hashMap->bytesMap;
  int keySizeInBytes = hashMap->bytesInKeyArray;
  char* keyArrayBase = hashMap->keyArray;

  while (true) {
    int KeyAddressOffset = *(int*)(keyArrayBase + pos * keySizeInBytes);
    int keyHashCode = *(int*)(keyArrayBase + pos * keySizeInBytes + 4);

    if (KeyAddressOffset < 0) {
      // This is a new key.
      return HASH_NEW_KEY;
    } else {
      if ((int)keyHashCode == hashVal) {
        // Full hash code matches.  Let's compare the keys for equality.
        char* record = base + KeyAddressOffset;
        if (memcmp(keyRow, getKeyFromBytesMap(record), keyLength)) {
          return 0;
        }
      }
    }

    pos = (pos + step) & mask;
    step++;
  }

  // Cannot reach here
  assert(0);
}

/*
 * return:
 *   0 if exists
 *   -1 if not exists
 */
static inline int safeLookup(unsafeHashMap* hashMap, std::shared_ptr<UnsafeRow> keyRow,
                             int hashVal) {
  assert(hashMap->keyArray != NULL);
  int mask = hashMap->arrayCapacity - 1;
  int pos = hashVal & mask;
  int step = 1;
  int keyLength = keyRow->sizeInBytes();
  char* base = hashMap->bytesMap;

  int keySizeInBytes = hashMap->bytesInKeyArray;
  char* keyArrayBase = hashMap->keyArray;

  while (true) {
    int KeyAddressOffset = *(int*)(keyArrayBase + pos * keySizeInBytes);
    int keyHashCode = *(int*)(keyArrayBase + pos * keySizeInBytes + 4);

    if (KeyAddressOffset < 0) {
      // This is a new key.
      return HASH_NEW_KEY;
    } else {
      if ((int)keyHashCode == hashVal) {
        // Full hash code matches.  Let's compare the keys for equality.
        char* record = base + KeyAddressOffset;
        if ((getKeyLength(record) == keyLength) &&
            (memcmp(keyRow->data, getKeyFromBytesMap(record), keyLength) == 0)) {
          return 0;
        }
      }
    }

    pos = (pos + step) & mask;
    step++;
  }

  // Cannot reach here
  assert(0);
}

/*
 * return:
 *   0 if exists
 *   -1 if not exists
 */
template <typename CType>
static inline int safeLookup(unsafeHashMap* hashMap, CType keyRow, int hashVal,
                             std::vector<char*>* output) {
  assert(hashMap->keyArray != NULL);
  int mask = hashMap->arrayCapacity - 1;
  int pos = hashVal & mask;
  int step = 1;
  int keyLength = sizeof(keyRow);
  char* base = hashMap->bytesMap;

  int keySizeInBytes = hashMap->bytesInKeyArray;
  char* keyArrayBase = hashMap->keyArray;

  while (true) {
    int KeyAddressOffset = *(int*)(keyArrayBase + pos * keySizeInBytes);
    int keyHashCode = *(int*)(keyArrayBase + pos * keySizeInBytes + 4);

    if (KeyAddressOffset < 0) {
      // This is a new key.
      return HASH_NEW_KEY;
    } else {
      if ((int)keyHashCode == hashVal) {
        if (keySizeInBytes > 8) {
          if (keyRow == *(CType*)(keyArrayBase + pos * keySizeInBytes + 8)) {
            char* record = base + KeyAddressOffset;
            while (record != nullptr) {
              (*output).push_back(getValueFromBytesMap(record));
              KeyAddressOffset = getNextOffsetFromBytesMap(record);
              record = KeyAddressOffset == 0 ? nullptr : (base + KeyAddressOffset);
            }
            return 0;
          }
        } else {
          // Full hash code matches.  Let's compare the keys for equality.
          char* record = base + KeyAddressOffset;
          if (keyRow == *((CType*)getKeyFromBytesMap(record))) {
            // there may be more than one record
            while (record != nullptr) {
              (*output).push_back(getValueFromBytesMap(record));
              KeyAddressOffset = getNextOffsetFromBytesMap(record);
              record = KeyAddressOffset == 0 ? nullptr : (base + KeyAddressOffset);
            }
            return 0;
          }
        }
      }
    }

    pos = (pos + step) & mask;
    step++;
  }

  // Cannot reach here
  assert(0);
}

static inline int safeLookup(unsafeHashMap* hashMap, const char* keyRow, size_t keyRowLen,
                             int hashVal, std::vector<char*>* output) {
  assert(hashMap->keyArray != NULL);
  int mask = hashMap->arrayCapacity - 1;
  int pos = hashVal & mask;
  int step = 1;
  int keyLength = keyRowLen;
  char* base = hashMap->bytesMap;
  int keySizeInBytes = hashMap->bytesInKeyArray;
  char* keyArrayBase = hashMap->keyArray;

  while (true) {
    int KeyAddressOffset = *(int*)(keyArrayBase + pos * keySizeInBytes);
    int keyHashCode = *(int*)(keyArrayBase + pos * keySizeInBytes + 4);

    if (KeyAddressOffset < 0) {
      // This is a new key.
      return HASH_NEW_KEY;
    } else {
      if ((int)keyHashCode == hashVal) {
        // Full hash code matches.  Let's compare the keys for equality.
        char* record = base + KeyAddressOffset;
        if (memcmp(keyRow, getKeyFromBytesMap(record), keyLength)) {
          // there may be more than one record
          while (record != nullptr) {
            (*output).push_back(getValueFromBytesMap(record));
            KeyAddressOffset = getNextOffsetFromBytesMap(record);
            record = KeyAddressOffset == 0 ? nullptr : (base + KeyAddressOffset);
          }
          return 0;
        }
      }
    }

    pos = (pos + step) & mask;
    step++;
  }

  // Cannot reach here
  assert(0);
}

static inline int safeLookup(unsafeHashMap* hashMap, std::shared_ptr<UnsafeRow> keyRow,
                             int hashVal, std::vector<char*>* output) {
  assert(hashMap->keyArray != NULL);
  int mask = hashMap->arrayCapacity - 1;
  int pos = hashVal & mask;
  int step = 1;
  int keyLength = keyRow->sizeInBytes();
  char* base = hashMap->bytesMap;

  int keySizeInBytes = hashMap->bytesInKeyArray;
  char* keyArrayBase = hashMap->keyArray;

  while (true) {
    int KeyAddressOffset = *(int*)(keyArrayBase + pos * keySizeInBytes);
    int keyHashCode = *(int*)(keyArrayBase + pos * keySizeInBytes + 4);

    if (KeyAddressOffset < 0) {
      // This is a new key.
      return HASH_NEW_KEY;
    } else {
      if ((int)keyHashCode == hashVal) {
        // Full hash code matches.  Let's compare the keys for equality.
        char* record = base + KeyAddressOffset;
        if ((getKeyLength(record) == keyLength) &&
            (memcmp(keyRow->data, getKeyFromBytesMap(record), keyLength) == 0)) {
          // there may be more than one record
          while (record != nullptr) {
            (*output).push_back(getValueFromBytesMap(record));
            KeyAddressOffset = getNextOffsetFromBytesMap(record);
            record = KeyAddressOffset == 0 ? nullptr : (base + KeyAddressOffset);
          }
          return 0;
        }
      }
    }

    pos = (pos + step) & mask;
    step++;
  }

  // Cannot reach here
  assert(0);
}

/**
 * append is used for same key may has multiple value scenario
 * if key does not exists, insert key and append a new record for key value
 * if key exists, append a new record and linked by previous same key record
 *
 * return should be a flag of succession of the append.
 **/
static inline bool append(unsafeHashMap* hashMap, UnsafeRow* keyRow, int hashVal,
                          char* value, size_t value_size) {
  assert(hashMap->keyArray != NULL);

  const int cursor = hashMap->cursor;
  const int mask = hashMap->arrayCapacity - 1;

  int pos = hashVal & mask;
  int step = 1;

  const int keyLength = keyRow->sizeInBytes();
  char* base = hashMap->bytesMap;
  int klen = keyRow->sizeInBytes();
  const int vlen = value_size;
  const int recordLength = 4 + klen + vlen + 4;
  char* record = nullptr;

  int keySizeInBytes = 8;
  char* keyArrayBase = hashMap->keyArray;

  while (true) {
    int KeyAddressOffset = *(int*)(keyArrayBase + pos * keySizeInBytes);
    int keyHashCode = *(int*)(keyArrayBase + pos * keySizeInBytes + 4);

    if (KeyAddressOffset < 0) {
      // This is a new key.
      int keyArrayPos = pos;
      record = base + cursor;
      // Update keyArray in hashMap
      hashMap->numKeys++;
      *(int*)(keyArrayBase + pos * keySizeInBytes) = cursor;
      *(int*)(keyArrayBase + pos * keySizeInBytes + 4) = hashVal;
      hashMap->cursor += recordLength;
      break;
    } else {
      if ((int)keyHashCode == hashVal) {
        // Full hash code matches.  Let's compare the keys for equality.
        record = base + KeyAddressOffset;
        if ((getKeyLength(record) == keyLength) &&
            (memcmp(keyRow->data, getKeyFromBytesMap(record), keyLength) == 0)) {
          if (cursor + recordLength >= hashMap->mapSize) {
            // Grow the hash table
            assert(growHashBytesMap(hashMap));
            base = hashMap->bytesMap;
            record = base + cursor;
          }

          // link current record next ptr to new record
          int cur_record_lengh = *((int*)record) >> 16;
          auto nextOffset = (int*)(record + cur_record_lengh - 4);
          while (*nextOffset != 0) {
            record = base + *nextOffset;
            cur_record_lengh = *((int*)record) >> 16;
            nextOffset = (int*)(record + cur_record_lengh - 4);
          }
          *nextOffset = cursor;
          record = base + cursor;
          klen = 0;

          // Update hashMap
          hashMap->cursor += (4 + klen + vlen + 4);
          break;
        }
      }
    }

    pos = (pos + step) & mask;
    step++;
  }

  // copy keyRow and valueRow into hashmap
  assert((klen & 0xff00) == 0);
  auto total_key_length = ((8 + klen + vlen) << 16) | klen;
  *((int*)record) = total_key_length;
  memcpy(record + 4, keyRow->data, klen);
  memcpy(record + 4 + klen, value, vlen);
  *((int*)(record + 4 + klen + vlen)) = 0;

  // See if we need to grow keyArray
  int growthThreshold = (int)(hashMap->arrayCapacity * loadFactor);
  if ((hashMap->numKeys > growthThreshold) &&
      (hashMap->arrayCapacity < MAX_HASH_MAP_CAPACITY)) {
    if (!growAndRehashKeyArray(hashMap)) hashMap->needSpill = true;
  }

  return true;
}

/**
 * append is used for same key may has multiple value scenario
 * if key does not exists, insert key and append a new record for key value
 * if key exists, append a new record and linked by previous same key record
 *
 * return should be a flag of succession of the append.
 **/
template <typename CType>
static inline bool append(unsafeHashMap* hashMap, CType keyRow, int hashVal, char* value,
                          size_t value_size) {
  assert(hashMap->keyArray != NULL);

  const int cursor = hashMap->cursor;
  const int mask = hashMap->arrayCapacity - 1;

  int pos = hashVal & mask;
  int step = 1;

  const int keyLength = sizeof(keyRow);
  char* base = hashMap->bytesMap;
  int klen = 0;
  const int vlen = value_size;
  const int recordLength = 4 + +klen + vlen + 4;
  char* record = nullptr;

  int keySizeInBytes = hashMap->bytesInKeyArray;
  char* keyArrayBase = hashMap->keyArray;

  while (true) {
    int KeyAddressOffset = *(int*)(keyArrayBase + pos * keySizeInBytes);
    int keyHashCode = *(int*)(keyArrayBase + pos * keySizeInBytes + 4);

    if (KeyAddressOffset < 0) {
      // This is a new key.
      int keyArrayPos = pos;
      record = base + cursor;
      // Update keyArray in hashMap
      hashMap->numKeys++;
      *(int*)(keyArrayBase + pos * keySizeInBytes) = cursor;
      *(int*)(keyArrayBase + pos * keySizeInBytes + 4) = hashVal;
      *(CType*)(keyArrayBase + pos * keySizeInBytes + 8) = keyRow;
      hashMap->cursor += recordLength;
      break;
    } else {
      if (((int)keyHashCode == hashVal) &&
          (keyRow == *(CType*)(keyArrayBase + pos * keySizeInBytes + 8))) {
        // Full hash code matches.  Let's compare the keys for equality.
        record = base + KeyAddressOffset;
        if (cursor + recordLength >= hashMap->mapSize) {
          // Grow the hash table
          assert(growHashBytesMap(hashMap));
          base = hashMap->bytesMap;
          record = base + cursor;
        }

        // link current record next ptr to new record
        int cur_record_lengh = *((int*)record) >> 16;
        auto nextOffset = (int*)(record + cur_record_lengh - 4);
        while (*nextOffset != 0) {
          record = base + *nextOffset;
          cur_record_lengh = *((int*)record) >> 16;
          nextOffset = (int*)(record + cur_record_lengh - 4);
        }
        *nextOffset = cursor;
        record = base + cursor;

        // Update hashMap
        hashMap->cursor += (4 + klen + vlen + 4);
        break;
      }
    }

    pos = (pos + step) & mask;
    step++;
  }

  // copy keyRow and valueRow into hashmap
  auto total_key_length = ((8 + klen + vlen) << 16) | klen;
  *((int*)record) = total_key_length;
  // memcpy(record + 4, &keyRow, klen);
  memcpy(record + 4 + klen, value, vlen);
  *((int*)(record + 4 + klen + vlen)) = 0;

  // See if we need to grow keyArray
  int growthThreshold = (int)(hashMap->arrayCapacity * loadFactor);
  if ((hashMap->numKeys > growthThreshold) &&
      (hashMap->arrayCapacity < MAX_HASH_MAP_CAPACITY)) {
    if (!growAndRehashKeyArray(hashMap)) hashMap->needSpill = true;
  }

  return true;
}