//
// Created by tony on 10.07.20.
//

#ifndef STREAMJOIN_INCLUDE_TYPES_H_
#define STREAMJOIN_INCLUDE_TYPES_H_

#include <cstdint>

//#define AUX_TYPE
#ifdef AUX_TYPE /* 64-bit key/value, 16B tuples */
typedef int32_t intkey_t;
typedef table_t value_t;
#else /* 32-bit key/value, 8B tuples */
typedef int32_t intkey_t;
typedef int32_t value_t;
#endif

/**
 * Type definition for a tuple, depending on KEY_8B a tuple can be 16B or 8B
 * @note this layout is chosen as a work-around for AVX double operations.
 */
struct tuple_t {     // 8bytes.
  value_t payloadID; // record the index of payload of this tuple.
  intkey_t key;      // little-endian, lowest is the most significant bit.
};

/**
 *  Type definition for a structure to save real payload, let original payload be index of this struct
 *  Currently, we assume only timestamp is stored.
 */
struct relation_payload_t {
  uint64_t *ts;//add timestamp for each tuple in the relation.
};


/**
 * Type definition for a relation.
 * It consists of an array of tuples and actual payloads and a size of the relation.
 */
struct relation_t {
  tuple_t *tuples;
  uint64_t num_tuples;
  relation_payload_t *payload;
};


/** Holds the join results of a thread */
struct threadresult_t {
  int64_t nresults;
  void *results;
  uint32_t threadid;
};

/** Type definition for join results. */
struct result_t {
  int64_t totalresults;
  threadresult_t *resultlist;
  int nthreads;
};


#endif // STREAMJOIN_INCLUDE_TYPES_H_
