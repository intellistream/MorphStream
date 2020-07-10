#include "../include/streamjoin.h"
#include <iostream>

void streamjoin::printVersion() {
  std::cout << "Hello, this is version 1.0.0!" << std::endl;
}

relation_t streamjoin::generateRelation() {
  relation_t relR;
  relR.payload = new relation_payload_t();
  result_t *results;
}
