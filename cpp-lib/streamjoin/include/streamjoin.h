//
// Created by tony on 09.07.20.
//

#ifndef STREAMJOIN__STREAMJOIN_H_
#define STREAMJOIN__STREAMJOIN_H_
#include "types.h"
#include <iostream>


class streamjoin{

public:
  void printVersion(); // print the current version of the library.
  relation_t generateRelation(); // generate input relation for testing purpose.

};



#endif // STREAMJOIN__STREAMJOIN_H_
