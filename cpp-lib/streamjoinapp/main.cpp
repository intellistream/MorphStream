#include <iostream>
#include <streamjoin.h>

/**
 * Assume the input stream windowing is managed by application, stream join lib
 * only "see" a window of tuples. Lazy and Eager stream join lib will have
 * different APIs.
 * @return
 */
int main() {
  streamjoin join;
  join.printVersion();

  return 0;
}
