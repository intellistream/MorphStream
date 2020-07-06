#ifndef _GNU_SOURCE
#define _GNU_SOURCE
#endif

//#include "benchmark.h"
#include "impl/version.h"
//#include "timer/clock.h"
#include <algorithm>
#include <zconf.h>

int main(int argc, char *argv[]) {
  print_version();
  return 0;
}
