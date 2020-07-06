#include <join_impl/version.h>

#include <stdio.h>

void print_version(void) {
  const char *m = "Not debug";
  printf("This is Join Lib version %s (%s)\n", JOIN_IMPL_VERSION, m);
}
