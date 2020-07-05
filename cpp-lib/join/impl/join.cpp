#include <impl/join.h>

#include <stdio.h>

void print_version(void) {
  const char *m = "Not debug";
  printf("This is foo version %s (%s)\n", FOO_VERSION, m);
}
