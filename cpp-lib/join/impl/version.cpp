#include <impl/version.h>

#include <stdio.h>

void print_version(void) {
  const char *m = "Not debug";
  printf("This is foo version %s (%s)\n",  m);
}
