#include "kv_utils.h"

#include <assert.h>

int run_server(struct GlobalConfig * conf);
int run_client(struct GlobalConfig * conf);

int main(int argc, char ** argv) {
    // assert(argc == 2);
    char * conf_file_name = argv[1];
    struct GlobalConfig conf;
    int ret = 0;

    ret = load_config(conf_file_name, &conf);
    if (ret != 0) {
        return 1;
    }

    if (conf.role == SERVER) {
        ret = run_server(&conf);
    } else {
        // assert(conf.role == CLIENT);
        ret = run_client(&conf);
    }
}