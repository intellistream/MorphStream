#include <stdio.h>
#include <stdint.h>
#include <stdlib.h>
#include <string.h>

#include "client.h"

#define KV_KEYLEN_LIMIT 64

enum ReqType {
    INSERT,
    SEARCH,
    UPDATE,
    DELETE
};

typedef struct TagClientCmd {
    ReqType  cmdType;
    char     key[KV_KEYLEN_LIMIT];
    uint64_t klen;
    int64_t  value;
    uint64_t vlen;
} ClientCmd;

static int checkNumber(char * str) {
    for (int i = 0; i < strlen(str); i++) {
        if (str[i] < '0' || str[i] > '9') {
            return -1;
        }
    }
    return 0;
} 

static void usage() {
    printf("==== Usage ====\n");
    printf("put key value\n");
    printf("get key\n");
    printf("del key\n");
    printf("===============\n");
}

int parseInput(char * buf, __OUT ClientCmd * cmd) {
    int ret = -1;
    char * p = strtok(buf, " ");
    char * parsed[3]; // parsed[0]: cmdName, parsed[1]: key, parsed[2]: value
    // fetch key and value to the parsed
    for (int i = 0; i < 3; i++) {
        parsed[i] = p;
        p = strtok(NULL, " ");
    }

    // parse commands
    if (!strcmp(parsed[0], "search") || !strcmp(parsed[0], "SEARCH")) {
        // check if parsed[1] exists
        if (parsed[1] == NULL) {
            printf("Usage: %s key\n", parsed[0]);
            return -1;
        }
        // check if the length of the key exceeds the limit
        int klen = strlen(parsed[1]);
        if (klen > KV_KEYLEN_LIMIT) {
            printf("Error: key should be less than %d characters\n", KV_KEYLEN_LIMIT);
            return -1;
        }
        // copy the key to the ClientCmd
        memcpy(cmd->key, parsed[1], klen);
        // set other arguments
        cmd->cmdType = SEARCH;
        cmd->klen = klen;
        cmd->vlen = 0;
        return 0; // return success here
    } else if (!strcmp(parsed[0], "insert") || !strcmp(parsed[0], "INSERT")) {
        if (parsed[1] == NULL || parsed[2] == NULL) {
            printf("Usage: %s key value\n", parsed[0]);
            return -1;
        }
        // check if the length of the key exceeds the limit
        int klen = strlen(parsed[1]);
        if (klen > KV_KEYLEN_LIMIT) {
            printf("Error: key should be less than %d characters\n", KV_KEYLEN_LIMIT);
            return -1;
        }
        // check if the second argument is a number
        ret = checkNumber(parsed[2]);
        if (ret < 0) {
            printf("Error: value should be an integer number\n");
            return -1;
        }
        // set cmd
        cmd->cmdType = INSERT;
        memcpy(cmd->key, parsed[1], klen);
        cmd->klen = klen;
        cmd->value = atoll(parsed[2]);
        cmd->vlen = sizeof(int64_t);
        return 0;
    } else if (!strcmp(parsed[0], "update") || !strcmp(parsed[0], "UPDATE")) {
        if (parsed[1] == NULL || parsed[2] == NULL) {
            printf("Usage: %s key value\n", parsed[0]);
            return -1;
        }
        // check if the length of the key exceeds the limit
        int klen = strlen(parsed[1]);
        if (klen > KV_KEYLEN_LIMIT) {
            printf("Error: key should be less than %d characters\n", KV_KEYLEN_LIMIT);
            return -1;
        }
        // check if the second argument is a number
        ret = checkNumber(parsed[2]);
        if (ret < 0) {
            printf("Error: value should be an integer number\n");
            return -1;
        }
        // set cmd
        cmd->cmdType = UPDATE;
        memcpy(cmd->key, parsed[1], klen);
        cmd->klen = klen;
        cmd->value = atoll(parsed[2]);
        cmd->vlen = sizeof(int64_t);
        return 0;
    } else if (!strcmp(parsed[0], "delete") || !strcmp(parsed[0], "DELETE")) {
        // check if the key exists
        if (parsed[1] == NULL) {
            printf("Usage: %s key\n", parsed[0]);
            return -1;
        }
        // check if the length of the key exceeds the limit
        int klen = strlen(parsed[1]);
        if (klen > KV_KEYLEN_LIMIT) {
            printf("Error: key should be less than %d characters\n", KV_KEYLEN_LIMIT);
            return -1;
        }
        // set cmd
        cmd->cmdType = DELETE;
        memcpy(cmd->key, parsed[1], klen);
        cmd->klen = klen;
        return 0; // return success here
    } else if (!strcmp(parsed[0], "quit") || !strcmp(parsed[0], "q") || !strcmp(parsed[0], "exit")) {
        exit(0);
    } else if (!strcmp(parsed[0], "help")) {
        usage();
        return 0;
    }
    else {
        // no match cmd
        printf("Error: command not supported\n");
        return -1;
    }
    return -1;
}

typedef struct TagRetVal {
    union {
        int ret_code;
        void * val_addr;
    } ret_val;
} RetVal;

static RetVal * clientShellExe(Client * client, ClientCmd * cmd) {
    RetVal * ret_val = (RetVal *)malloc(sizeof(RetVal));
    char buf[17] = {0};

    void * client_local_addr = client->get_local_buf_mr()->addr;
    uint64_t client_input_addr = (uint64_t)client->get_input_buf();
    
    KVReqCtx ctx;
    KVInfo   kv_info;
    memset((void *)client_input_addr, 0, 1024);
    memcpy((void *)(client_input_addr + sizeof(KVLogHeader)), cmd->key, cmd->klen);
    memcpy((void *)(client_input_addr + sizeof(KVLogHeader) + cmd->klen), &cmd->value, cmd->vlen);
    KVLogHeader * header = (KVLogHeader *)client_input_addr;
    header->key_length = cmd->klen;
    header->value_length = cmd->vlen;
    header->is_valid = true;

    KVLogTail * tail = (KVLogTail *)((uint64_t)client_input_addr
        + sizeof(KVLogHeader) + header->key_length + header->value_length);
    
    kv_info.key_len = cmd->klen;
    kv_info.l_addr  = (void *)client_input_addr;
    kv_info.lkey    = client->get_input_buf_lkey();
    kv_info.value_len = cmd->vlen;

    ctx.kv_info = &kv_info;
    ctx.coro_id = 0;
    ctx.use_cache = true;
    ctx.lkey = client->get_local_buf_mr()->lkey;

    switch (cmd->cmdType) {
    case SEARCH:
        ctx.req_type = KV_REQ_SEARCH;
        printf("searching\n");
        client->init_kv_search_space(client_local_addr, &ctx);
        ret_val->ret_val.val_addr = client->kv_search(&ctx);
        break;
    case UPDATE:
        ctx.req_type = KV_REQ_UPDATE;
        tail->op = KV_OP_UPDATE;
        client->init_kv_update_space(client_local_addr, &ctx);
        ret_val->ret_val.ret_code = client->kv_update(&ctx);
        break;
    case DELETE:
        ctx.req_type = KV_REQ_DELETE;
        client->init_kv_delete_space(client_local_addr, &ctx);
        ret_val->ret_val.ret_code = client->kv_delete(&ctx);
        break;
    case INSERT:
        ctx.req_type = KV_REQ_INSERT;
        tail->op = KV_OP_INSERT;
        printf("inserting\n");
        client->init_kv_insert_space(client_local_addr, &ctx);
        ret_val->ret_val.ret_code = client->kv_insert(&ctx);
        break;
    default:
        ret_val->ret_val.ret_code = -1;
    }
    return ret_val;
}

int main(int argc, char ** argv) {
    int ret = 0;
    if (argc != 2) {
        printf("Usage: %s path-to-config\n", argv[0]);
        return 1;
    }
    GlobalConfig config;
    ret = load_config(argv[1], &config);
    // assert(ret == 0);
    RetVal * ret_val = NULL;

    Client client(&config);

    boost::fibers::fiber polling_fb = client.start_polling_fiber();

    while (1) {
        char buf[256];
        // cmdline hint
        printf("mykv >> ");

        // get input
        fgets(buf, 256, stdin);
        printf("buf: %s\n", buf);
        buf[strlen(buf) - 1] = '\0';
        
        // parse command
        ClientCmd cmd;
        ret = parseInput(buf, &cmd);
        if (ret < 0) {
            printf("parse failed\n");
            continue;
        }

        // execute command
        ret_val = clientShellExe(&client, &cmd);
        if (ret < 0) {
            printf("%s failed\n", buf);
            continue;
        }

        // print result
        if (cmd.cmdType == SEARCH) {
            if (ret_val->ret_val.val_addr != NULL)
                printf("%ld\n", *(uint64_t *)ret_val->ret_val.val_addr);
            else
                printf("key not found\n");
        } else {
            printf("%d\n", ret_val->ret_val.ret_code);
        }
        free(ret_val);
    }
    polling_fb.join();
}