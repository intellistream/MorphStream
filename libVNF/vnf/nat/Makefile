DEBUG=false

APP_KERNEL_STATIC=kernel-static
APP_KERNEL_DYNAMIC=kernel-dynamic
APP_KERNELBYPASS_STATIC=kernelbypass-static
APP_KERNELBYPASS_DYNAMIC=kernelbypass-dynamic
APP_KERNEL_STANDALONE=kernel-standalone

CXX_FLAGS=-fpermissive -std=c++11 -Wno-write-strings -Wcomment -fPIC -Wno-pointer-arith
ifeq ($(DEBUG),true)
	CXX_FLAGS += -g -O0
else
	CXX_FLAGS += -O3
endif

# options
DPDK=0
PS=0
NETMAP=1

# mtcp library and header
MTCP_P_FLD  = /home/rahul2514888/trishal/mtcp-p
MTCP_FLD    = $(MTCP_P_FLD)/mtcp
MTCP_INC    = -I$(MTCP_FLD)/include
MTCP_LIB    = -L$(MTCP_FLD)/lib
MTCP_TARGET = $(MTCP_LIB)/libmtcp.a

# mtcp util library and header
UTIL_FLD = $(MTCP_P_FLD)/util
UTIL_INC = -I$(UTIL_FLD)/include
JAVA_INCLUDE=-I${JAVA_HOME}/include -I${JAVA_HOME}/include/linux
UTIL_OBJ = $(UTIL_FLD)/http_parsing.o $(UTIL_FLD)/tdate_parse.o $(UTIL_FLD)/netlib.o

INCLUDES = $(UTIL_INC) $(MTCP_INC) 

LIBS = $(MTCP_LIB) -lmtcp -lpthread -lnuma -lrt -lboost_system

# ps library and header
PS_DIR = $(MTCP_P_FLD)/io_engine/
PS_INC = -I$(PS_DIR)/include
ifeq ($(PS),1)
	INCLUDES += $(PS_INC)
	LIBS += -lmtcp -L$(PS_DIR)/lib -lps -lpthread -lnuma -lrt
endif

# dpdk library and header
DPDK_INC=/include
DPDK_LIB=/lib
ifeq ($(DPDK),1)
	DPDK_MACHINE_FLAGS = $(shell cat /include/cflags.txt)
	INCLUDES += $(DPDK_MACHINE_FLAGS) -I$(DPDK_INC) -include $(DPDK_INC)/rte_config.h
endif

ifeq ($(shell uname -m),x86_64)
	LIBS += -m64
endif

ifeq ($(DPDK),1)
	DPDK_LIAPP_FLAGS = $(shell cat /lib/ldflags.txt)
	LIBS += -ldl -march=native -export-dynamic -L$(MTCP_P_FLD)/dpdk/lib $(DPDK_LIAPP_FLAGS)
else
	LIBS += -ldl -march=native -export-dynamic -L$(MTCP_P_FLD)/dpdk/lib $(DPDK_LIAPP_FLAGS)
endif

all:

$(APP_KERNELBYPASS_STATIC): app.cpp
	g++ $(CXX_FLAGS) app.cpp -c
	g++ app.o -o $(APP_KERNELBYPASS_STATIC) $(CXX_FLAGS) -lvnf-kernelbypass-static $(LIBS) $(INCLUDES)

$(APP_KERNELapp.PASS_DYNAMIC): app.cpp
	g++ app.cpp -o $(APP_KERNELapp.PASS_DYNAMIC) $(CXX_FLAGS) -lvnf-kernelbypass-dynamic $(LIBS) $(INCLUDES)

$(APP_KERNEL_STATIC): app.cpp
	g++ $(CXX_FLAGS) app.cpp -c
	g++ app.o -O${CPP_OPTIMIZE} -lvnf-kernel-static -lpthread -lboost_system -lsctp -lfmt -ljsoncpp -o app-$(APP_KERNEL_STATIC)

$(APP_KERNEL_DYNAMIC): app.cpp
	g++ app.cpp $(CXX_FLAGS) -shared $(JAVA_INCLUDE) -lvnf-kernel-dynamic -lpthread -lboost_system -ljsoncpp -o app-$(APP_KERNEL_DYNAMIC)

$(APP_KERNEL_STANDALONE): app.cpp
	g++ $(CXX_FLAGS) $(JAVA_INCLUDE) app.cpp -c
	g++ app.o -O${CPP_OPTIMIZE} -lvnf-kernel-static -lpthread -lboost_system -lsctp -lfmt -ljsoncpp -o app-$(APP_KERNEL_STANDALONE)

clean:
	rm -f *~ *.o *.log $(APP_KERNEL_STATIC) $(APP_KERNEL_DYNAMIC) $(APP_KERNELBYPASS_STATIC) $(APP_KERNELBYPASS_DYNAMIC)  log_*