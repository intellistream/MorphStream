DEBUG=-g
CPP_OPTIMIZE=0

SL_KERNEL_STATIC=kernel-static
SL_KERNEL_DYNAMIC=kernel-dynamic
SL_KERNELBYPASS_STATIC=kernelbypass-static
SL_KERNELBYPASS_DYNAMIC=kernelbypass-dynamic
CXX_FLAGS=-fpermissive -std=c++11 ${DEBUG} -O$(CPP_OPTIMIZE) -Wno-write-strings -Wall -Wcomment -fPIC 

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
JAVA_INCLUDE=-I${JAVA_HOME}/../include -I${JAVA_HOME}/../include/linux
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
	DPDK_LISL_FLAGS = $(shell cat /lib/ldflags.txt)
	LIBS += -ldl -march=native -export-dynamic -L$(MTCP_P_FLD)/dpdk/lib $(DPDK_LISL_FLAGS)
else
	LIBS += -ldl -march=native -export-dynamic -L$(MTCP_P_FLD)/dpdk/lib $(DPDK_LISL_FLAGS)
endif

all:

$(SL_KERNELBYPASS_STATIC): sl.cpp
	g++ $(CXX_FLAGS) sl.cpp -c
	g++ sl.o -o $(SL_KERNELBYPASS_STATIC) $(CXX_FLAGS) -lvnf-kernelbypass-static $(LIBS) $(INCLUDES)

$(SL_KERNELsl.PASS_DYNAMIC): sl.cpp
	g++ sl.cpp -o $(SL_KERNELsl.PASS_DYNAMIC) $(CXX_FLAGS) -lvnf-kernelbypass-dynamic $(LIBS) $(INCLUDES)

$(SL_KERNEL_STATIC): sl.cpp
	g++ $(CXX_FLAGS) sl.cpp -c
	g++ sl.o -O${CPP_OPTIMIZE} $(DEBUG) -std=c++11 -lvnf-kernel-static -lpthread -lboost_system -lsctp -lfmt -ljsoncpp -o $(SL_KERNEL_STATIC)

$(SL_KERNEL_DYNAMIC): sl.cpp
	g++ sl.cpp -O$(CPP_OPTIMIZE) $(DEBUG) -Wno-pointer-arith -shared -fPIC $(JAVA_INCLUDE) -std=c++11 -lvnf-kernel-dynamic -lpthread -lboost_system -ljsoncpp -o $(SL_KERNEL_DYNAMIC)

clean:
	rm -f *~ *.o *.log $(SL_KERNEL_STATIC) $(SL_KERNEL_DYNAMIC) $(SL_KERNELBYPASS_STATIC) $(SL_KERNELBYPASS_DYNAMIC)  log_*