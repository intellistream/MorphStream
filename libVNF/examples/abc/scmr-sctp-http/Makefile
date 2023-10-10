CXX_FLAGS=-std=c++11 -g -O3 -Wall
B_KERNEL_STATIC=b-kernel-static
B_KERNEL_DYNAMIC=b-kernel-dynamic
A=a
C=c.py

all: $(A) $(B_KERNEL_STATIC) $(C)

# todo what will happen if b-kernel-static is directly made without object intermediate
$(B_KERNEL_STATIC): b.cpp
	g++ $(CXX_FLAGS) b.cpp -c 
	g++ b.o -O3 -std=c++11 -lvnf-kernel-static -lpthread -lboost_system -lsctp -o $(B_KERNEL_STATIC)

# run `export LD_LIBRARY_PATH=/usr/local/lib:$LD_LIBRARY_PATH` after making this target
$(B_KERNEL_DYNAMIC): b.cpp
	g++ b.cpp -O3 -std=c++11 -lvnf-kernel-dynamic -lpthread -lboost_system -o $(B_KERNEL_DYNAMIC)

$(A): a.cpp
	g++ a.cpp -std=c++11 -lpthread -lboost_system -lsctp -o $(A)

$(C):
	@echo It is a python file no need to build

clean:
	rm -f *~ *.o $(B_KERNEL_STATIC) $(B_KERNEL_DYNAMIC) $(A) log_*
