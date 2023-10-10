CXX_FLAGS=-std=c++11 -g -O3 -Wall
B_KERNEL_STATIC=b-kernel-static
B_KERNEL_DYNAMIC=b-kernel-dynamic
A=a
C=c

all: $(A) $(B_KERNEL_STATIC) $(C)

$(B_KERNEL_STATIC): b.cpp
	g++ $(CXX_FLAGS) b.cpp -c 
	g++ b.o -O3 -std=c++11 -lvnf-kernel-static -lpthread -lboost_system -lsctp -o $(B_KERNEL_STATIC)

$(B_KERNEL_DYNAMIC): b.cpp
	g++ b.cpp -O3 -std=c++11 -lvnf-kernel-dynamic -lpthread -lboost_system -o $(B_KERNEL_DYNAMIC)

$(A): a.cpp
	g++ a.cpp -std=c++11 -lpthread -lboost_system -o $(A)

$(C): c.cpp
	g++ c.cpp -std=c++11 -lpthread -lboost_system -o $(C)

clean:
	rm -f *~ *.o $(B_KERNEL_STATIC) $(B_KERNEL_DYNAMIC) $(A) $(C) log_*
