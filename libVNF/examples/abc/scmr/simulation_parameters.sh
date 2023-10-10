# Global
TEST_NAME='abc-scmr'

# C
C_VNF='yashsriram@10.129.131.78'
C_FILES='c.cpp Makefile'
C_TARGET='c'
C_IP='127.0.0.1'
C_PORT=6000

# libvnf
LIBVNF_FILES='../../../src ../../../include ../../../CMakeLists.txt'
LIBVNF_CMAKE_COMMAND='cmake .. -DSTACK=KERNEL'
LIBVNF_MAKE_INSTALL_COMMAND='sudo make install'

# B
B_VNF='yashsriram@10.129.131.78'
B_FILES='b.cpp libconfig.json Makefile'
B_TARGET='b-kernel-static'
B_IP='127.0.0.1'
B_PORT=5000

# A
A_VNF='yashsriram@10.129.131.78'
A_FILES='a.cpp Makefile'
A_TARGET='a'
A_THREADS=20
A_DURATION=120
A_IP='127.0.0.1'
A_PORT=4000
