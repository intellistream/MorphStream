# Setup for Kernel Bypass
* Kernel network stack can be bypassed using an alternate stack of of software like **Netmap** **DPDK** and user space stacks like **mTCP**

# Netmap Setup
* Netmap uses vale as the software switch. Follow steps (1-4) from [here](https://github.com/networkedsystemsIITB/Modified_mTCP/blob/master/mTCP_over_Netmap/docs/netmap_docs/user_manual.pdf)
* In netmap setup, mTCP requires multi-queue in vale.
* Copy the `netmap_vale.c` to **netmap/sys/dev/netmap/** and compile netmap again. We have tested netmap with igb driver.
* Create interface for each VM with no.of of queues=no of cores of VM.
* `./vale−ctl −n viX −C 2048,2048,Y,Y`
    * where Y i s the number o f cores of the VM, X will be a number ( 1 , 2 , 3 .. )
* `./vale−ctl −a vale 0:viX`
    * ifconfig viX hw ether 00:aa:bb:cc:dd:0Z //MAC address of the VM
* change ifname in XML to `vale0:viX`

* Install mTCP in VM as given in the [mTCP github page](https://github.com/eunyoung14/mtcp/blob/master/README.netmap)

# DPDK Setup:
* We could not get mTCP to work on VMs on DPDK as we were unable to expose multiqueue to the VM. Follow the [steps](https://github.com/eunyoung14/mtcp) for the DPDK + mTCP setup on host machine:

## Using mTCP:
* Make sure to configure and make mTCP whenever you want to change #cores of your VM
	* `./configure --enable-netmap CFLAGS="-DMAX_CPUS=32"` if #cores = 32
        * `sudo make`
	* mTCP configuration needs a **server.conf** file. You can find an example [here](../../examples/abc/layman/server.conf)
	* Changes required in **server.conf**
		* **port=<network_interface_name>**
		* **num_cores=<number_of_cores_of_your_VM>**
		* For mTCP over DPDK, comment line **6**, uncomment line **7**
	* Give the path to your mTCP folder in [CMakeLists.txt](../../CMakeLists.txt) on line **55, 56**
	* For mTCP over netmap stack, set the netmap path on line **88** of CMakeLists.txt(../../CMakeLists.txt)
	* Use proper mTCP paths in your VNF Makefile
	* You need not recompile and install libvnf every time you change #cores of VM, but you should recompile your VNF code
