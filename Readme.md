# DB4NFV

## Environment setup:

DB4NFV contains the following modules:
LibVNF: A C++ based NFV library for deploying VNF instances for per-flow execution.
MorphStream: A java based stream processing engine for cross-flow execution.

Use java 8 for all java modules.


## Running Stateful VNF:

### Compilation:
At the project root directory, run the two following commands to compile LibVNF code (C++)
$ sudo ./scripts/main.sh --compile --KERNEL_STACK
$ sudo ./scripts/main.sh --compile --EXAMPLE_VNF

### Execution (example of TPG-based CC strategy):
Run the main() method in morphStream/morph-clients/src/main/java/cli/FastSLClient.java

The execution flow is as follows:
1. FastSLClient initiates both LibVNF threads (C++) and DB4NFV state manager (Java)
2. LibVNF threads takes input packets, construct transactions, and submit transactions to DB4NFV for state access
3. Transaction results are sent back to LibVNF threads

Currently, DB4NFV state manager and LibVNF threads communicate through JNI.




