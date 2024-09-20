
<meta name="robots" content="noindex">

# Hardware Dependencies
- MorphStream is designed to run on a general-purpose multi-core TSPE and does not require any special hardware.
- For optimal performance, we recommend using a machine with at least 24 cores and 300GB of memory.
- This configuration should be sufficient to run the MorphStream artifact effectively.
# Software Dependencies
- To ensure successful compilation, we recommend using a machine with Ubuntu 20.04 with JDK 1.8.0_301 and Mavean 3.8.1.
- Additionally, we set -Xmx and -Xms to be 300GB and use G1GC as the garbage collector arcoss all the experiments.
# Experiment Workflow
## Installation
- Clone the MorphStream repository to your local machine.
- Once downloaded, you can use the provided scripts to compile the source code and install the JAR artifact by running the following command.
```
cd ..
mvn clean install -Dmaven.test.skip=true
cd scripts
```
- The project and JAR directory can be modified in global.sh:
```
project_Dir="/home/username/workspace/MorphStream"
jar_Dir="${project_Dir}/application/target/application-0.0.2-jar-with-dependencies.jar"
```
## Comparing to Conventional SPEs (Fig.10)
- To compare MorphStream with conventional SPEs, we provide a script to run the experiment in /scripts/PerformanceEvaluation/PerformanceComparison
- The script will run the experiment and generate the result in the result directory.
- To run all experiments together, you can run the following command:
```
bash PerformanceComparison.sh
```
- To run MorphStream, TStream and S-Store, you can run the following command:
```
bash morphstream_autorun.sh
```
- To run Flink, you can run the following command:
```
bash flink_autorun.sh
```
## Evaluation on Dynamic Workloads (Fig.11)
- To evaluate the performance of MorphStream under dynamic workloads, we provide a script to run the experiment in /PerformanceEvaluation
- The script will run the experiment and generate the result in the result directory.
- To run the experiment, you can run the following command:
```
bash DynamicWorkload.sh
```
## Evaluation of Multiple Scheduling Strategies (Fig.12)
- To evaluate the performance of MorphStream under multiple scheduling strategies, we provide a script to run the experiment in /PerformanceEvaluation
- The script will run the experiment and generate the result in the result directory.
- To run the experiment, you can run the following command:
```
bash MultipleSchedulingStrategies.sh
```
## Evaluation of System Overhead (Fig.13)
- To evaluate the system overhead of MorphStream, we provide a script to run the experiment in /Overhead
- The script will run the experiment and generate the result in the result directory.
- To run the experiment, you can run the following command:
```
bash SystemOverhead.sh
```
## Evaluation of GC Overhead (Fig.14)
- To evaluate the GC overhead of MorphStream, we provide a script to run the experiment in /Overhead
- The script will run the experiment and generate the result in the result directory.
- To run the experiment, you can run the following command:
```
bash VaryingJVMSize.sh
```
## Evaluation of Scheduling Decision (Fig.15 ~ Fig.17)
- To evaluate the scheduling decision of MorphStream, we provide a script to run the experiment in /SchedulingDecisions
- The script will run the experiment and generate the result in the result directory.
- Impact of Scheduling Exploration Strategies (Fig.15)
```
bash ExplorationStrategies.sh
```
- Impact of Scheduling Granularities (Fig.16)
```
bash SchedulingGranularities.sh
```
- Impact of Abort Handling Mechanisms (Fig.17)
```
bash AbortHandling.sh
```
## Evaluation of Modern Hardware (Fig.18)
- To evaluate the performance of MorphStream on modern hardware, we provide a script to run the experiment in /ModernHardware
- Micro-architectural Analysis (Fig.18 a). The script will run the experiment and generate the result in the result directory. We measure the hardware performance counters through Intel Vtune Profiler during the
algorithm execution and compute the top-down metrics. 
```
bash MicroArchitecturalAnalysis.sh.sh
```
- Multicore Scalability (Fig.18 b). The script will run the experiment and generate the result in the result directory.
```
bash MulticoreScalability.sh
```
