<meta name="robots" content="noindex">

# MorphStream

![Java CI with Maven](https://github.com/intellistream/MorphStream/workflows/Java%20CI%20with%20Maven/badge.svg?branch=master)

- This project aims at building a scalable transactional stream processing engine on modern hardware. It allows ACID transactions to be run directly on streaming data. It shares similar project vision with Flink [StreamingLedger](https://www.ververica.com/hubfs/Ververica/Docs/%5B2018-08%5D-dA-Streaming-Ledger-whitepaper.pdf) from Data Artisans , but MorphStream emphsizes more on improving system performance leveraging modern multicore processors.
- MorphStream is built based on our previous work of TStream (ICDE'20) but with significant changes: the codebase are exclusive.
- The code is still under active development and more features will be introduced. We are also actively maintaining the project [wiki](https://github.com/intellistream/MorphStream/wiki). Please checkout it for more detailed desciptions.
- We welcome your contributions, if you are interested to contribute to the project, please fork and submit a PR. If you have questions, feel free to log an issue or write an email to me: shuhao_zhang AT sutd.edu.sg


## Project Structrue

- `affinity` contains libraries used for pinning threads to corresponding CPUs in Multicore architecture.
- `appliication` contains all experimental workloads implemented based on MorphStream's programming APIs.
- `common` contains common utility variable/functions for the entire project.
- `scripts` contain all experiment scripts to reproduce our experiments.
- `state-engine` contains state management modules in MorphStream.
- `stream-engine` contains transactional events scheduling modules in MorphStream.


## How to Reproduce Our Experimental Results?

1. System specification: We run all our experiment in

   | Component         | Details                                                                                            |
      |-------------------|----------------------------------------------------------------------------------------------------|
   | Server            | Dual-socket Intel Xeon Gold 6248R server with 384 GB DRAM                                          |
   | OS                | Ubuntu                                                                                             |
   | Cores per Socket  | 24 cores of 3.00GHz                                                                                |
   | L3 Cache          | 35.75MB                                                                                            |
   | NUMA              | Single socket used to isolate the impact of NUMA                                                   |
   | Core Pinning      | Each thread pinned to one core, using 1 to 24 cores to evaluate scalability                        |
   | OS Kernel         | Linux 4.15.0-118-generic                                                                           |
   | JDK Version       | JDK 1.8.0_301                                                                                      |
   | JVM Configuration | -Xmx and -Xms set to 300 GB                                                                        |
   | Garbage Collector | G1GC, configured to not clear temporal objects such as processed TPGs and multi-versions of states |

2. Third-party Lib: Make sure to install and configure environments all required dependencies as shown below.
    - wget: ```sudo apt install wget -y```
    - [JDK 1.8.x](https://www.oracle.com/sg/java/technologies/javase/javase8-archive-downloads.html)
    - Maven 3.8.6: ```sudo apt install maven -y```
    - [Intel Vtune](https://www.intel.com/content/www/us/en/developer/tools/oneapi/vtune-profiler.html#gs.ffrana): Configure Vtune path to `/opt/intel/oneapi/vtune/latest/bin64/vtune`, set `/proc/sys/kernel/perf_event_paranoid` to 0.

3. Environment Configurations: All environment configurations are extracted to `global.sh`, in fact, users only need to set `project_Dir` before running all our experiments.

4. We categorize all our experiment scripts by 4 subsections in our paper, you can reproduce the results of each subsection by enter the associated folder and run scripts.
   Inside every folder, we can run each experiment easily with the following command. More detailed instructions can be found in `scripts/README.md`.
   ```agsl
   cd $EXPERIMENT_FOLDER_PATH # The experiment must be done in the associated folder.
   bash ${EXPERIMENT_SCRIPT}.sh # e.g., EXPERIMENT_SCRIPT=PerformanceComparison
   ```


## Results

All raw data results can be found in `${project_Dir}/result/data/`.

We demonstrate how to draw all figures in our experiments in `scripts/draw/README.md`.


## How to Cite MorphStream

If you use MorphStream in your paper, please cite our work.

* **[ICDE]** Shuhao Zhang, Bingsheng He, Daniel Dahlmeier, Amelie Chi Zhou, Thomas Heinze. Revisiting the design of data stream processing systems on multi-core processors, ICDE, 2017 (code: https://github.com/ShuhaoZhangTony/ProfilingStudy)
* **[SIGMOD]** Shuhao Zhang, Jiong He, Chi Zhou (Amelie), Bingsheng He. BriskStream: Scaling Stream Processing on Multicore Architectures, SIGMOD, 2019 (code: https://github.com/Xtra-Computing/briskstream)
* **[ICDE]** Shuhao Zhang, Yingjun Wu, Feng Zhang, Bingsheng He. Towards Concurrent Stateful Stream Processing on Multicore Processors, ICDE, 2020
* **[SIGMOD]** Yancan Mao and Jianjun Zhao and Shuhao Zhang and Haikun Liu and Volker Markl. MorphStream: Adaptive Scheduling for Scalable Transactional Stream Processing on Multicores, SIGMOD, 2023
* We are working on another two follow-up works on MorphStream. Stay tuned.
```
@inproceedings{9101749,
	title        = {Towards Concurrent Stateful Stream Processing on Multicore Processors},
	author       = {Zhang, Shuhao and Wu, Yingjun and Zhang, Feng and He, Bingsheng},
	year         = 2020,
	booktitle    = {2020 IEEE 36th International Conference on Data Engineering (ICDE)},
	volume       = {},
	number       = {},
	pages        = {1537--1548},
	doi          = {10.1109/ICDE48307.2020.00136}
}
@inproceedings{mao2023morphstream,
	title        = {MorphStream: Adaptive Scheduling for Scalable Transactional Stream Processing on Multicores},
	author       = {Yancan Mao and Jianjun Zhao and Shuhao Zhang and Haikun Liu and Volker Markl},
	year         = 2023,
	booktitle    = {Proceedings of the 2023 International Conference on Management of Data (SIGMOD)},
	location     = {Seattle, WA, USA},
	publisher    = {Association for Computing Machinery},
	address      = {New York, NY, USA},
	series       = {SIGMOD '23},
	abbr         = {SIGMOD},
	bibtex_show  = {true},
	selected     = {true},
	pdf          = {papers/MorphStream.pdf},
	code         = {https://github.com/intellistream/MorphStream},
	tag          = {full paper}
}
```
