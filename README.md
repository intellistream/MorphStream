# MorphStream

![Java CI with Maven](https://github.com/ShuhaoZhangTony/TStream/workflows/Java%20CI%20with%20Maven/badge.svg?branch=master)

This project aims at building scalable transactional stream processing engine on modern hardware.

### Prerequisite

1. System Specification

   | Component          | Description                                                |
   | ------------------ | ---------------------------------------------------------- |
   | Processor (w/o HT) | Intel(R) Xeon(R) Gold 6248R CPU, 2 (socket) * 24 * 3.00GHz |
   | L3 cache size      | 35.75MB                                                    |
   | Memory             | 384GB                                                      |
   | OS & Compiler      | Linux 4.15.0-118-generic                                   |

2. JDK version 1.11.0

### Reproduce all the experiment results

All of our experiments including rawdata and figures can be automatically reproduced by using the following scripts:

```shell
bash run_all.sh
```

You can also run each of the following command to get the rawdata and figures for each individual experiments:

1. Overall system sensitivity study

```shell
bash overview_all.sh
```

2. Workload sensitivity study

```shell
bash sensitivity_study.sh
```

3. Model decision study

```shell
bash model_study.sh
```

