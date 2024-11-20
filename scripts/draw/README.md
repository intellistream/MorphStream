## Draw Figure

In our paper, we used a tool [prism](https://www.graphpad.com/features) to draw all figures using the experiment raw data stored in `${project_Dir}/result/data`.

To simplify the verification of experimental results, we also created Python scripts to help draw all figures using `matplotlib`, as stated in `scripts/README.md`, you can simply use `draw_figure_all.sh` in the last folder to draw all desirable figures. 

## Experiment Data Folder Layout

We show an example layout of our `${project_Dir}/result/data` folder. The following examples shows the expected data output after running `DynamicWorkload.sh` for experiment of Figure 11 in our paper. 
- The `DynamicWorkload/` is the folder name of the experiment.
- The `inputs/` contains events specifications
  - The `HASHCODE/events.txt` contains all events.
  - The `HASHCODE.txt` contains stats of `events.txt`.
- The `stats/` is the folder contains the actual experiment raw data.
  - `StreamLedger/` is the application/workload name used for running experiment. There are multiple subfolders to differentiate the workload characteristics settings, e.g., `threads` and `totalEvents`.
    - `OG_BFS_A` is the default scheduling decision name of MorphStream, note that it may change the scheduling decisions during runtime.
    - `PAT` contains experiment results of S-Store.
    - `TStream` contains experiment results of TStream.

We draw figures by extracting useful data from the necessary files and use `prism` to draw figures.

```agsl
.
└── DynamicWorkload
    ├── inputs
    │   ├── 467EFF6CDB12CC6AA1963378E482D3908F7FB15C832AECC69561E147B8FE2765
    │   │   └── events.txt
    │   └── 467EFF6CDB12CC6AA1963378E482D3908F7FB15C832AECC69561E147B8FE2765_3194880.txt
    └── stats
        └── StreamLedger
            ├── OG_BFS_A
            │   └── threads = 24
            │       └── totalEvents = 3194880
            │           ├── 491520_100_0_10_0_1_true_8000
            │           ├── 491520_100_0_10_0_1_true_8000.latency
            │           └── 491520_100_0_10_0_1_true_8000.txt
            ├── PAT
            │   └── threads = 24
            │       └── totalEvents = 3194880
            │           ├── 491520_100_0_10_0_1_false_8000
            │           ├── 491520_100_0_10_0_1_false_8000.latency
            │           └── 491520_100_0_10_0_1_false_8000.txt
            └── TStream
                └── threads = 24
                    └── totalEvents = 3194880
                        ├── 491520_100_0_10_0_1_false_8000
                        ├── 491520_100_0_10_0_1_false_8000.latency
                        └── 491520_100_0_10_0_1_false_8000.txt
```

- Inside each folder, there are actual raw data stored.
  - `491520_100_0_10_0_1_true_8000` stores the statistics of runtime, e.g., avg throughput, time breakdown during execution, and detailed throughput per each phase, etc.
  - `491520_100_0_10_0_1_true_8000.latency` stores the end-to-end latency statistics, especially we also recorded the descriptive statistics at the end of the latency.
  - `491520_100_0_10_0_1_true_8000.txt` records the Used Memory every seconds, this helps analyzing the system overhead of MorphStream.

`491520_100_0_10_0_1_true_8000` Stats Snippet.
```agsl
Throughput: 186.27160619785025
AverageTotalTimeBreakdownReport
thread_id        total_time      stream_process  txn_process     overheads
0       128823.93       18812.01        108992.81       1019.11
1       128839.91       18729.38        109108.19       1002.34
2       128839.37       18281.37        109376.45       1181.55
...
phase_id         throughput
0       223.9692
1       319.5250
2       394.2096
...
SchedulerTimeBreakdownReport
thread_id        explore_time    next_time       useful_time     notify_time     construct_time  first_explore_time      scheduler_switch
0       60341.50        266.97          47468.85        440.22          13433.28        10768.89        21.08
1       52455.61        303.83          55478.65        504.03          13113.83        11516.22        32.92
2       54884.44        337.32          53457.49        497.98          12902.21        11719.97        36.25
3       53543.43        1094.01         54643.59        501.00          13109.59        11798.40        32.74
4       73354.22        6598.09         34590.27        365.23          13029.81        9890.38         35.00
...
```

`491520_100_0_10_0_1_true_8000.latency` Stats Snippet.

```agsl
1053.232784
1039.45748
1039.377715
...

=======Details=======
DescriptiveStatistics:
n: 133120
min: 531.135473
max: 2114.810556
mean: 1213.6329771797334
std dev: 494.4779533285706
median: 1269.610293
skewness: 0.046701885042783414
kurtosis: -1.480585618955568

===99th===
2064.82811184
Percentile       Latency
0.500000        535.7091
20.000000       642.2533
40.000000       959.9110
60.000000       1418.9719
80.000000       1743.9447
99      2064.8281
```

`491520_100_0_10_0_1_true_8000.txt` Stats Snippet.

```agsl
UsedMemory
0.000000        0.0000
1.000000        0.0000
...
11.000000       1.0000
12.000000       1.0000
...
194.000000      113.0000
195.000000      114.0000
196.000000      116.0000
...
```
