<meta name="robots" content="noindex">

# MorphStream

![Java CI with Maven](https://github.com/intellistream/MorphStream/workflows/Java%20CI%20with%20Maven/badge.svg?branch=master)

- This project aims at building a scalable transactional stream processing engine on modern hardware. It allows ACID
  transactions to be run directly on streaming data. It shares similar project vision with
  Flink [StreamingLedger](https://www.ververica.com/hubfs/Ververica/Docs/%5B2018-08%5D-dA-Streaming-Ledger-whitepaper.pdf)
  from Data Artisans , but MorphStream emphsizes more on improving system performance leveraging modern multicore
  processors.
- MorphStream is built based on our previous work of TStream (ICDE'20) but with significant changes: the codebase are
  exclusive.
- The code is still under active development and more features will be introduced. We are also actively maintaining the
  project [wiki](https://github.com/intellistream/MorphStream/wiki). Please checkout it for more detailed desciptions.
- We welcome your contributions, if you are interested to contribute to the project, please fork and submit a PR. If you
  have questions, feel free to log an issue or write an email to me: shuhao_zhang AT sutd.edu.sg

## How to Cite MorphStream

If you use MorphStream in your paper, please cite our work.

* **[ICDE]** Shuhao Zhang, Bingsheng He, Daniel Dahlmeier, Amelie Chi Zhou, Thomas Heinze. Revisiting the design of data
  stream processing systems on multi-core processors, ICDE, 2017 (
  code: https://github.com/ShuhaoZhangTony/ProfilingStudy)
* **[SIGMOD]** Shuhao Zhang, Jiong He, Chi Zhou (Amelie), Bingsheng He. BriskStream: Scaling Stream Processing on
  Multicore Architectures, SIGMOD, 2019 (code: https://github.com/Xtra-Computing/briskstream)
* **[ICDE]** Shuhao Zhang, Yingjun Wu, Feng Zhang, Bingsheng He. Towards Concurrent Stateful Stream Processing on
  Multicore Processors, ICDE, 2020
* **[SIGMOD]** Yancan Mao*, Jianjun Zhao*, Shuhao Zhang, and Haikun Liu, Volker Markl. MorphStream: Adaptive Scheduling
  for Scalable Transactional Stream Processing on Multicores, SIGMOD, 2023 (To Appear). *:equal contribution

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
@inproceedings{10.1145/3299869.3300067,
	title        = {BriskStream: Scaling Data Stream Processing on Shared-Memory Multicore Architectures},
	author       = {Zhang, Shuhao and He, Jiong and Zhou, Amelie Chi and He, Bingsheng},
	year         = 2019,
	booktitle    = {Proceedings of the 2019 International Conference on Management of Data},
	location     = {Amsterdam, Netherlands},
	publisher    = {Association for Computing Machinery},
	address      = {New York, NY, USA},
	series       = {SIGMOD '19},
	pages        = {705–722},
	doi          = {10.1145/3299869.3300067},
	isbn         = 9781450356435,
	url          = {https://doi.org/10.1145/3299869.3300067},
	numpages     = 18,
	keywords     = {operator replication and placement, numa-awareness}
}
@inproceedings{7930015,
	title        = {Revisiting the Design of Data Stream Processing Systems on Multi-Core Processors},
	author       = {Zhang, Shuhao and He, Bingsheng and Dahlmeier, Daniel and Zhou, Amelie Chi and Heinze, Thomas},
	year         = 2017,
	booktitle    = {2017 IEEE 33rd International Conference on Data Engineering (ICDE)},
	volume       = {},
	number       = {},
	pages        = {659--670},
	doi          = {10.1109/ICDE.2017.119}
}
```
