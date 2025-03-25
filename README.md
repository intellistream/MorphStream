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
- We welcome your contributions, if you are interested to contribute to the project, please fork and submit a PR. 

## How to Cite MorphStream

If you use MorphStream in your paper, please cite our work.

* **[TKDE]** Jianjun Zhao, Yancan Mao, Zhonghao Yang, Haikun Liu and Shuhao Zhang. Scalable Transactional Stream Processing on Multicores. TKDE, 2025
* **[VLDBJ]** Shuhao Zhang, Soto Juan and Volker Markl. A survey on transactional stream processing. The VLDB Journal, 2024
* **[ICDE]** Jianjun Zhao, Haikun Liu, Shuhao Zhang, Zhuohui Duan, Xiaofei Liao, Hai Jin, Yu Zhang. Fast Parallel Recovery for Transactional Stream Processing on Multicores. ICDE, 2024
* **[ICDE]** Siqi Xiang, Zhonghao Yang, Shuhao Zhang, Jianjun Zhao, Yancan Mao. MorphStream: Scalable Processing of Transactions over Streams. ICDE (Demo), 2024
* **[SIGMOD]** Yancan Mao, Jianjun Zhao, Shuhao Zhang, Haikun Liu and Volker Markl. MorphStream: Adaptive
  Scheduling for Scalable Transactional Stream Processing on Multicores. SIGMOD, 2023
* **[ICDE]** Shuhao Zhang, Yingjun Wu, Feng Zhang, Bingsheng He. Towards Concurrent Stateful Stream Processing on
  Multicore Processors, ICDE, 2020
* **[SIGMOD]** Shuhao Zhang, Jiong He, Chi Zhou (Amelie), Bingsheng He. BriskStream: Scaling Stream Processing on
  Multicore Architectures. SIGMOD, 2019 (code: https://github.com/Xtra-Computing/briskstream)
* **[ICDE]** Shuhao Zhang, Bingsheng He, Daniel Dahlmeier, Amelie Chi Zhou, Thomas Heinze. Revisiting the design of data
  stream processing systems on multi-core processors. ICDE, 2017 (code: https://github.com/ShuhaoZhangTony/ProfilingStudy)
```
@article{zhang2024survey,
  title={A survey on transactional stream processing},
  author={Zhang, Shuhao and Soto, Juan and Markl, Volker},
  journal={The VLDB Journal},
  volume={33},
  number={2},
  pages={451--479},
  year={2024},
  publisher={Springer}
}
@inproceedings{zhao2024fast,
  title={Fast Parallel Recovery for Transactional Stream Processing on Multicores},
  author={Zhao, Jianjun and Liu, Haikun and Zhang, Shuhao and Duan, Zhuohui and Liao, Xiaofei and Jin, Hai and Zhang, Yu},
  booktitle={2024 IEEE 40th International Conference on Data Engineering (ICDE)},
  pages={1478--1491},
  year={2024},
  organization={IEEE}
}
@inproceedings{xiang2024morphstream,
  title={MorphStream: Scalable Processing of Transactions over Streams},
  author={Xiang, Siqi and Yang, Zhonghao and Zhao, Jianjun and Mao, Yancan and Zhang, Shuhao},
  booktitle={2024 IEEE 40th International Conference on Data Engineering (ICDE)},
  pages={5485--5488},
  year={2024},
  organization={IEEE}
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
@inproceedings{zhang2020towards,
	title        = {Towards Concurrent Stateful Stream Processing on Multicore Processors},
	author       = {Zhang, Shuhao and Wu, Yingjun and Zhang, Feng and He, Bingsheng},
	year         = 2020,
	booktitle    = {2020 IEEE 36th International Conference on Data Engineering (ICDE)},
	volume       = {},
	number       = {},
	pages        = {1537--1548},
	doi          = {10.1109/ICDE48307.2020.00136}
}
@inproceedings{zhang2019briskstream,
  title={Briskstream: Scaling data stream processing on shared-memory multicore architectures},
  author={Zhang, Shuhao and He, Jiong and Zhou, Amelie Chi and He, Bingsheng},
  booktitle={Proceedings of the 2019 International Conference on Management of Data},
  pages={705--722},
  year={2019}
}
@inproceedings{zhang2017revisiting,
  title={Revisiting the design of data stream processing systems on multi-core processors},
  author={Zhang, Shuhao and He, Bingsheng and Dahlmeier, Daniel and Zhou, Amelie Chi and Heinze, Thomas},
  booktitle={2017 IEEE 33rd International conference on data engineering (ICDE)},
  pages={659--670},
  year={2017},
  organization={IEEE}
}
```
