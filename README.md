# MorphStream

![Java CI with Maven](https://github.com/ShuhaoZhangTony/TStream/workflows/Java%20CI%20with%20Maven/badge.svg?branch=master)

- This project aims at building a scalable transactional stream processing engine on modern hardware. It allows ACID transactions to be run directly on streaming data. It shares similar project vision with Flink [StreamingLedger](https://www.ververica.com/hubfs/Ververica/Docs/%5B2018-08%5D-dA-Streaming-Ledger-whitepaper.pdf) from Data Artisans , but MorphStream emphsizes more on improving system performance leveraging modern multicore processors.
- MorphStream is built based on our previous work of TStream (ICDE'20) but with significant changes: the codebase are exclusive. 
- The code is still under active development and more features will be introduced. We are also actively maintaining the project [wiki](https://github.com/intellistream/MorphStream/wiki). Please checkout it for more detailed desciptions.
- We welcome your contributions, if you are interested to contribute to the project, please fork and submit a PR. If you have questions, feel free to log an issue or write an email to me: shuhao_zhang AT sutd.edu.sg

## How to Cite MorphStream

If you use MorphStream in your paper, please cite our work.

* **[ICDE]** Shuhao Zhang, Bingsheng He, Daniel Dahlmeier, Amelie Chi Zhou, Thomas Heinze. Revisiting the design of data stream processing systems on multi-core processors, ICDE, 2017 (code: https://github.com/ShuhaoZhangTony/ProfilingStudy)
* **[SIGMOD]** Shuhao Zhang, Jiong He, Chi Zhou (Amelie), Bingsheng He. BriskStream: Scaling Stream Processing on Multicore Architectures, SIGMOD, 2019 (code: https://github.com/Xtra-Computing/briskstream)
* **[ICDE]** Shuhao Zhang, Yingjun Wu, Feng Zhang, Bingsheng He. Towards Concurrent Stateful Stream Processing on Multicore Processors, ICDE, 2020
* **[xxx]** We have an anonymized submission under review. Stay tuned.
```
@INPROCEEDINGS{9101749,  
author={Zhang, Shuhao and Wu, Yingjun and Zhang, Feng and He, Bingsheng},  
booktitle={2020 IEEE 36th International Conference on Data Engineering (ICDE)},   
title={Towards Concurrent Stateful Stream Processing on Multicore Processors},   
year={2020},  
volume={},  
number={},  
pages={1537-1548},  
doi={10.1109/ICDE48307.2020.00136}
}

```

