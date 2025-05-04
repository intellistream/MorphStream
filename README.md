<meta name="robots" content="noindex">

# RTSFaaS

![Java CI with Maven](https://github.com/intellistream/RTSFaaS/workflows/Java%20CI%20with%20Maven/badge.svg?branch=master)

Function-as-a-Service (FaaS) offers a flexible and scalable model for deploying cloud applications. However, existing FaaS frameworks struggle with transactional stateful workflows, where multiple functions share and update state. Prior approaches typically rely on external datastores, incurring high communication overhead to maintain consistency.
This project aims at building an RDMA-capable transactional stateful FaaS framework that achieves high performance while guaranteeing transactional consistency.
RTSFaaS exploits a lease-based concurrency control protocol to dynamically assign and transfer leases among workers to achieve concurrency control. The major contributions are summarized as follows:
- We design RTSFaaS, an RDMA-capable transactional stateful FaaS framework that significantly improves the performance while guaranteeing strong consistency for transactional stateful serverless workflows
- We propose an affinity-aware lease assignment mechanism that improves the benefit of caching by dynamically assigning data leases to selected workers according to
  the data-function affinity.
- We propose an RDMA-capable dynamic lease transferring mechanism that reduces the cost of locking by serializing concurrent data accesses with one-sided RDMA
  primitives.

We welcome your contributions, if you are interested to contribute to the project, please fork and submit a PR. If you have questions, feel free to log an issue or write an email to me: shuhao_zhang AT hust.edu.cn.


## Project Structrue

- `disni` contains libraries used for RDMA-based networking and communication.
- `morph-clients` contains all experimental workloads implemented based on RTSFaaS's programming APIs.
- `morph-common` contains common utility variable/functions for the entire project.
- `morph-core` contains transactional workflow scheduling modules in RTSFaaS (under active development).
- `morph-web` contains a web interface and runtime services for managing and invoking functions
- `scripts` contains all experiment scripts to reproduce our experiments.

## Hardware and Software Requirements
- We deploy RTSFaaS in a cluster with five physical machines, each equipped with Intel Xeon Gold 6230 processors and 128 GB DDR-4 memory. These machines are interconnected
via Mellanox ConnectX-3 40/56 GbE network controllers.
- The driver and each worker operate in individual Docker containers, each configured with 8 CPUs and 32 GB memory. Each worker reserves 2 GB
  memory used for the local data cache.
- We use [TiKV](https://tikv.org/), a highly scalable, low-latency, and easy-to-use key-value database for database management.

## Environment setup
### Download and install dependencies
``` 
git clone -b FaaS https://github.com/intellistream/MorphStream.git 
```
### Build Docker images
```
bash scripts/FaaS/build.sh
```
### Cluster setup
1. **Configure RTSFaaS Cluster**: 
   ```
   modify the cluster configuration in scripts/FaaS/Cluster.env
   ```
2. **Configure Database**: 
   ```
   modify the database configuration in scripts/FaaS/Database.env and tikv-cluster.yaml
   ```
3. **Configure Workload (such as MediaReview)**: 
   ```
   modify the workload configuration in scripts/FaaS/MediaReview.env
   ```

## Evaluation
### Start the TiKV cluster
Follow the official [TiUP Cluster documentation](https://docs.pingcap.com/tidb/stable/tiup-cluster/) to start the TiKV cluster.
### Start the RTSFaaS cluster
Follow the scripts in `scripts/FaaS/run-application` to start the RTSFaaS cluster.

