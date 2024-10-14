#!/bin/bash
scp -r Env nvmtrans@node8:/home/nvmtrans/jjzhao@node26/FaaS/
ssh nvmtrans@node8 bash /home/nvmtrans/jjzhao@node26/updateFaaSToCluster.sh