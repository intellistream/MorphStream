# TStream

![Java CI with Maven](https://github.com/ShuhaoZhangTony/TStream/workflows/Java%20CI%20with%20Maven/badge.svg?branch=master)

This project aims at building scalable transactional stream processing engine on modern hardware

# Time Table
1. Revert PAT's lock to ReentrantLock. 1/Nov/2021 -- Not Valid Anymore.
2. Revert PAT with sorting under TStream's two-phase execution framework. 3/Nov/2021 -- done.
3. Add Original Busy-wait TStream into our framework and call it TStream. Ours is TStream+. 4/Nov/2021 -- done.
4. Add computation complexity to wrkload. 5/Nov/2021 -- done.
5. Algorithm evaluation under varying workloads. 12/Nov/2021
6. First complete draft. 19/Nov/2021
7. Ready for Volker's review. Improve code quality by addressing minor issues. 22/Nov/2021
8. Make the submission. 1/Dec/2021
