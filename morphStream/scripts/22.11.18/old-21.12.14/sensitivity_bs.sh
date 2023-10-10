for tt in 48
do
    for scheduler in BL NB_BL RR_v2 NB_RR_v2 SW NB_SW S_NB_SW_v5
    do
        for events in 1024 2048 4096 8192 16384 32768 65536 131072 262144 524288 1048576
        do
            java -Xms60g -Xmx60g -jar -d64 application-0.0.1-jar-with-dependencies.jar -tt $tt --totalEventsPerBatch $events --numberOfBatches 1 --fanoutDist zipfcenter --idGenType hgaussian --scheduler $scheduler --rootFilePath ./data_$1/
        done
    done
done
