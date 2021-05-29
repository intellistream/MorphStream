for dd in 1024 512 256 128 64 32 16 8
do
    for scheduler in BL NB_BL RR_v2 NB_RR_v2 SW NB_SW S_NB_SW_v5
    do
        for events in 1048576
        do
            java -Xms60g -Xmx60g -jar -d64 application-0.0.1-jar-with-dependencies.jar --numberOfDLevels $dd -tt 48 --totalEventsPerBatch $events --numberOfBatches 1 --fanoutDist zipfcenter --idGenType hgaussian --scheduler $scheduler --rootFilePath ./data_$1/
        done
    done
done
