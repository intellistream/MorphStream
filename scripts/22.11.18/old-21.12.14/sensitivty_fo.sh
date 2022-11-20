for folder in 1 2 3 4
do
    for scheduler in BL NB_BL RR_v2 NB_RR_v2 SW NB_SW S_NB_SW_v5
    do
        for events in 1048576
        do
            java -Xms60g -Xmx60g -jar -d64 application-0.0.1-jar-with-dependencies.jar -tt 48 --totalEventsPerBatch $events --numberOfBatches 1 --fanoutDist $1 --idGenType hgaussian --scheduler $scheduler --rootFilePath ./data_$folder/
        done
    done
done
