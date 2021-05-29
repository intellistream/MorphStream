# mkdir /data/1/hadoop/aqif/thesisdeliverables/data/data_$1/
# cp /home/hadoop/sesame/data_$1/stats/ /data/1/hadoop/aqif/thesisdeliverables/data/data_$1/
# 
for folder in 1 2 3 4
do
    mkdir /home/hadoop/sesame/thesisdata_v7/$1/data_$folder/
    cp -r /data/1/hadoop/aqif/thesisdeliverables/data_$folder/stats/. /home/hadoop/sesame/thesisdata_v7/$1/data_$folder/
done