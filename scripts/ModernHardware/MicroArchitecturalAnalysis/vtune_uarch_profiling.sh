#!/bin/bash
source ../../global.sh || exit

# Remove profiling logs
rm -rf r00*

# bash topdown_morphstream.sh &
# sleep 45
# /opt/intel/oneapi/vtune/latest/bin64/vtune -collect uarch-exploration -target-pid $!
# /opt/intel/oneapi/vtune/latest/bin64/vtune -R summary -report-output topdown_morphstream.csv -format csv -csv-delimiter tab

# for profiling in topdown_tstream topdown_sstore # topdown_tstream topdown_sstore
#   do
# bash $profiling.sh &
# sleep 30
# /opt/intel/oneapi/vtune/latest/bin64/vtune -collect uarch-exploration -target-pid $!
# /opt/intel/oneapi/vtune/latest/bin64/vtune -R summary -report-output $profiling.csv -format csv -csv-delimiter tab
# done

mv *.csv $project_Dir/result/data/MicroArchitecturalAnalysis