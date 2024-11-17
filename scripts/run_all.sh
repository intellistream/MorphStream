#!/bin/bash

# Define the main project directory and JAR file path
project_Dir="/home/myc/workspace/myc/MorphStream_Reproduce"
jar_Dir="${project_Dir}/application/target/application-0.0.2-jar-with-dependencies.jar"

# Clear cached synthetic data colocated with /result/data
echo "Clearing cached data..."
rm -rf $project_Dir/result/data

# Modify global.sh in the same folder
echo "Updating global.sh with project_Dir and jar_Dir..."
global_file="$project_Dir/scripts/global.sh" # Assuming it's in the same directory as run_all.sh

if [[ -f $global_file ]]; then
    # Use sed to update or append the export statements
    sed -i "s|^export project_Dir=.*|export project_Dir=\"$project_Dir\"|" "$global_file"
    sed -i "s|^export jar_Dir=.*|export jar_Dir=\"$jar_Dir\"|" "$global_file"

    # If the variables are not present, append them
    grep -q "^export project_Dir=" "$global_file" || echo "export project_Dir=\"$project_Dir\"" >> "$global_file"
    grep -q "^export jar_Dir=" "$global_file" || echo "export jar_Dir=\"$jar_Dir\"" >> "$global_file"

    echo "global.sh updated successfully."
else
    echo "global.sh not found. Creating a new one..."
    echo "export project_Dir=\"$project_Dir\"" > "$global_file"
    echo "export jar_Dir=\"$jar_Dir\"" >> "$global_file"
    echo "global.sh created successfully."
fi

# Source the global.sh to ensure the variables are available
source "$global_file"


# Set Java options
export JAVA_OPTS="-Xmx300G -Xms300G -XX:+UseG1GC"

# Compilation and Installation
echo "Compiling MorphStream..."
cd "$project_Dir/scripts"
bash compile.sh

# Function to change directory and run specific experiment scripts
run_experiment() {
    script_dir=$1
    script_file=$2
    experiment_name=$3
    
    echo "Running $experiment_name..."
    cd "$script_dir"
    bash "$script_file"
    echo "$experiment_name completed."
}

# # Comparison with Conventional SPEs (Fig.10)
# run_experiment "$project_Dir/scripts/PerformanceEvaluation/PerformanceComparison" "PerformanceComparison.sh" "Performance Comparison with Conventional SPEs"

# # Dynamic Workloads Evaluation (Fig.11)
# run_experiment "$project_Dir/scripts/PerformanceEvaluation" "DynamicWorkload.sh" "Dynamic Workload Evaluation"

# Scheduling Strategies Evaluation (Fig.12)
run_experiment "$project_Dir/scripts/PerformanceEvaluation" "MultipleSchedulingStrategies.sh" "Multiple Scheduling Strategies Evaluation"

# # System Overhead Evaluation (Fig.13)
# run_experiment "$project_Dir/scripts/Overhead" "SystemOverhead.sh" "System Overhead Evaluation"

# # GC Overhead Evaluation (Fig.14)
# run_experiment "$project_Dir/scripts/Overhead" "VaryingJVMSize.sh" "GC Overhead Evaluation"

# Scheduling Decisions Evaluation (Fig.15 ~ Fig.17)
# run_experiment "$project_Dir/scripts/SchedulingDecisions" "ExplorationStrategies.sh" "Impact of Scheduling Exploration Strategies"
# run_experiment "$project_Dir/scripts/SchedulingDecisions" "SchedulingGranularities.sh" "Impact of Scheduling Granularities"
run_experiment "$project_Dir/scripts/SchedulingDecisions" "AbortHandling.sh" "Impact of Abort Handling Mechanisms"

# # Modern Hardware Evaluation (Fig.18)
# echo "Running Micro-architectural Analysis on Modern Hardware (Fig.18a)..."
# cd "$project_Dir/scripts/ModernHardware/MicroArchitecturalAnalysis"
# bash vtune_uarch_profiling.sh
# echo "Micro-architectural Analysis completed."

# run_experiment "$project_Dir/scripts/ModernHardware" "MulticoreScalability.sh" "Multicore Scalability Evaluation (Fig.18b)"

echo "All experiments completed."
