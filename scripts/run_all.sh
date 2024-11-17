#!/bin/bash

# Define the main project directory and JAR file path
project_Dir="/home/myc/workspace/myc/MorphStream_Reproduce"
jar_Dir="${project_Dir}/application/target/application-0.0.2-jar-with-dependencies.jar"

# Flags for experiments (set to "y" to enable, "n" to skip)
COMPILE_PROJECT="y"
RUN_COMPARISON="y"
RUN_DYNAMIC_WORKLOAD="y"
RUN_SCHEDULING_STRATEGIES="y"
RUN_SYSTEM_OVERHEAD="y"
RUN_GC_OVERHEAD="y"
RUN_SCHEDULING_DECISIONS="y"
RUN_HARDWARE_EVALUATION="y"

# Clear cached synthetic data colocated with /result/data
echo "Clearing cached data..."
rm -rf "$project_Dir/result/data"

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
if [[ $COMPILE_PROJECT == "y" ]]; then
    echo "Compiling MorphStream..."
    cd "$project_Dir/scripts" || exit
    bash compile.sh
else
    echo "Skipping compilation of MorphStream."
fi

# Function to change directory and run specific experiment scripts
run_experiment() {
    script_dir=$1
    script_file=$2
    experiment_name=$3
    
    echo "Running $experiment_name..."
    cd "$script_dir" || exit
    bash "$script_file"
    echo "$experiment_name completed."
}

# Experiment Sections with Toggle Flags

# Comparison with Conventional SPEs (Fig.10)
if [[ $RUN_COMPARISON == "y" ]]; then
    run_experiment "$project_Dir/scripts/PerformanceEvaluation/PerformanceComparison" "PerformanceComparison.sh" "Performance Comparison with Conventional SPEs"
else
    echo "Skipping Performance Comparison with Conventional SPEs (Fig.10)"
fi

# Dynamic Workloads Evaluation (Fig.11)
if [[ $RUN_DYNAMIC_WORKLOAD == "y" ]]; then
    run_experiment "$project_Dir/scripts/PerformanceEvaluation" "DynamicWorkload.sh" "Dynamic Workload Evaluation"
else
    echo "Skipping Dynamic Workload Evaluation (Fig.11)"
fi

# Scheduling Strategies Evaluation (Fig.12)
if [[ $RUN_SCHEDULING_STRATEGIES == "y" ]]; then
    run_experiment "$project_Dir/scripts/PerformanceEvaluation" "MultipleSchedulingStrategies.sh" "Multiple Scheduling Strategies Evaluation"
else
    echo "Skipping Multiple Scheduling Strategies Evaluation (Fig.12)"
fi

# System Overhead Evaluation (Fig.13)
if [[ $RUN_SYSTEM_OVERHEAD == "y" ]]; then
    run_experiment "$project_Dir/scripts/Overhead" "SystemOverhead.sh" "System Overhead Evaluation"
else
    echo "Skipping System Overhead Evaluation (Fig.13)"
fi

# GC Overhead Evaluation (Fig.14)
if [[ $RUN_GC_OVERHEAD == "y" ]]; then
    run_experiment "$project_Dir/scripts/Overhead" "VaryingJVMSize.sh" "GC Overhead Evaluation"
else
    echo "Skipping GC Overhead Evaluation (Fig.14)"
fi

# Scheduling Decisions Evaluation (Fig.15 ~ Fig.17)
if [[ $RUN_SCHEDULING_DECISIONS == "y" ]]; then
    run_experiment "$project_Dir/scripts/SchedulingDecisions" "ExplorationStrategies.sh" "Impact of Scheduling Exploration Strategies"
    run_experiment "$project_Dir/scripts/SchedulingDecisions" "SchedulingGranularities.sh" "Impact of Scheduling Granularities"
    run_experiment "$project_Dir/scripts/SchedulingDecisions" "AbortHandling.sh" "Impact of Abort Handling Mechanisms"
else
    echo "Skipping Scheduling Decisions Evaluation (Fig.15 ~ Fig.17)"
fi

# Modern Hardware Evaluation (Fig.18)
if [[ $RUN_HARDWARE_EVALUATION == "y" ]]; then
    echo "Running Micro-architectural Analysis on Modern Hardware (Fig.18a)..."
    cd "$project_Dir/scripts/ModernHardware/MicroArchitecturalAnalysis" || exit
    bash vtune_uarch_profiling.sh
    echo "Micro-architectural Analysis completed."

    run_experiment "$project_Dir/scripts/ModernHardware" "MulticoreScalability.sh" "Multicore Scalability Evaluation (Fig.18b)"
else
    echo "Skipping Modern Hardware Evaluation (Fig.18)"
fi

echo "All experiments completed."
