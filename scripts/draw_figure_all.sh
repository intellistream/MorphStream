#!/bin/bash

# Source the global.sh to ensure the variables are available
source global.sh

# Flags to control execution of specific groups
RUN_ABORT_HANDLING="y"
RUN_EXPLORATION_STRATEGIES="y"
RUN_MODERN_HARDWARE="y"
RUN_OVERHEAD="y"
RUN_PERFORMANCE_EVALUATION="y"
RUN_SCHEDULING_GRANULARITIES="y"

# Function to execute Python scripts in a given directory
run_python_scripts() {
    script_dir=$1
    group_name=$2
    echo "====== Running scripts in $group_name ======"
    cd "$script_dir" || exit
    for script in *.py; do
        echo "Running $script..."
        python3 "$script"
        if [[ $? -ne 0 ]]; then
            echo "Error: $script failed."
        else
            echo "$script completed successfully."
        fi
    done
    echo "====== Finished $group_name ======"
    echo ""
}

# Execute scripts based on flags
if [[ $RUN_ABORT_HANDLING == "y" ]]; then
    run_python_scripts "$project_Dir/scripts/draw/AbortHandling" "Abort Handling"
else
    echo "Skipping Abort Handling scripts."
fi

if [[ $RUN_EXPLORATION_STRATEGIES == "y" ]]; then
    run_python_scripts "$project_Dir/scripts/draw/ExplorationStrategies" "Exploration Strategies"
else
    echo "Skipping Exploration Strategies scripts."
fi

if [[ $RUN_MODERN_HARDWARE == "y" ]]; then
    run_python_scripts "$project_Dir/scripts/draw/ModernHardware" "Modern Hardware"
else
    echo "Skipping Modern Hardware scripts."
fi

if [[ $RUN_OVERHEAD == "y" ]]; then
    run_python_scripts "$project_Dir/scripts/draw/Overhead" "Overhead"
else
    echo "Skipping Overhead scripts."
fi

if [[ $RUN_PERFORMANCE_EVALUATION == "y" ]]; then
    run_python_scripts "$project_Dir/scripts/draw/PerformanceEvaluation" "Performance Evaluation"
else
    echo "Skipping Performance Evaluation scripts."
fi

if [[ $RUN_SCHEDULING_GRANULARITIES == "y" ]]; then
    run_python_scripts "$project_Dir/scripts/draw/SchedulingGranularities" "Scheduling Granularities"
else
    echo "Skipping Scheduling Granularities scripts."
fi

echo "All figure scripts completed."
