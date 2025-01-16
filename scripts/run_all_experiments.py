import os
import subprocess
from pathlib import Path


root_dir = Path.cwd()
print(f"Root directory: {root_dir}")


def run_Pre_Study():
    exp_script_path = root_dir / 'morphStream/scripts/TransNFV' / '5.1_PreliminaryStudy.py'
    command = f"python3 {exp_script_path} --root_dir {root_dir} --exp_dir {root_dir / 'morphStream/scripts/TransNFV'}"
    result = subprocess.run(command, shell=True)
    if result.returncode == 0:
        print("Done")
    else:
        print(f"Error: {result.returncode}")

def run_5_2_1_Static():
    exp_script_path = root_dir / 'morphStream/scripts/TransNFV' / '5.2.1_StaticPerformance_BarChart.py'
    command = f"python3 {exp_script_path} --root_dir {root_dir} --exp_dir {root_dir / 'morphStream/scripts/TransNFV'}"
    result = subprocess.run(command, shell=True)
    if result.returncode == 0:
        print("Done")
    else:
        print(f"Error: {result.returncode}")

def run_5_2_2_Dynamic_Variation():
    exp_script_path = root_dir / 'morphStream/scripts/TransNFV' / '5.2.2_DynamicPerformance.py'
    command = f"python3 {exp_script_path} --root_dir {root_dir} --exp_dir {root_dir / 'morphStream/scripts/TransNFV'}"
    result = subprocess.run(command, shell=True)
    if result.returncode == 0:
        print("Done")
    else:
        print(f"Error: {result.returncode}")

def run_5_2_3_FineGrained_Variation():
    exp_script_path = root_dir / 'morphStream/scripts/TransNFV' / '5.2.3_FineGrainedPerformance.py'
    command = f"python3 {exp_script_path} --root_dir {root_dir} --exp_dir {root_dir / 'morphStream/scripts/TransNFV'}"
    result = subprocess.run(command, shell=True)
    if result.returncode == 0:
        print("Done")
    else:
        print(f"Error: {result.returncode}")

def run_5_3_1_Time_Breakdown():
    exp_script_path = root_dir / 'morphStream/scripts/TransNFV' / '5.3.1_TimeBreakdown.py'
    command = f"python3 {exp_script_path} --root_dir {root_dir} --exp_dir {root_dir / 'morphStream/scripts/TransNFV'}"
    result = subprocess.run(command, shell=True)
    if result.returncode == 0:
        print("Done")
    else:
        print(f"Error: {result.returncode}")

def run_5_3_2_Memory_Footprint():
    exp_script_path = root_dir / 'morphStream/scripts/TransNFV' / '5.3.2_MemFootprint.py'
    command = f"python3 {exp_script_path} --root_dir {root_dir} --exp_dir {root_dir / 'morphStream/scripts/TransNFV'}"
    result = subprocess.run(command, shell=True)
    if result.returncode == 0:
        print("Done")
    else:
        print(f"Error: {result.returncode}")

def run_5_2_4_Model_Test():
    exp_script_path = root_dir / 'morphStream/scripts/TransNFV' / '5.5_ModelEvaluation.py'
    command = f"python3 {exp_script_path} --root_dir {root_dir} --exp_dir {root_dir / 'morphStream/scripts/TransNFV'}"
    result = subprocess.run(command, shell=True)
    if result.returncode == 0:
        print("Done")
    else:
        print(f"Error: {result.returncode}")

def run_5_4_1_Tradeoff_DR_KeySkew():
    exp_script_path = root_dir / 'morphStream/scripts/TransNFV' / '5.4.1_Tradeoff_DR_KeySkew.py'
    command = f"python3 {exp_script_path} --root_dir {root_dir} --exp_dir {root_dir / 'morphStream/scripts/TransNFV'}"
    result = subprocess.run(command, shell=True)
    if result.returncode == 0:
        print("Done")
    else:
        print(f"Error: {result.returncode}")

def run_5_4_1_Tradeoff_DR_WorkloadSkew():
    exp_script_path = root_dir / 'morphStream/scripts/TransNFV' / '5.4.1_Tradeoff_DR_WorkloadSkew.py'
    command = f"python3 {exp_script_path} --root_dir {root_dir} --exp_dir {root_dir / 'morphStream/scripts/TransNFV'}"
    result = subprocess.run(command, shell=True)
    if result.returncode == 0:
        print("Done")
    else:
        print(f"Error: {result.returncode}")

def run_5_4_2_Tradeoff_SAC_ReadRatio():
    exp_script_path = root_dir / 'morphStream/scripts/TransNFV' / '5.4.2_Tradeoff_SAC_ReadRatio.py'
    command = f"python3 {exp_script_path} --root_dir {root_dir} --exp_dir {root_dir / 'morphStream/scripts/TransNFV'}"
    result = subprocess.run(command, shell=True)
    if result.returncode == 0:
        print("Done")
    else:
        print(f"Error: {result.returncode}")

def run_5_4_2_Tradeoff_SAC_KeySkew():
    exp_script_path = root_dir / 'morphStream/scripts/TransNFV' / '5.4.2_Tradeoff_SAC_KeySkew.py'
    command = f"python3 {exp_script_path} --root_dir {root_dir} --exp_dir {root_dir / 'morphStream/scripts/TransNFV'}"
    result = subprocess.run(command, shell=True)
    if result.returncode == 0:
        print("Done")
    else:
        print(f"Error: {result.returncode}")

def run_5_4_3_Tradeoff_SSL_Locality():
    exp_script_path = root_dir / 'morphStream/scripts/TransNFV' / '5.4.3_Tradeoff_SSL_Locality.py'
    command = f"python3 {exp_script_path} --root_dir {root_dir} --exp_dir {root_dir / 'morphStream/scripts/TransNFV'}"
    result = subprocess.run(command, shell=True)
    if result.returncode == 0:
        print("Done")
    else:
        print(f"Error: {result.returncode}")

def run_5_4_3_Tradeoff_SSL_ReadRatio():
    exp_script_path = root_dir / 'morphStream/scripts/TransNFV' / '5.4.3_Tradeoff_SSL_ReadRatio.py'
    command = f"python3 {exp_script_path} --root_dir {root_dir} --exp_dir {root_dir / 'morphStream/scripts/TransNFV'}"
    result = subprocess.run(command, shell=True)
    if result.returncode == 0:
        print("Done")
    else:
        print(f"Error: {result.returncode}")

def run_5_4_3_Tradeoff_SSL_ScopeRatio():
    exp_script_path = root_dir / 'morphStream/scripts/TransNFV' / '5.4.3_Tradeoff_SSL_ScopeRatio.py'
    command = f"python3 {exp_script_path} --root_dir {root_dir} --exp_dir {root_dir / 'morphStream/scripts/TransNFV'}"
    result = subprocess.run(command, shell=True)
    if result.returncode == 0:
        print("Done")
    else:
        print(f"Error: {result.returncode}")

def run_5_5_1_System_Scalability():
    exp_script_path = root_dir / 'morphStream/scripts/TransNFV' / '5.5.1_System_Scalability.py'
    command = f"python3 {exp_script_path} --root_dir {root_dir} --exp_dir {root_dir / 'morphStream/scripts/TransNFV'}"
    result = subprocess.run(command, shell=True)
    if result.returncode == 0:
        print("Done")
    else:
        print(f"Error: {result.returncode}")

def run_5_5_2_Monitor_Window():
    exp_script_path = root_dir / 'morphStream/scripts/TransNFV' / '5.5.2_Monitor_Window.py'
    command = f"python3 {exp_script_path} --root_dir {root_dir} --exp_dir {root_dir / 'morphStream/scripts/TransNFV'}"
    result = subprocess.run(command, shell=True)
    if result.returncode == 0:
        print("Done")
    else:
        print(f"Error: {result.returncode}")

def run_6_Training_Data_Generation():
    exp_script_path = root_dir / 'morphStream/scripts/TransNFV' / '6_TrainingDataGeneration.py'
    command = f"python3 {exp_script_path} --root_dir {root_dir} --exp_dir {root_dir / 'morphStream/scripts/TransNFV'}"
    result = subprocess.run(command, shell=True)
    if result.returncode == 0:
        print("Done")
    else:
        print(f"Error: {result.returncode}")

def run_6_Training_Data_Labeling():
    exp_script_path = root_dir / 'morphStream/scripts/TransNFV' / '6_TrainingDataLabeling.py'
    command = f"python3 {exp_script_path} --root_dir {root_dir} --exp_dir {root_dir / 'morphStream/scripts/TransNFV'}"
    result = subprocess.run(command, shell=True)
    if result.returncode == 0:
        print("Done")
    else:
        print(f"Error: {result.returncode}")

def model_training():
    exp_script_path = root_dir / 'morphStream/scripts/TransNFV' / '6_ModelTraining.py'
    command = f"python3 {exp_script_path} --root_dir {root_dir} --exp_dir {root_dir / 'morphStream/scripts/TransNFV'}"
    result = subprocess.run(command, shell=True)
    if result.returncode == 0:
        print("Done")
    else:
        print(f"Error: {result.returncode}")

def model_testing():
    exp_script_path = root_dir / 'morphStream/scripts/TransNFV' / '6_Model_Testing.py'
    command = f"python3 {exp_script_path} --root_dir {root_dir} --exp_dir {root_dir / 'morphStream/scripts/TransNFV'}"
    result = subprocess.run(command, shell=True)
    if result.returncode == 0:
        print("Done")
    else:
        print(f"Error: {result.returncode}")






def generate_Pre_Study():
    exp_script_path = root_dir / 'morphStream/scripts/TransNFV/generators' / '5.1_Generator.py'
    command = f"python3 {exp_script_path} --root_dir {root_dir} --exp_dir {root_dir / 'morphStream/scripts/TransNFV'}"
    result = subprocess.run(command, shell=True)
    if result.returncode == 0:
        print("Done")
    else:
        print(f"Error: {result.returncode}")

def generate_5_2_1():
    exp_script_path = root_dir / 'morphStream/scripts/TransNFV/generators' / '5.2.1_Generator.py'
    command = f"python3 {exp_script_path} --root_dir {root_dir} --exp_dir {root_dir / 'morphStream/scripts/TransNFV'}"
    result = subprocess.run(command, shell=True)
    if result.returncode == 0:
        print("Done")
    else:
        print(f"Error: {result.returncode}")

def generate_5_2_2():
    exp_script_path = root_dir / 'morphStream/scripts/TransNFV/generators' / '5.2.2_Generator.py'
    command = f"python3 {exp_script_path} --root_dir {root_dir} --exp_dir {root_dir / 'morphStream/scripts/TransNFV'}"
    result = subprocess.run(command, shell=True)
    if result.returncode == 0:
        print("Done")
    else:
        print(f"Error: {result.returncode}")

def generate_5_2_3():
    exp_script_path = root_dir / 'morphStream/scripts/TransNFV/generators' / '5.2.3_Generator.py'
    command = f"python3 {exp_script_path} --root_dir {root_dir} --exp_dir {root_dir / 'morphStream/scripts/TransNFV'}"
    result = subprocess.run(command, shell=True)
    if result.returncode == 0:
        print("Done")
    else:
        print(f"Error: {result.returncode}")
def generate_5_3():
    exp_script_path = root_dir / 'morphStream/scripts/TransNFV/generators' / '5.3_Generator.py'
    command = f"python3 {exp_script_path} --root_dir {root_dir} --exp_dir {root_dir / 'morphStream/scripts/TransNFV'}"
    result = subprocess.run(command, shell=True)
    if result.returncode == 0:
        print("Done")
    else:
        print(f"Error: {result.returncode}")

def generate_5_4_1_KeySkew():
    exp_script_path = root_dir / 'morphStream/scripts/TransNFV/generators' / '5.4.1_KeySkew_Generator.py'
    command = f"python3 {exp_script_path} --root_dir {root_dir} --exp_dir {root_dir / 'morphStream/scripts/TransNFV'}"
    result = subprocess.run(command, shell=True)
    if result.returncode == 0:
        print("Done")
    else:
        print(f"Error: {result.returncode}")

def generate_5_4_1_WorkloadSkew():
    exp_script_path = root_dir / 'morphStream/scripts/TransNFV/generators' / '5.4.1_WorkloadSkew_Generator.py'
    command = f"python3 {exp_script_path} --root_dir {root_dir} --exp_dir {root_dir / 'morphStream/scripts/TransNFV'}"
    result = subprocess.run(command, shell=True)
    if result.returncode == 0:
        print("Done")
    else:
        print(f"Error: {result.returncode}")

def generate_5_4_2():
    exp_script_path = root_dir / 'morphStream/scripts/TransNFV/generators' / '5.4.2_Generator.py'
    command = f"python3 {exp_script_path} --root_dir {root_dir} --exp_dir {root_dir / 'morphStream/scripts/TransNFV'}"
    result = subprocess.run(command, shell=True)
    if result.returncode == 0:
        print("Done")
    else:
        print(f"Error: {result.returncode}")

def generate_5_4_3():
    exp_script_path = root_dir / 'morphStream/scripts/TransNFV/generators' / '5.4.3_Generator.py'
    command = f"python3 {exp_script_path} --root_dir {root_dir} --exp_dir {root_dir / 'morphStream/scripts/TransNFV'}"
    result = subprocess.run(command, shell=True)
    if result.returncode == 0:
        print("Done")
    else:
        print(f"Error: {result.returncode}")


def generate_5_5_1():
    exp_script_path = root_dir / 'morphStream/scripts/TransNFV/generators' / '5.5.1_Generator.py'
    command = f"python3 {exp_script_path} --root_dir {root_dir} --exp_dir {root_dir / 'morphStream/scripts/TransNFV'}"
    result = subprocess.run(command, shell=True)
    if result.returncode == 0:
        print("Done")
    else:
        print(f"Error: {result.returncode}")

def generate_5_5_2():
    exp_script_path = root_dir / 'morphStream/scripts/TransNFV/generators' / '5.5.2_Generator.py'
    command = f"python3 {exp_script_path} --root_dir {root_dir} --exp_dir {root_dir / 'morphStream/scripts/TransNFV'}"
    result = subprocess.run(command, shell=True)
    if result.returncode == 0:
        print("Done")
    else:
        print(f"Error: {result.returncode}")

def generate_6_Training():
    exp_script_path = root_dir / 'morphStream/scripts/TransNFV/generators' / '6_Training_Generator.py'
    command = f"python3 {exp_script_path} --root_dir {root_dir} --exp_dir {root_dir / 'morphStream/scripts/TransNFV'}"
    result = subprocess.run(command, shell=True)
    if result.returncode == 0:
        print("Done")
    else:
        print(f"Error: {result.returncode}")

def generate_6_Testing():
    exp_script_path = root_dir / 'morphStream/scripts/TransNFV/generators' / '6_Testing_Generator.py'
    command = f"python3 {exp_script_path} --root_dir {root_dir} --exp_dir {root_dir / 'morphStream/scripts/TransNFV'}"
    result = subprocess.run(command, shell=True)
    if result.returncode == 0:
        print("Done")
    else:
        print(f"Error: {result.returncode}")



if __name__ == "__main__":
    # generate_Pre_Study()
    # run_Pre_Study()

    # generate_5_2_1()
    # run_5_2_1_Static()

    # generate_5_2_2()
    # run_5_2_2_Dynamic_Variation()

    # generate_5_2_3()
    # run_5_2_3_FineGrained_Variation()

    # generate_5_3()
    # run_5_3_1_Time_Breakdown()
    # run_5_3_2_Memory_Footprint()

    # generate_5_4_1_KeySkew()
    # run_5_4_1_Tradeoff_DR_KeySkew()

    # generate_5_4_1_WorkloadSkew()
    # run_5_4_1_Tradeoff_DR_WorkloadSkew()

    # generate_5_4_2()
    # run_5_4_2_Tradeoff_SAC_ReadRatio()
    # run_5_4_2_Tradeoff_SAC_KeySkew()

    # generate_5_4_3()
    # run_5_4_3_Tradeoff_SSL_Locality()
    # run_5_4_3_Tradeoff_SSL_ReadRatio()
    # run_5_4_3_Tradeoff_SSL_ScopeRatio()

    # generate_5_5_1()
    # run_5_5_1_System_Scalability()

    # generate_5_5_2()
    # run_5_5_2_Monitor_Window()

    # generate_6_Training()
    # generate_6_Testing()
    # run_6_Training_Data_Generation()
    # run_6_Training_Data_Labeling()
    # model_training()
    model_testing()

    print("Done")
