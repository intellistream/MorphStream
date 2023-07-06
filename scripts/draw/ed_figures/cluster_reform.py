import csv

index = 15
# Define input and output file paths
input_file = "/Users/zhonghao/Downloads/ED_Dataset_unique/%d_32000_5_20_10_0_1_false_0_80_100.events" % index
output_file = "/Users/zhonghao/Downloads/ED_Dataset_unique/filtered_events_%d.csv" % index

time_window = 400

# Function to calculate the window index for a given timestamp
def get_window_index(timestamp):
    return int(timestamp // time_window)

# Read the input file
with open(input_file, "r") as f:
    reader = csv.reader(f)
    lines = list(reader)

# Create a dictionary to store the selected lines within each window
window_lines = {}

# Iterate over the lines and select the lines with the largest rate within each window
for line in lines:
    cluster_id = line[3]
    timestamp = float(line[1])
    rate = float(line[4])
    window_index = get_window_index(timestamp)

    if window_index not in window_lines:
        window_lines[window_index] = {}

    if cluster_id not in window_lines[window_index]:
        window_lines[window_index][cluster_id] = line
    else:
        current_rate = float(window_lines[window_index][cluster_id][4])
        if rate > current_rate:
            window_lines[window_index][cluster_id] = line

# Write the selected lines to the output file
with open(output_file, "w", newline="") as f:
    writer = csv.writer(f)
    for window_index in window_lines.values():
        for line in window_index.values():
            writer.writerow(line)

