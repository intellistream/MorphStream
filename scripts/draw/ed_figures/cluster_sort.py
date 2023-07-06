import csv

input_file = "/Users/zhonghao/Downloads/ED_Dataset_unique/filtered_events.csv"
output_file = "/Users/zhonghao/Downloads/ED_Dataset_unique/filtered_events_sort.csv"

lines = []
with open(input_file, 'r') as file:
    reader = csv.reader(file)
    for line in reader:
        lines.append(line)

sorted_lines = sorted(lines, key=lambda x: (float(x[-1]), float(x[1])))

with open(output_file, 'w', newline='') as file:
    writer = csv.writer(file)
    writer.writerows(sorted_lines)

print("Lines sorted and written to", output_file)
