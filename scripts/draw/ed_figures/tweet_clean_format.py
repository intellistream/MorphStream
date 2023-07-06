import csv
import re

# Code 1: Clean special characters

def clean_special_characters(text):
    cleaned_text = re.sub(r'[^\w\s]', '', text)
    return cleaned_text

def clean_csv_file(input_file):
    with open(input_file, 'r') as csv_file:
        reader = csv.reader(csv_file)
        lines = list(reader)

    cleaned_lines = []
    for line in lines:
        if len(line) >= 3:
            line[2] = clean_special_characters(line[2])
        cleaned_lines.append(line)

    return cleaned_lines

# Code 2: Trim sentence spaces

def trim_sentence_spaces(lines):
    for row in lines:
        sentence = row[2]
        trimmed_sentence = ' '.join(sentence.split())
        row[2] = trimmed_sentence

    return lines

# Code 3: Remove numbers and colons

def remove_numbers_and_colons(lines):
    modified_lines = []
    for row in lines:
        if len(row) >= 3:
            third_column = row[2]
            modified_third_column = ''.join([char for char in third_column if not (char.isdigit() or char == ':')])
            row[2] = modified_third_column

            if modified_third_column.strip():
                modified_lines.append(row)

    return modified_lines

# Code 4: Remove repeating words

def remove_repeating_words(lines):
    updated_lines = []
    for row in lines:
        sentence = row[2]
        words = sentence.split()
        unique_words = list(set(words))
        updated_sentence = ' '.join(unique_words)
        row[2] = updated_sentence
        updated_lines.append(row)

    return updated_lines

# Specify file paths
input_file = '/Users/zhonghao/Downloads/ED_Dataset_unique/dataset_origin.csv'
final_output_file = '/Users/zhonghao/Downloads/ED_Dataset_unique/dataset.csv'

# Execute the code snippets in sequence
lines = clean_csv_file(input_file)
lines = trim_sentence_spaces(lines)
lines = remove_numbers_and_colons(lines)
lines = remove_repeating_words(lines)

# Write the final output directly to the file
with open(final_output_file, 'w', newline='') as file:
    writer = csv.writer(file)
    writer.writerows(lines)

print("Processing complete. Final output saved to", final_output_file)
