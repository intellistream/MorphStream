import csv

# map dataset to indexes.

word_map = {}

def create_word_map(csv_file):
    global word_map
    with open(csv_file, 'r') as file:
        csv_reader = csv.reader(file)
        for line in csv_reader:
            sentence = line[2]
            words = sentence.split()
            for word in words:
                if word not in word_map:
                    # Assign unique index to new word
                    index = len(word_map) + 1
                    word_map[word] = index

def write_word_map_txt(txt_file):
    with open(txt_file, 'w') as file:
        for word, index in word_map.items():
            file.write(f"{word}:{index}\n")

# Usage example
csv_file = '/Users/zhonghao/Downloads/ED_Dataset_unique/dataset.csv'
txt_file = '/Users/zhonghao/Downloads/ED_Dataset_unique/wordmap.txt'

create_word_map(csv_file)
write_word_map_txt(txt_file)
