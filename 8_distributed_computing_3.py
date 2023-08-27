# Parallel Word Count:

# Distribute the task of counting words in multiple text files across multiple processes using
# the concurrent.futures module

import concurrent.futures

def count_words(filename):
    with open(filename, 'r') as file:
        text = file.read()
        words = text.split()
        return len(words)

if __name__ == "__main__":
    file_names = ["sample_text.txt"]

    with concurrent.futures.ProcessPoolExecutor() as executor:
        results = executor.map(count_words, file_names)

    for file_name, word_count in zip(file_names, results):
        print(f"File: {file_name}, Word Count: {word_count}")


