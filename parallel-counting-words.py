import numpy as np
import multiprocessing as mp
import os 
import time

#Counting word function in a file
def count_words_in_file(file_path):
    try:
        with open(file_path, "r", encoding="utf-8") as f:
            text = f.read()
            return len(text.split())
    except Exception as e:
        return f"Error: {e}"
    

#Counting word function in group of files
def count_words_in_files(file_list, result_queue):
    word_count = {}
    for file in file_list:
        word_count[file] = count_words_in_file(file)
    result_queue.put(word_count)

#Sequential algorithm
def sequential_word_count(folder):
    file_list = [os.path.join(folder, f) for f in os.listdir(folder) if f.endswith(".txt")]
    word_counts = {}
    for file in file_list:
        word_counts[file] = count_words_in_file(file)
    return word_counts

#Parallel algorithm
def parallel_word_count(folder, num_workers):
    file_list = [os.path.join(folder, f) for f in os.listdir(folder) if f.endswith(".txt")]

    # <----------------PARTITION STAGE-------------------->
    chunks = [file_list[i::num_workers] for i in range(num_workers)]

    # <----------------COMUNICATION STAGE-------------------->
    result_queue = mp.Queue()
    processes = []

    for chunk in chunks:
        # <----------------MAPPING STAGE-------------------->
        p = mp.Process(target=count_words_in_files, args=(chunk, result_queue))
        processes.append(p)
        p.start()

    total_word_count = {}

    # <----------------AGLOMERATION STAGE-------------------->
    for _ in range(num_workers):
        total_word_count.update(result_queue.get())

    for p in processes:
        p.join()

    return total_word_count


if __name__ == "__main__":
    num_workers = mp.cpu_count()
    folder_path = os.path.join(os.path.dirname(__file__), "texts")

    #Time Sequential
    start_seq = time.time()
    sequential_results  = sequential_word_count(folder_path)
    end_seq = time.time()
    sequential_time = end_seq - start_seq
    
    #Parallel Time
    start_par = time.time()
    parallel_results = parallel_word_count(folder_path, num_workers)
    end_par = time.time()
    parallel_time = end_par - start_par

    total_words_sequential = sum(sequential_results.values())
    total_words_parallel = sum(parallel_results.values())

    #Results
    print("\nExecution time:")
    print(f"Sequential: {sequential_time:.4f} sec")
    print(f"Parallel: {parallel_time:.4f} sec")
    print(f"Speedup: {sequential_time / parallel_time:.2f}")
    
    print(f"Total words (sequential): {total_words_sequential}")
    print(f"Total words (parallel): {total_words_parallel}")