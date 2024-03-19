import multiprocessing as mp
import re

##### Framework Functions defined below #####
def hadoop_map(data, map_func, num_nodes=mp.cpu_count()):
    """
    Takes in the data, map function and number of cpus to use to 
    mimic multiple nodes, then applies the map in parallel and returns
    a list of key-value mapped values.

    Args:
        data (file): Input data.
        map_func (function): Map function to apply.
        num_nodes (int, optional): Number of cpus to use for parallelisation, defaults to cpu count.

    Returns:
        (list): List of mapped key-value pairs.
    """
    data_list = data.readlines() # convert file to list of lines
    with mp.Pool(processes=num_nodes) as pool: # using multiprocessing.pool to parallelise
        return pool.map(map_func, data_list, chunksize=int(len(data_list)/num_nodes))

def shuffle(mapped_data):
    """
    Performs the shuffle phase on input key-value pairs data.

    Args:
        mapped_data (list): List of mapped key-value pairs.

    Returns:
        (dict_items): Dictionary items of grouped key-value pairs.
    """
    output = {}
    for i in mapped_data:
        if i[0] in output:
            output[i[0]].append(i[1])
        else:
            output[i[0]] = [i[1]]
    return output.items()

def hadoop_reduce(shuffled_data, red_func, num_nodes=mp.cpu_count()):
    """
    Takes in the grouped key-value pairs, reduce function and number of cpus to use to 
    mimic multiple nodes, then applies the reduce function in parallel and returns
    a list of reduced key-value pairs.

    Args:
        shuffled_data (dict_items): Dictionary items of grouped key-value pairs.
        red_func (function): Reduce function to apply.
        num_nodes (int, optional): Number of cpus to use for parallelisation, defaults to cpu count.

    Returns:
        (list): List of reduced key-value pairs.
    """
    with mp.Pool(processes=num_nodes) as pool:# using multiprocessing.pool to parallelise
        return pool.map(red_func, shuffled_data, chunksize=int(len(shuffled_data)/num_nodes))

def map_reduce(data, map_func, red_func, num_nodes=mp.cpu_count()):
    """
    Executes the full MapReduce process.

    Args:
        data (file): Input data.
        map_func (function): Map function to apply.
        red_func (function): Reduce function to apply.
        num_nodes (int, optional): Number of cpus to use for parallelisation, defaults to cpu count.

    Returns:
        (list): List of reduced key-value pairs.
    """
    mapped = hadoop_map(data, map_func, num_nodes)
    shuffled = shuffle(mapped)
    return hadoop_reduce(shuffled, red_func, num_nodes)

##### Map and reduce Functions defined below #####

def map_func(line):
        cols = line.split(',') # Splits the features of the csv
        if re.match(r'^[A-Z]{3}\d{4}[A-Z]{2}\d$', cols[0]): # Ensures passenger id is of correct format
            return (cols[0], 1) # Only returns a value if match else returns None
        
def reduce_func(pair):
        return pair[0], sum(pair[1]) # Aggregates count of keys

if __name__ == '__main__':
    passengers = open('AComp_Passenger_data_no_error.csv', "r", encoding="utf8")

    result = map_reduce(passengers, map_func, reduce_func) # Obtains list of key-value pairs of individual passenger id's and their count
    max_val = max(result, key=lambda x: x[1])[1] # Obtains the max value of occurences
    print([item for item in result if item[1] == max_val]) # Prints a list of all passenger-occurence pair with highest number of flights


