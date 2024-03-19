import multiprocessing as mp
import re
import csv

def hadoop_map(data, map_func, cpu_num=mp.cpu_count()):
    with mp.Pool(processes=cpu_num) as pool:
        return pool.map(map_func, data, chunksize=int(len(data)/cpu_num))
    # with open('map_out.csv', 'w', newline='') as map_out_file:
    #         writer = csv.writer(map_out_file)
    #         for row in map_out:
    #             writer.writerow(row)

def shuffle(mapped_data):
    output = {}
    for i in mapped:
        if i[0] in output:
            output[i[0]].append(i[1])
        else:
            output[i[0]] = [i[1]]
    return output

def hadoop_reduce(data, red_func, cpu_num=mp.cpu_count()):
    with mp.Pool(processes=cpu_num) as pool:
        red_out = pool.map(red_func, data, chunksize=int(len(reduce_in.keys())/cpu_num))

class MapReduce:
    def __init__(self, data, map_func, reduce_func, cpu_num=mp.cpu_count()):
        self.data = data
        self.map_func = map_func
        self.reduce_func = reduce_func
        self.cpu_num = cpu_num

    def shuffle(self, mapped):
        output = {}
        for i in mapped:
            if i[0] in output:
                output[i[0]].append(i[1])
            else:
                output[i[0]] = [i[1]]
        return output

    def execute(self):
        with mp.Pool(processes=self.cpu_num) as pool:
            map_out = pool.map(self.map_func, self.data, chunksize=int(len(self.data)/self.cpu_num))
            map_out = [x for x in map_out if x is not None]
            reduce_in = self.shuffle(map_out)
            reduce_out = pool.map(self.reduce_func, reduce_in.items(), chunksize=int(len(reduce_in.keys())/self.cpu_num))
            return list(reduce_out)


def map_func(line):
        cols = line.split(',')
        if re.match(r'^[A-Z]{3}\d{4}[A-Z]{2}\d$', cols[0]):
            return (cols[0], 1)
def reduce_func(pair):
        return pair[0], sum(pair[1])

if __name__ == '__main__':
    passengers = open('AComp_Passenger_data_no_error.csv', "r", encoding="utf8")

    passengers_list = passengers.readlines()

    passedngers_mapped = MapReduce(passengers_list, map_func, reduce_func)
    result = passedngers_mapped.execute()
    max_val = max(result, key=lambda x: x[1])[1]
    print([item for item in result if item[1] == max_val])

    hadoop_map(passengers_list, map_func)


