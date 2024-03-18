import multiprocessing as mp
import re

class MapReduce:
    def __init__(self, data, map_func, reduce_func):
        self.data = data
        self.map_func = map_func
        self.reduce_func = reduce_func

    def shuffle(self, mapped):
        output = {}
        for i in mapped:
            if i[0] in output:
                output[i[0]].append(i[1])
            else:
                output[i[0]] = [i[1]]
        return output

    def execute(self):
        with mp.Pool(processes=mp.cpu_count()) as pool:
            map_out = pool.map(self.map_func, self.data, chunksize=int(len(self.data)/mp.cpu_count()))
            reduce_in = self.shuffle(map_out)
            reduce_out = pool.map(self.reduce_func, reduce_in.items(), chunksize=int(len(reduce_in.keys())/mp.cpu_count()))
            return list(reduce_out)


def map_func(line):
        cols = line.split(',')
        if re.match("^[A-Z]{3}\d{4}[A-Z]{2}\d$", cols[0]):
            return (cols[0], 1)
def reduce_func(pair):
        return pair[0], sum(pair[1])

if __name__ == '__main__':
    passengers = open('AComp_Passenger_data_no_error.csv', "r", encoding="utf8")
    airports = open('Top30_airports_LatLong.csv', "r", encoding="utf8")

    passengers_list = passengers.readlines()

    passedngers_mapped = MapReduce(passengers_list, map_func, reduce_func)
    result = passedngers_mapped.execute()
    print(max(result, key=lambda x: x[1]))


