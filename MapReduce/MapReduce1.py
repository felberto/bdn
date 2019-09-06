import math

from mrjob.job import MRJob


class MapReducer1(MRJob):
    def mapper(self, _, line):
        lineList = line.split(",")
        if 'res_send' in lineList[0]:
            yield math.ceil(lineList[6]/60000), 1
            print(math.ceil(lineList[6]/60000), 1)

    def reducer(self, key, values):
        yield key, sum(values)


if __name__ == '__main__':
    MapReducer1.run()
