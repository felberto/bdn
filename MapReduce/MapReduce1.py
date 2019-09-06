import math

from mrjob.job import MRJob


class MapReduce1(MRJob):
    def mapper(self, _, line):
        lineList = line.split(',')
        if 'res_send' in lineList[0]:
            print(math.ceil(int(lineList[6]) / 60000.0), 1)
            yield math.ceil(int(lineList[6]) / 60000.0), 1

    def reducer(self, key, values):
        yield key, sum(values)


if __name__ == '__main__':
    MapReduce1.run()
