import math

from mrjob.job import MRJob


class MapReduce1(MRJob):
    def mapper(self, _, line):
        lineList = line.split(',')
        if 'res_snd' in lineList[0]:
            yield math.ceil(int(lineList[6]) / 60000), 1

    def combiner(self, key, values):
        yield key, sum(values)

    def reducer(self, key, values):
        yield key, sum(values)


if __name__ == '__main__':
    MapReduce1.run()
