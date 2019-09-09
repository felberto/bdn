import math

from mrjob.job import MRJob
from mrjob.step import MRStep


class MapReduce2(MRJob):

    def steps(self):
        return [
            MRStep(mapper=self.mapper,
                   reducer=self.reducer1),
            MRStep(combiner=self.combiner,
                   reducer=self.reducer2)
        ]

    def mapper(self, _, line):
        lineList = line.split(',')
        if lineList[0] == 'msg_snd' and lineList[0] == 'res_rcv':
            if lineList[1] != -1 and lineList[1] != 0:
                yield str(lineList[1]) + ':' + str(lineList[2]), int(lineList[6])

    def reducer1(self, key, values):
        values = list(values)
        yield math.ceil(min(values) / 60000), max(values) - min(values)

    def combiner(self, key, values):
        values = list(values)
        yield key, [sum(values), len(values)]

    def reducer2(self, key, values):
        yield key, sum(values[0]) / sum(values[1])


if __name__ == '__main__':
    MapReduce2.run()
