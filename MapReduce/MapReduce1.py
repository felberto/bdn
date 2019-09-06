from mrjob.job import MRJob
import math


class MapReduce1(MRJob):

    def mapper(self, _, line):
        list = line.split(",")
        if (list[0] == "res_snd"):
            yield math.ceil(int(list[
                                    6]) / 60000.0), 1  # Runden, damit von 1.n bis 2.0 alles als Minute 2 gilt. Damit wird die Minute als Periode betrachtet.

    def combiner(self, key, values):
        yield key, sum(values)  # Vorverarbeitung für Reducer

    def reducer(self, key, values):
        yield key, sum(values)


if __name__ == '__main__':
    MapReduce1.run()
