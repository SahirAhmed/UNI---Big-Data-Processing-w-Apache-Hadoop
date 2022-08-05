
from mrjob.job import MRJob
import time
from datetime import datetime

class PartB1(MRJob):

    def mapper(self, _, line):
        fields = line.split(",")

        try:
            if (len(fields)==7):
                toadd = fields[2]
                value = int(fields[3])

                yield (toadd, value)
        except:
            pass

    def combiner(self, toadds, values):
        yield (toadds, sum(values))

    def reducer(self, toadds, values):
        yield (toadds, sum(values))

if __name__ == '__main__':
    PartB1.run()
