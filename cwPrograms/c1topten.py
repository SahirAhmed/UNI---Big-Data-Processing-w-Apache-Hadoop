
from mrjob.job import MRJob
import time
from datetime import datetime

class PartC1(MRJob):

    def mapper(self, _, line):
        fields = line.split(",")

        try:
            if (len(fields)==9):
                miner = fields[2]
                size = int(fields[4])

                yield (miner, size)
        except:
            pass

    def combiner(self, miner, values):
        yield (miner, sum(values))

    def reducer(self, miner, values):
        yield (miner, sum(values))

if __name__ == '__main__':
    PartC1.JOBCONF = { 'mapreduce.job.reduces': '3' }
    PartC1.run()
