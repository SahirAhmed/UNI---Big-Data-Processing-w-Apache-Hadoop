
from mrjob.job import MRJob
import time
from datetime import datetime

class PartA(MRJob):

    def mapper(self, _, line):
        fields = line.split(",")

        try:
            if (len(fields)==7):
                time_epoch = int(fields[6])
                month = str(datetime.fromtimestamp(time_epoch).strftime("%B"))
                year = str(datetime.fromtimestamp(time_epoch).strftime("%Y"))
                date = str(month + " " + year)

                yield (date, 1)

        except:
            pass

    def combiner(self, date, count):
        yield (date, sum(count))

    def reducer(self, date, count):
        yield (date, sum(count))

if __name__ == '__main__':
    PartA.JOBCONF = { 'mapreduce.job.reduces': '3' }
    PartA.run()
