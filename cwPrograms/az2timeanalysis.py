
from mrjob.job import MRJob
import time
from datetime import datetime

class PartA2(MRJob):

    def mapper(self, _, line):
        fields = line.split(",")

        try:
            if (len(fields)==7):
                value = int(fields[3])

                time_epoch = int(fields[6])
                month = str(datetime.fromtimestamp(time_epoch).strftime("%B"))
                year = str(datetime.fromtimestamp(time_epoch).strftime("%Y"))
                date = str(month + " " + year)


                yield (date, (value,1))

        except:
            pass

    def combiner(self, date, values):
        wei = 0
        count = 0
        for x in values:
            wei += x[0]
            count += x[1]
        yield (date, (wei,count))

    def reducer(self, date, values):
        wei = 0
        count = 0
        for x in values:
            wei += x[0]
            count += x[1]
        yield (date, (wei/count))

if __name__ == '__main__':
    PartA2.JOBCONF = { 'mapreduce.job.reduces': '3' }
    PartA2.run()
