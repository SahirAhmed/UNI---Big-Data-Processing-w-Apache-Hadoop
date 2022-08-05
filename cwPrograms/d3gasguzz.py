
from mrjob.job import MRJob
import time
from datetime import datetime

class PartD3(MRJob):

    def mapper(self, _, line):
        fields = line.split(",")

        try:
            if (len(fields)==7):
                gasprice = int(fields[5])

                time_epoch = int(fields[6])
                month = str(datetime.fromtimestamp(time_epoch).strftime("%B"))
                year = str(datetime.fromtimestamp(time_epoch).strftime("%Y"))
                date = str(month + " " + year)


                yield (date, (gasprice,1))

        except:
            pass

    def combiner(self, date, values):
        price = 0
        count = 0
        for x in values:
            price += x[0]
            count += x[1]
        yield (date, (price,count))

    def reducer(self, date, values):
        price = 0
        count = 0
        for x in values:
            price += x[0]
            count += x[1]
        yield (date, (price/count))

if __name__ == '__main__':
    PartD3.JOBCONF = { 'mapreduce.job.reduces': '3' }
    PartD3.run()
