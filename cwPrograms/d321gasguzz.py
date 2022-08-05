
from mrjob.job import MRJob
import time
from datetime import datetime

class PartD3(MRJob):

    def mapper(self, _, line):

        try:
            fields = line.split(",")
            if (len(fields)==7):
                toadd = fields[2] #toaddress from transactions
                agggas = int(fields[4]) #aggregate of gas

                if agggas == 0:
                    pass
                else:
                    yield (toadd, agggas)
        except:
            pass

    def combiner(self, toadds, values):
        yield (toadds, sum(values))

    def reducer(self, toadds, values):
        yield (toadds, sum(values)) #unique toaddress, gas aggregate

if __name__ == '__main__':
    PartD3.JOBCONF = { 'mapreduce.job.reduces': '3' }
    PartD3.run()
