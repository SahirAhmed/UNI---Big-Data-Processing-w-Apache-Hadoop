
from mrjob.job import MRJob
import time
from datetime import datetime

class PartB3(MRJob):

    def mapper(self, _, line):
        fields = line.split("\t")

        try:
            #access the fields you want, assuming the format is correct now
            if (len(fields)==2):
                address = fields[0]
                aggvalue = int(fields[1])
                yield(None, (address, aggvalue))

        except:
            pass
            #no need to do anything, just ignore the line, as it was malformed

    def combiner(self, _, values):
        sorted_values = sorted(values,reverse=True,key=lambda tup:tup[1])
        i=0
        for value in sorted_values:
            yield("top",value)
            i+=1
            if i >= 10:
                break

    def reducer(self, _, values):
        sorted_values = sorted(values,reverse=True,key=lambda tup:tup[1])
        i=0
        for value in sorted_values:
            yield("{} - {}".format(value[0], value[1]), None)
            i+=1
            if i >= 10:
                break

if __name__ == '__main__':
    PartB3.JOBCONF = { 'mapreduce.job.reduces': '3' }
    PartB3.run()
