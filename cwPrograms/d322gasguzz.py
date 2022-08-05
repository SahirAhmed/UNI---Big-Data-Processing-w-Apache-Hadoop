
from mrjob.job import MRJob
import time
from datetime import datetime
import re

class PartD3(MRJob):

    def mapper(self, _, line):

        try:
            #touniqueaddress, aggregation | cut down transactions
            if len(line.split('\t')) == 2:
                fields = line.split('\t')
                join_key = fields[0] #to_address
                refined_join_key = join_key[1:-1] #to cater for the speech marks
                join_value = int(fields[1]) #aggregated_value
                yield(refined_join_key,(join_value,1))

            #address, is_erc20, is_erc721, block_number, block_timestamp | contracts
            if len(line.split(',')) == 5:
                fields = line.split(',')
                join_key = fields[0] #address
                join_value = str(fields[4]) #block_timestamp
                yield(join_key,(join_value,2))
        except:
            pass

    def reducer(self, address, values):
        blocktime = ""
        aggvalue = 0

        for value in values:
            if value[1]==1:
                aggvalue = value[0]
            elif value[1]==2:
               blocktime = str(value[0])
        if aggvalue != 0 and blocktime != "":
            yield (address, (aggvalue, blocktime))

if __name__ == '__main__':
    PartD3.JOBCONF = { 'mapreduce.job.reduces': '5' }
    PartD3.run()
