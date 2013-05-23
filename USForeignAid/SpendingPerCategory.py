import MapReduce
import sys
import csv

reducerList  = {}

mr = MapReduce.MapReduce()
def is_number(s):
    try:
        float(s)
        return True
    except ValueError:
        return False

def aggregator(res):
    return sorted(res, key= lambda category: category[1]) 
       
def mapper(record):
    category = record[1]
    mr.emit_intermediate(category, record[2:])
    
       
def reducer(key, data):
   totalSpending = 0
   for i in data:
         for spending in i:
             if spending is not None and len(spending) > 0 and is_number(spending):
                 totalSpending = totalSpending + float(spending)
   mr.emit((key, totalSpending))

if __name__ == '__main__':
  with open('ForeignAid2.csv', 'rb') as csvfile:
      aid = csv.reader(csvfile, delimiter = ',')
      mr.execute(aid, mapper, reducer, aggregator)