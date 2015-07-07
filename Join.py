__author__ = 'Chiru'
import sys
from MapReduce import MapReduce
mr = MapReduce()

#Mapper is called for every record in the data file
def mapper(record):
    #Structure of record : every record has record_type in first field,order_id in the second field
    record_type = record[0]
    order_id = record[1]

    #Mapper will be called on all the orders first(As the data file contains the records of orders before line_items)
    #As the output of mapper is fed to reducer,all the output to be displayed need to be present in the output of the Mapper.
    #So emit_intermdiate all the records with order_id as the key
    #So for every record the id,record is emmited
    if record_type == "order":
        mr.emit_intermediate(order_id,record)
    elif record_type == "line_item":
        mr.emit_intermediate(order_id,record)

#Reducer funtion is called for every record in the output of the map phase (here it is the global dictionary mr.intermediate)
def reducer(key,list_of_values):
    #for all list_of_values first field will be order record and all the others will be line_item records
    #ie list_of_values[0] will be the orders and list_of_values[1:n] will be the list_item records

    #Every order is emitted with all the list_items having the same order ID
    current = 1;
    while current < len(list_of_values):
        mr.emit((list_of_values[0],list_of_values[current]))
        current = current + 1;

#####################################################
if __name__ == '__main__':
  inputdata = open(sys.argv[1])
  #execute funtion steps
  #1.Mapper function for every record (Every mapper appends the output to a global dictionary by calling the functin mr.emit_intermediate ..Line number 18 )
  #2.Reducer is called for every record in the output of the map phase (Every reducer appends the data to the global array mr.result)
  #3.The final result array is printed
  mr.execute(inputdata, mapper, reducer)