__author__ = 'Chiru'
import sys
from MapReduce import MapReduce
mr = MapReduce()

#Mapper is called for every record in the data file
def mapper(record):
    #Structure of record : each record is an Array , first elements is book name and second element is book content
    bookName = record[0]
    bookContent = record[1]
    #Find all the words in book content
    words = bookContent.split()
    #Filter duplicates
    unique_words = set(words)
    #For every word emit_intermediate word,book_name
    for current_word in unique_words:
        mr.emit_intermediate(current_word,bookName)

#Reducer funtion is called for every record in the output of the map phase (here it is the global dictionary mr.intermediate)
def reducer(key,list_of_values):
    #No more processin is required. Just emit the arguments as a single array. Emit appends the data to global array mr.result
    mr.emit((key,list_of_values))

#####################################################
if __name__ == '__main__':
  inputdata = open(sys.argv[1])
  #execute funtion steps
  #1.Mapper function for every record (Every mapper appends the output to a global dictionary by calling the functin mr.emit_intermediate ..Line number 18 )
  #2.Reducer is called for every record in the output of the map phase (Every reducer appends the data to the global array mr.result)
  #3.The final result array is printed
  mr.execute(inputdata, mapper, reducer)