import json

class MapReduce:
    def __init__(self):
        self.intermediate = {}
        self.result = []

    def emit_intermediate(self, key, value):
        self.intermediate.setdefault(key, [])
        self.intermediate[key].append(value)

    def emit(self, value):
        self.result.append(value) 

    def execute(self, data, mapper, reducer, cb):
        for row in data:
          mapper(row)
        for key in self.intermediate:
            reducer(key, self.intermediate[key])
        
        self.result = cb(self.result)
        #self.result = sorted(self.result, key= lambda country: country[1]) 
        
        jenc = json.JSONEncoder()
        for item in self.result:
            #print jenc.encode(item)
            print item[0], "\t" ,item[1]