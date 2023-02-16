import pydoop.mapreduce.api as api
import pydoop.mapreduce.pipes as pipes
from itertools import tee

class Mapper(api.Mapper):
    def map(self, context):
        for w in context.value.split():
            if len(w) >0 and w[0].isalpha():
                context.emit(w, 1)
            context.emit("word length", len(w))
    
class Reducer(api.Reducer):
    def reduce(self, context):
        if context.key == "word length":
            # error omdat len niet bestaat        
            #context.emit("gemiddelde", sum(context.values) / len(context.values))
            
            # met itertools
            print('met itertools')
            it1, it2 = tee(context.values, 2)
            som = sum(it1)
            aantal = len(it2)
            context.emit("gemiddelde lengte", som/aantal)
            
            # met list
            print('met list')
            l = list(context.values)
            context_emit("gemmiddelde lengte met list", sum(l) / len(l))
            
            print('met for')
            som = 0
            aantal = 0
            for val in context.values:
                aantal += 1
                som += val
            context.emit("gemiddelde lengte met for", som/aantal)
        else:
            context.emit(context.key, sum(context.values))
        
FACTORY = pipes.Factory(Mapper, reducer_class=Reducer)

def main():
    pipes.run_task(FACTORY)

if __name__ == "__main__":
    main()
