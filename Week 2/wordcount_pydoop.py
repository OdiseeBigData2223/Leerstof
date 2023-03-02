import pydoop.mapreduce.api as api
import pydoop.mapreduce.pipes as pipes

class Mapper(api.Mapper):
    def map(self, context):
        for w in context.value.split():
            context.emit(w, 1)
    
class Reducer(api.Reducer):
    def reduce(self, context):
        context.emit(context.key, sum(context.values))
        
FACTORY = pipes.Factory(Mapper, reducer_class=Reducer)

def main():
    pipes.run_task(FACTORY)

if __name__ == "__main__":
    main()
