# met counters
import pydoop.mapreduce.api as api
import pydoop.mapreduce.pipes as pipes

class Mapper(api.Mapper):
    def __init__(self, context):
        super(Mapper, self).__init__(context)
        # constructor in python
        context.set_status("constructor mapper")
        self.input_words = context.get_counter("WORDCOUNT", "INPUT_WORDS")
        
    def map(self, context):
        words = context.value.split()
        for w in words:
            context.emit(w, 1)
        context.increment_counter(self.input_words, len(words))
    
class Reducer(api.Reducer):
    def __init__(self, context):
        super(Reducer, self).__init__(context)
        # constructor in python
        context.set_status("constructor reducer")
        self.output_words = context.get_counter("WORDCOUNT", "OUTPUT_WORDS")
        
    def reduce(self, context):
        context.emit(context.key, sum(context.values))
        context.increment_counter(self.output_words, 1)
        
FACTORY = pipes.Factory(Mapper, reducer_class=Reducer)

def main():
    pipes.run_task(FACTORY)

if __name__ == "__main__":
    main()
