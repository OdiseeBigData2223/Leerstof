def mapper(_, text, context):
    for word in text.split():
        print(word)
        raise Exception("oepsie")
        context.emit(word, 1)
        
def reducer(word, counts, context):
    context.emit(word, sum(counts))
    
