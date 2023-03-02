from mrjob.job import MRJob

class MrWordCount(MRJob):
    def mapper(self, _, line):
        # 2 argumenten -> (key, value) , key is niet belangrijk dus _ , line is 1 lijn in het bestand
        # deze functie wordt dus lijn per lijn opgeroepen
        for word in line.split():
            # yield -> return die de functie niet stopt
            yield (word, 1)
    
    def reducer(self, word, counts):
        # counts is een soort list van de aantal die we yielden in de mapper
        yield (word, sum(counts))
        
if __name__ == '__main__':
    MrWordCount.run()
