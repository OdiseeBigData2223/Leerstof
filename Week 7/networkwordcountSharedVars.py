from pyspark import SparkContext
from pyspark.streaming import StreamingContext

# Aanmaken van een streaming applicatie
# Maak een spark applicatie
sc = SparkContext('local[2]', 'networkwordcount')
sc.setLogLevel('ERROR') # verwijder warnings van output
# Maak er een streaming applicatie van
ssc = StreamingContext(sc, 5) # data wordt per 5 seconden gegroepeerd
# waar wordt de state bewaard
ssc.checkpoint('checkpoint')

# dit gaat werken in pyspark zelf en in streaming tot als het crashed
# dan wordt deze broadcasted variabele niet gerecoveren uit de checkpoint omdat deze niet bewaard blijft.
#excludeList = sparkContext.broadcast(['a', 'b'])
# dit kan opgelost worden door er een singleton van te maken
def getExcludeList(sparkContext):
    if 'excludeList' not in globals():
        # als de lijst nog niet bestaat -> voeg hem toe / maak hem opnieuw
        globals()['excludeList'] = sparkContext.broadcast(['a', 'b'])
    return globals()['excludeList']

# accumulator werkt hetzelfde als broadcast variabele
def getDroppedWordCounter(sparkContext):
    if 'droppedWord' not in globals():
        # als de lijst nog niet bestaat -> voeg hem toe / maak hem opnieuw
        globals()['droppedWord'] = sparkContext.accumulator(0)
    return globals()['droppedWord']

# wat is de bron van de data (in dit geval de socket met het nc commando)
lines = ssc.socketTextStream('localhost', 9999)

words = lines.flatMap(lambda line: line.split(' ')) # splits elke rij in woorden en geef elke woord een aparte rij
kvs = words.map(lambda word: (word, 1))  # maak een key-value paar van elk woord
wordCounts = kvs.reduceByKey(lambda x,y: x+y)

# DOEL: wordcount te doen waarvan woorden die starten met de letters a of b niet geteld worden
# dit gaat niet met updateStateByKey omdat je de key niet hebt in de updatefunctie

def fnc(time, rdd):
    excludeList = getExcludeList(rdd.context)
    counter = getDroppedWordCounter(rdd.context)
    
    # filteren van de woorden gebeurt dmv een functie die false returned als het woord weggelaten moet worden
    # en true als het woord in de rdd mag blijven
    def filterFnc(word_count):
        woord = word_count[0]
        if len(woord) > 0 and woord[0] in excludeList.value:
            counter.add(word_count[1])
            return False
        else:
            return True
        
    f = rdd.filter(filterFnc)
    
    print('filtered rdd:', str(f.collect()))
    print('dropped words:', str(counter.value))
    
wordCounts.foreachRDD(fnc)

# hierboven wordt er nog niets gestart
# wat je nog moet doen is eigenlijk zeggen dat de applicatie moet beginnen luisteren
ssc.start()
# om de applicatie continue te laten luisteren mag hij niet afsluiten
ssc.awaitTermination()
