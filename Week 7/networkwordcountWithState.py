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

# wat is de bron van de data (in dit geval de socket met het nc commando)
lines = ssc.socketTextStream('localhost', 9999)

words = lines.flatMap(lambda line: line.split(' ')) # splits elke rij in woorden en geef elke woord een aparte rij
kvs = words.map(lambda word: (word, 1))  # maak een key-value paar van elk woord
wordCounts = kvs.reduceByKey(lambda x, y: x+y) # tel de values van een key/woord op

# een functie die gaat bepalen hoe de state berekend wordt
def updateFunctie(newValues, oldValues):
    if oldValues is None:
        oldValues = 0
    return sum(newValues, oldValues)

globalWordCounts = kvs.updateStateByKey(updateFunctie) # dit berekent de globale wordcount ipv de lokale met reduceByKey

# output (mooiere print, pretty print)
wordCounts.pprint()
globalWordCounts.pprint()

# hierboven wordt er nog niets gestart
# wat je nog moet doen is eigenlijk zeggen dat de applicatie moet beginnen luisteren
ssc.start()
# om de applicatie continue te laten luisteren mag hij niet afsluiten
ssc.awaitTermination()
