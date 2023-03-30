from pyspark import SparkContext
from pyspark.streaming import StreamingContext

# Aanmaken van een streaming applicatie
# Maak een spark applicatie
sc = SparkContext('local[2]', 'networkwordcount')
sc.setLogLevel('ERROR') # verwijder warnings van output
# Maak er een streaming applicatie van
ssc = StreamingContext(sc, 1) # data wordt per 5 seconden gegroepeerd
# waar wordt de state bewaard
ssc.checkpoint('temp_state')    # OPLOSSING DEEL 2

# wat is de bron van de data (in dit geval de socket met het nc commando)
lines = ssc.socketTextStream('localhost', 9999)
lines = lines.window(5,2)  # OPLOSSING DEEL 3 
# -> 5 is de grootte van het tijdsvenster (hoe ver in de geschiedenis moet er gekeken worden)
# -> 2 is de stap waarin het venster opschuift

def getCounter(sparkContext):
    if 'counter' not in globals():
        # als de lijst nog niet bestaat -> voeg hem toe / maak hem opnieuw
        globals()['counter'] = sparkContext.accumulator(0)
    return globals()['counter']

# OPLOSSING DEEL 1
words = lines.flatMap(lambda line: line.split(' '))
kvs = words.map(lambda word: (len(word), 1))  
lengteCounts = kvs.reduceByKey(lambda x, y: x+y)
lengteCounts.pprint()

# 2 manieren voor OPLOSSING DEEL 4
# update state by key om het totaal per lengte bij te houden
# accumulator om alle ontvangen woorden te tellen (alle lengtes bij elkaar)
def updateFunctie(newValues, oldValues):
    if oldValues is None:
        oldValues = 0
    s = sum(newValues)
    getCounter().add(s)    # manier 2 (de rest is manier 1)
    return newValues + oldValues

globalWordCounts = kvs.updateStateByKey(updateFunctie) 

ssc.start()
ssc.awaitTermination()
