from pyspark import SparkContext
from pyspark.streaming import StreamingContext
from datetime import datetime

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
kvs = words.map(lambda word: (word, datetime.now()))  # de value moet NIET altijd een integer zijn

#def updateFunctie(newValues, oldValues):
#    if len(newValues) == 0:
#        return oldValues
#    return newValues[0]

def updateFunctie(newValues, oldValues):
    # pas op dat de update functie wordt voor elke key in de state opgeroepen en niet voor elke key in het rdd van de dstream
    # elke key wordt hier opgeroepen per minitbatch
    if oldValues is None:
        oldValues = []
    oldValues.extend([datetime.now()])
    return oldValues

  
# meest recente tijdstip dat elk woord ontvangen werd
most_recent_detection = kvs.updateStateByKey(updateFunctie)

# output (mooiere print, pretty print)
most_recent_detection.pprint()

# hierboven wordt er nog niets gestart
# wat je nog moet doen is eigenlijk zeggen dat de applicatie moet beginnen luisteren
ssc.start()
# om de applicatie continue te laten luisteren mag hij niet afsluiten
ssc.awaitTermination()
