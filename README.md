# Big Data Project - World Cup Stats

Progetto per l'analisi delle statistiche dei Mondiali di calcio utilizzando **Hadoop MapReduce** e **Apache Spark**.

## Descrizione
Il progetto implementa due principali analisi sul dataset `worldcup1.csv`:
1.  **MapReduce**: Creazione di un indice degli arbitri raggruppati per nazionalità.
2.  **Apache Spark**: Calcolo della classifica delle squadre che hanno vinto più partite con una specifica differenza reti (inclusi i rigori).

## Struttura del Repository
- **MapReduce/**: Contiene il codice sorgente Java, il Jar compilato e le istruzioni per Hadoop.
- **Spark/**: Contiene il codice sorgente Java, il Jar compilato, gli script di lancio (`.sh`) e le istruzioni per Spark.

## Istruzioni per l'Esecuzione

### Hadoop MapReduce
Vedi il file `MapReduce/how_to_run.txt` per i dettagli completi.
In breve:
```bash
hadoop jar FinalProject.jar /dataset /refereeIndex <NAZIONALITA>
```

### Apache Spark
Vedi il file `Spark/how_to_run.txt` per i dettagli.
Sono forniti due script per facilitare l'esecuzione:
- **Locale**: `./Spark/launch_single.sh`
- **Cluster**: `./Spark/launch_cluster.sh`

## Autore
Sessa Antonio
Matricola: 0622702305
Canale IZ
