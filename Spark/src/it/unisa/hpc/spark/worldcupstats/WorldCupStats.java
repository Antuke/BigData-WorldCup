/*
 * Click nbfs://nbhost/SystemFileSystem/Templates/Licenses/license-default.txt to change this license
 * Click nbfs://nbhost/SystemFileSystem/Templates/Classes/Main.java to edit this template
 */
package it.unisa.hpc.spark.worldcupstats;

import java.io.Serializable;
import java.util.*;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.*;
import scala.Tuple2;

/**
 *
 * author : Sessa Antonio 0622702305, a.sessa108@studenti.unisa.it
 * Canale IZ
 * lecturer : Giuseppe D'Aniello gidaniello@unisa.it
 */
/*
Realizzare la classifica delle top X squadre che hanno vinto più partite con Y o più goal di differenza, ad esempio se Y = 1, ottengo la classifica delle squadre che hanno vinto più partite. Considerare la possibilità che una partita finisca ai rigori. X e Y devono essere indicati dall'utente e sono entrambi interi positivi. La classifica deve essere stampata a video.
*/
public class WorldCupStats {

    /**
     * @param args the command line arguments
     */
    public static void main(String[] args) {
        if(args.length < 3) {
            System.out.println("Usage : inputPath  , K , differenza reti (numero intero positivo)");
            return ;
        }
        String inputFile = args[0];
        Integer K = Integer.parseInt(args[1]);
        Integer threshold = Integer.parseInt(args[2]);
        
        
        
        
// Create a configuration object and set the name of the application 
        SparkConf conf = new SparkConf().setAppName("Spark World Cup Stats");

// Create a Spark Context object
        JavaSparkContext sc = new JavaSparkContext(conf);

        // Build an RDD of Strings from the input textual file 
// Each element of the RDD is a line of the input file 
        JavaRDD<String> lines = sc.textFile(inputFile);
        // filter out the header
        JavaRDD<String> linesWithoutHeader = lines.filter(s -> !s.contains("Away Team Name"));

        // Create an RDD of matches
        JavaRDD<Match> matches = linesWithoutHeader.map(s -> {
            String[] splitted = s.split(",");
            // check for penalties
            if(splitted[9].contains("penalties"))
            {
                // Assuming that the penalites are the only integers in the string, this leaves us with a string containing
                // the penalties shootout result separeted by "-" (e.g 4-1)
                String [] penalties = splitted[9].replaceAll("[^0-9-]", "").split("-");
                // adding the result of the penalties to the scores
                return new Match(splitted[5], splitted[8], Integer.valueOf(splitted[6]) + Integer.valueOf(penalties[0]), Integer.valueOf(splitted[7])+ Integer.valueOf(penalties[1]),threshold);

            }
            // the value we are intrest in are homeTeam (5), awayTeam(6) and the result of the match (7 and 8)
            return new Match(splitted[5], splitted[8], Integer.valueOf(splitted[6]), Integer.valueOf(splitted[7]),threshold);
        });

        // Create the RDD <teamName,1>
        // flat map beacause not every match ends with a Y goal difference (or larger)
        JavaPairRDD<String, Integer> partialWinners = matches.flatMapToPair(f -> {
            List<Tuple2<String, Integer>> pairs = new ArrayList();
            if(f.checkGoalDifference() != null){
                pairs.add(f.checkGoalDifference());
            }
            return pairs.iterator();
        });

        // Create the RDD <teamName, numberOfWinsWithThreeGoalLead>
        JavaPairRDD<String, Integer> reducedWinners = partialWinners.reduceByKey((a, b) -> {
            return a + b; //basically, groupby key and sum the values
        });
        
        // Create the RDD <numberOfWinsWithThreeGoalLead, teamName> by simply inverting the previous RDD
        JavaPairRDD<Integer,String> invertedReducedWinners = reducedWinners.mapToPair(f -> {
            return new Tuple2(f._2(),f._1());
        });
        
        // Create the RDD <numberOfWinsWithThreeGoalLead, list of teamNames>
        JavaPairRDD<Integer, String> invertedIndex = invertedReducedWinners.reduceByKey((a,b)->{
            return a + " " + b; //basically, groupby key and concatenate the values
        });
        
        // Sort the RDD by key invertedIndex, false for descending order
        JavaPairRDD<Integer, String> sortedInvertedIndex = invertedIndex.sortByKey(false);
        
        // Take the top K of the sorted invertedIndex
        List<Tuple2<Integer, String>> TopK = sortedInvertedIndex.take(K);
        
        /***********************************/
        /* ALTERNATIVE SOLUTION THAT DOES NOT MANAGES TIES */
        /***********************************/
        
        // Get the top K, sorted by their values
        // I tried to use lambda expressions but I got the error task not serializable
        // It seems that the lambda expressions the functions as a comparator is not serializable and this causes the exceptions
        // Unsure if it's a bug
        // the work around is simple, I created a comparator class that implements serializable to pass to the top function
        List<Tuple2<String, Integer>> Results = reducedWinners.top(K,new Tuple2Comparator());
        
        /***************************************************/
        /* PRINTING RESULTS TO VIDEO */
        System.out.println("/*****************************************/\n");
        System.out.println("Le Top " + K + " Squadre dei mondiali di calcio tenutasi tra il 1930 e il 1998 che hanno vinto il maggior numero di partite con "+ threshold +" o più goal di scarto (rigori inclusi)");
        for (Tuple2<String, Integer> t : Results) {
            System.out.println(t);
        }
        
        System.out.println("\n\nCON GESTIONE TIES\n");
        System.out.println("Le Top " + K + " Squadre dei mondiali di calcio tenutasi tra il 1930 e il 1998 che hanno vinto il maggior numero di partite con "+ threshold +" o più goal di scarto (rigori inclusi)");
        for (Tuple2<Integer, String> t : TopK) {
            System.out.println(t);
        }
        System.out.println("\n");
// Close the Spark Context object
        sc.close();

    }
    
    // compartor for .top function
    public static class Tuple2Comparator implements Comparator<Tuple2<String, Integer>>, Serializable {

        @Override
        public int compare(Tuple2<String, Integer> tuple1, Tuple2<String, Integer> tuple2) {
            return tuple1._2() - tuple2._2(); 
        }
    }

    public static class Match implements Serializable {

        String homeTeam;
        String awayTeam;
        Integer goalDifference;
        Integer treshold;    
        
    
        /**
         *
         * @param homeTeam
         * @param awayTeam
         * @param goalHomeTeam
         * @param goalAwayTeam
         */
        public Match(String homeTeam, String awayTeam, Integer goalHomeTeam, Integer goalAwayTeam, Integer treshold) {
            this.homeTeam = homeTeam;
            this.awayTeam = awayTeam;
            this.goalDifference = goalHomeTeam - goalAwayTeam;
            this.treshold = treshold;
        }

        /**
         *
         * @return The team that won the match with 3 or more goal lead, null otherwise
         */
        public Tuple2<String, Integer> checkGoalDifference() {
            if (goalDifference >= treshold) {
                return new Tuple2(homeTeam, 1);
            }
            if (goalDifference <= -treshold) {
                return new Tuple2(awayTeam, 1);
            }
            return null;
        }

    }
}
