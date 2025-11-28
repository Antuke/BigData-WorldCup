/*
 * Click nbfs://nbhost/SystemFileSystem/Templates/Licenses/license-default.txt to change this license
 * Click nbfs://nbhost/SystemFileSystem/Templates/Classes/Main.java to edit this template
 */
package finalproject;

import java.io.IOException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

/**
 *
 * author : Sessa Antonio 0622702305, a.sessa108@studenti.unisa.it
 * Canale IZ
 * lecturer : Giuseppe D'Aniello gidaniello@unisa.it
 */
/*
Realizzare l’indice degli arbitri, di una certa nazionalità, che consenta di sapere ogni arbitro quali partite ha arbitrato, considerando che un arbitro può ricoprire il ruolo di Referee, Assistant 1 e Assistant 2 in partite diverse. La nazionalità deve essere indicata dall’utente (ad esempio ITA).
*/
public class FinalProject {

    /**
     * @param args the command line arguments
     */
    public static void main(String[] args) throws IOException, InterruptedException, ClassNotFoundException {
        Path inputPath;
        Path outputDir;
        
        if(args.length < 3) {
            System.out.println("Usage : inputPath (hdfs) , outputPath (hdfs), nationality e.g ITA");
            return ;
        }
        
        // Parse the parameters
        inputPath = new Path(args[0]);
        outputDir = new Path(args[1]);
        Configuration conf = new Configuration();
        conf.set("nationality", args[2]);
        
        // Define a new job
        Job job = Job.getInstance(conf);

        // Assign a name to the job
        job.setJobName("InvertedIndex");

        // Set path of the input file/folder (if it is a folder, the job reads all the files in the specified folder) for this job
        FileInputFormat.addInputPath(job, inputPath);

        // Set path of the output folder for this job
        FileOutputFormat.setOutputPath(job, outputDir);

        // Specify the class of the Driver for this job
        job.setJarByClass(FinalProject.class);

        // Set job input format
        job.setInputFormatClass(TextInputFormat.class);

        // Set job output format
        job.setOutputFormatClass(TextOutputFormat.class);

        // Set map class
        job.setMapperClass(MapperReferee.class);
        
        // Set map input key and value classes
        // Set map output key and value classes
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);

        // Set reduce class
        job.setReducerClass(ReducerReferee.class);

        // Set reduce output key and value classes
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        // Set number of reducers
        //job.setNumReduceTasks(numberOfReducers);
        
        // Execute the job and wait for completion
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
    
}
