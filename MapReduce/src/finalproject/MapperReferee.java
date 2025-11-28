/*
 * Click nbfs://nbhost/SystemFileSystem/Templates/Licenses/license-default.txt to change this license
 * Click nbfs://nbhost/SystemFileSystem/Templates/Classes/Class.java to edit this template
 */
package finalproject;

import org.apache.hadoop.mapreduce.Mapper;
import java.io.IOException;
import org.apache.hadoop.io.LongWritable;

import org.apache.hadoop.io.Text;

/**
 *
 * author : Sessa Antonio 0622702305, a.sessa108@studenti.unisa.it
 * Canale IZ
 * lecturer : Giuseppe D'Aniello gidaniello@unisa.it
 */
/*
Realizzare l’indice degli arbitri, di una certa nazionalità, che consenta di sapere ogni arbitro quali partite ha arbitrato, considerando che un arbitro può ricoprire il ruolo di Referee, Assistant 1 e Assistant 2 in partite diverse. La nazionalità deve essere indicata dall’utente (ad esempio ITA).
*/
class MapperReferee extends Mapper 
        <LongWritable, // Input key type
	Text, // Input value type
	Text, // Output key type
	Text> {// Output value type
        
        private String nationality;
        
        @Override
        public void setup(Context context){
            // Get the searched nationality from the context
            nationality = context.getConfiguration().get("nationality");
        }
        
        @Override
	protected void map(LongWritable key, // Input key type
			Text value, // Input value type
			Context context) throws IOException, InterruptedException {

		// Split each sentence in words. Use whitespace"," as delimiter 
		// The split method returns an array of strings
		String[] words = value.toString().split(",");
		// Check field 13,14,15 (respectively Referee, assistant 1 and assistant 2)
                // if it contains the searched nationality, context write it + match Id
                // match Id is field 17   

                if(words[13].contains(nationality)) context.write(new Text(words[13]), new Text(words[17]));
                if(words[14].contains(nationality)) context.write(new Text(words[14]), new Text(words[17]));
                if(words[15].contains(nationality)) context.write(new Text(words[15]), new Text(words[17]));

                
	
	}
}