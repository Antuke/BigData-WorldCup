/*
 * Click nbfs://nbhost/SystemFileSystem/Templates/Licenses/license-default.txt to change this license
 * Click nbfs://nbhost/SystemFileSystem/Templates/Classes/Class.java to edit this template
 */
package finalproject;


import java.io.IOException;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

/**
 *
 * author : Sessa Antonio 0622702305, a.sessa108@studenti.unisa.it
 * Canale IZ
 * lecturer : Giuseppe D'Aniello gidaniello@unisa.it
 */
/*
Realizzare l’indice degli arbitri, di una certa nazionalità, che consenta di sapere ogni arbitro quali partite ha arbitrato, considerando che un arbitro può ricoprire il ruolo di Referee, Assistant 1 e Assistant 2 in partite diverse. La nazionalità deve essere indicata dall’utente (ad esempio ITA).
*/

class ReducerReferee extends Reducer 
        <Text, // Input key type
	Text, // Input value typeF
	Text, // Output key type
	Text> { // Output value type

	@Override
	protected void reduce(Text key, // Input key type (Referee name)
			Iterable<Text> values, // Input value type (Match Ids)
			Context context) throws IOException, InterruptedException {

		String invIndex = new String();

		// Iterate over the set of Match Ids and concatenate them
		for (Text value : values) {
			invIndex = invIndex.concat(value + ",");
		}
                
                // avoid writing the last comma by taking the substring
		context.write(key, new Text(invIndex.substring(0,invIndex.length() - 1 )));
	}
}
