package myudfs;
import java.io.IOException;
import java.lang.Math;
import org.apache.pig.EvalFunc;
import org.apache.pig.data.Tuple;
import org.apache.pig.data.DataType;
import org.apache.pig.data.BagFactory;
import org.apache.pig.data.DataBag;
import org.apache.pig.data.Tuple;
import org.apache.pig.data.TupleFactory;


/*
Como ejecutar:
MAPREDUCE: Run a native Hadoop .jar


Transformar ese archivo a un .jar
Luego, en pig, escribir:
register myudfs.jar;
A = LOAD 'student_data' AS (t1:tuple(danceability:float,energy:float,speechiness:float), t2:tuple(danceability:float,energy:float,speechiness:float));
B = FOREACH A GENERATE myudfs.Similarity(A);
DUMP B;
*/

public class Similarity extends EvalFunc<Double>{
    public static double pow2(double a){
        return Math.pow(a, 2);
    }

    public Double exec(Tuple input) throws IOException {
        if (input == null || input.size() == 0 || input.get(0) == null)
            return null;
        try {
            /*
            id, name, popularity, duration_ms, explicit, artists, id_artists, release_date, danceability, energy, key, loudness, mode, speechiness, acousticness, instrumentalness, liveness, valence, tempo, time_signature
            id = 0
            danceability = 1, *
            energy = 2,  *
            speechiness = 3, *
            acousticness = 4, * 
            instrumentalness = 5, *
            liveness = 6, *
            valence = 7, *
            id2 = 8
            danceability2 = 9, *
            energy2 = 10,  *
            speechiness2 = 11, *
            acousticness2 = 12, * 
            instrumentalness2 = 13, *
            liveness2 = 14, *
            valence2 = 15, *
            */
            // First value
            float danceability = (float) input.get(8);
            float energy = (float) input.get(9);
            float speechiness = (float) input.get(13);
            float acousticness = (float) input.get(14);
            float instrumentalness = (float) input.get(15);
            float liveness = (float) input.get(16);
            float valence = (float) input.get(17);

            // Second value
            int second_tuple_offset = (int) input.size() / 2;
            
            float danceability2 = (float) input.get(8 + second_tuple_offset);
            float energy2 = (float) input.get(9 + second_tuple_offset);
            float speechiness2 = (float) input.get(13 + second_tuple_offset);
            double acousticness2 = (float) input.get(14 + second_tuple_offset);
            double instrumentalness2 = (float) input.get(15 + second_tuple_offset);
            double liveness2 = DataType.toDouble(input.get(16 + second_tuple_offset));
            double valence2 = DataType.toDouble(input.get(17 + second_tuple_offset));

            double similarity = Math.sqrt(
                pow2(danceability - danceability2) + pow2(energy - energy2) + pow2(speechiness - speechiness2) +
                pow2(acousticness - acousticness2) + pow2(instrumentalness - instrumentalness2) +
                pow2(liveness - liveness2) + pow2(valence - valence2)
            );

            return similarity;
        }

        catch (Exception e){
            throw new IOException("Caught exception processing input row ", e);
        }
    }
}