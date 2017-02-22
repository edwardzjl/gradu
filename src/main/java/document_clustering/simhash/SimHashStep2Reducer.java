package document_clustering.simhash;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/**
 * take one comodity out of every group
 * <p>
 * Created by edwardlol on 2016/12/3.
 */
public class SimHashStep2Reducer extends Reducer<IntWritable, Text, IntWritable, Text> {

    //~ Methods ----------------------------------------------------------------

    /**
     * @param key     id
     * @param values  entry_id@@g_no::g_name##g_model
     * @param context
     * @throws IOException
     * @throws InterruptedException
     */
    @Override
    protected void reduce(IntWritable key, Iterable<Text> values, Context context)
            throws IOException, InterruptedException {

        Text main = values.iterator().next();
        // id, entry_id@@g_no::g_name##g_model
        context.write(key, main);
    }
}

// End SimHashStep2Reducer.java
