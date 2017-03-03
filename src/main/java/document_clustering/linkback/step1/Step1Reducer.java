package document_clustering.linkback.step1;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/**
 * join the simhash intermediate result and the mst result
 * <p>
 * Created by edwardlol on 2016/12/2.
 */
public class Step1Reducer extends Reducer<Step1KeyWritable, Text, IntWritable, Text> {
    //~ Instance fields --------------------------------------------------------

    private IntWritable outputKey = new IntWritable();

    //~ Methods ----------------------------------------------------------------

    /**
     * @param key     group_id, join_order
     * @param values  cluster_id or contents
     * @param context
     * @throws IOException
     * @throws InterruptedException
     */
    @Override
    public void reduce(Step1KeyWritable key, Iterable<Text> values, Context context)
            throws IOException, InterruptedException {

        for (Text value : values) {
            if (key.getTag().get() == 1) {
                /*
                    mst result, value = cluster_id
                 */
                this.outputKey.set(Integer.valueOf(value.toString()));
            } else {
                // cluster_id, entry_id@@g_no::g_name##g_model
                context.write(this.outputKey, value);
            }
        }
    }
}

// End Step1Reducer.java
