package document_clustering.linkback;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/**
 * Created by edwardlol on 2016/12/2.
 */
public class Step1Reducer extends Reducer<Step1KeyWritable, Text, IntWritable, Text> {
    //~ Instance fields --------------------------------------------------------

    private IntWritable outputKey = new IntWritable();

    //~ Methods ----------------------------------------------------------------

    /**
     * @param key     group_id
     * @param values  doc_id or contents
     * @param context
     * @throws IOException
     * @throws InterruptedException
     */
    @Override
    public void reduce(Step1KeyWritable key, Iterable<Text> values, Context context)
            throws IOException, InterruptedException {

        for (Text value : values) {
            if (key.getTag().get() == 1) {
                outputKey.set(Integer.valueOf(value.toString()));
            } else {
                context.write(outputKey, value);
            }
        }
    }
}

// End Step1Reducer.java
