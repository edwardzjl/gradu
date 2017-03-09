package document_clustering.tf_idf;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/**
 * Created by edwardlol on 2016/12/3.
 */
public class TermCountReducer extends Reducer<Text, IntWritable, Text, IntWritable> {
    //~ Instance fields --------------------------------------------------------

    private IntWritable outputValue = new IntWritable();

    //~ Methods ----------------------------------------------------------------

    /**
     * @param key     term@@@entry_id@@g_no@@group_id::position
     * @param values  count
     * @param context
     * @throws IOException
     * @throws InterruptedException
     */
    @Override
    protected void reduce(Text key, Iterable<IntWritable> values, Context context)
            throws IOException, InterruptedException {

        int sum = 0;
        for (IntWritable val : values) {
            sum += val.get();
        }
        this.outputValue.set(sum);
        context.write(key, this.outputValue);
    }
}

// End TermCountReducer.java
