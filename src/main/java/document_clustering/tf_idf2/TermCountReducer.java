package document_clustering.tf_idf2;

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
     *
     * @param key     term@@@id@@g_no@@line_no::content
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
        //write the key and the adjusted value (removing the last comma)
        this.outputValue.set(sum);
        context.write(key, this.outputValue);
    }
}

// End TermCountReducer.java
