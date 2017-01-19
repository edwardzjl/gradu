package document_clustering.simhash;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * Created by edwardlol on 2016/12/3.
 */
public class SimHashStep2Mapper extends Mapper<Text, Text, IntWritable, Text> {
    //~ Instance fields --------------------------------------------------------

    private IntWritable outputKey = new IntWritable();

    //~ Methods ----------------------------------------------------------------

    /**
     * @param key     id
     * @param value   entry_id@@g_no::g_name##g_model
     * @param context
     * @throws IOException
     * @throws InterruptedException
     */
    @Override
    protected void map(Text key, Text value, Context context)
            throws IOException, InterruptedException {

        this.outputKey.set(Integer.valueOf(key.toString()));
        context.write(this.outputKey, value);
    }
}

// End SimHashStep1Mapper.java
