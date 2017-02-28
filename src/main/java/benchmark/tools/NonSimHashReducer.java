package benchmark.tools;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Created by edwardlol on 2016/12/2.
 */
public class NonSimHashReducer extends Reducer<NullWritable, Text, IntWritable, Text> {
    //~ Static fields/initializers ---------------------------------------------

    /**
     * used to initialize ids
     */
    protected static final AtomicInteger count = new AtomicInteger(0);

    //~ Instance fields --------------------------------------------------------

    private IntWritable outputKey = new IntWritable();

    //~ Methods ----------------------------------------------------------------

    /**
     * @param key     docId1,docId2
     * @param values  termId:sim
     * @param context
     * @throws IOException
     * @throws InterruptedException
     */
    @Override
    public void reduce(NullWritable key, Iterable<Text> values,
                       Context context) throws IOException, InterruptedException {

        for (Text value : values) {
            this.outputKey.set(count.incrementAndGet());

            context.write(this.outputKey, value);
        }
    }
}

// End InvertedSimilarityReducer.java
