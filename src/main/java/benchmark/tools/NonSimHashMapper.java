package benchmark.tools;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Created by edwardlol on 2016/12/3.
 */
public class NonSimHashMapper extends Mapper<LongWritable, Text, IntWritable, Text> {
    //~ Static fields/initializers ---------------------------------------------

    /**
     * used to initialize ids
     */
    protected static final AtomicInteger count = new AtomicInteger(0);

    //~ Instance fields --------------------------------------------------------

    private IntWritable outputKey = new IntWritable();

    private Text outputValue = new Text();

    //~ Methods ----------------------------------------------------------------

    /**
     * @param key     offset of every line
     * @param value   entry_id@@g_no \t g_name##g_model
     * @param context
     * @throws IOException
     * @throws InterruptedException
     */
    @Override
    protected void map(LongWritable key, Text value, Context context)
            throws IOException, InterruptedException {

        this.outputKey.set(count.incrementAndGet());
        this.outputValue.set(value.toString().replaceAll("\t", "::"));

        context.write(this.outputKey, this.outputValue);
    }
}

// End SimHashStep1Mapper.java
