package benchmark.tools;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * Created by edwardlol on 2016/12/3.
 */
public class NonSimHashMapper extends Mapper<LongWritable, Text, NullWritable, Text> {

    //~ Instance fields --------------------------------------------------------

    private final NullWritable outputKey = NullWritable.get();

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

        this.outputValue.set(value.toString().replaceAll("\t", "::"));

        context.write(this.outputKey, this.outputValue);

    }
}

// End SimHashStep1Mapper.java
