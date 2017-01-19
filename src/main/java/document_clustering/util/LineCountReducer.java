package document_clustering.util;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/**
 * Created by edwardlol on 2016/12/5.
 */
public class LineCountReducer extends Reducer<NullWritable, IntWritable, NullWritable, IntWritable> {
    //~ Instance fields --------------------------------------------------------

    private IntWritable outputValue = new IntWritable();

    private int counter = 0;

    //~ Methods ----------------------------------------------------------------

    @Override
    protected void reduce(NullWritable key, Iterable<IntWritable> values, Context context)
            throws IOException, InterruptedException {
        for (IntWritable value : values) {
            this.counter += value.get();
        }
        this.outputValue.set(this.counter);
        context.write(NullWritable.get(), this.outputValue);
    }
}

// End LineCountReducer.java
