package document_clustering.util;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * Created by edwardlol on 2016/12/5.
 */
public class LineCountMapper extends Mapper<LongWritable, Text, NullWritable, IntWritable> {
    //~ Instance fields --------------------------------------------------------

    private final IntWritable outputValue = new IntWritable(1);

    //~ Methods ----------------------------------------------------------------

    @Override
    protected void map(LongWritable key, Text value, Context context)
            throws IOException, InterruptedException {
        context.write(NullWritable.get(), this.outputValue);
    }
}

// End LineCountMapper.java
