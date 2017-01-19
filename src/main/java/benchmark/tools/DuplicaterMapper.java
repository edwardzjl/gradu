package benchmark.tools;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * Created by edwardlol on 2016/12/24.
 */
public class DuplicaterMapper extends Mapper<Text, Text, IntWritable, Text> {
    //~ Instance fields --------------------------------------------------------

    private IntWritable outputKey = new IntWritable();

    //~ Methods ----------------------------------------------------------------

    @Override
    protected void map(Text key, Text value, Context context) throws IOException, InterruptedException {
        int id = Integer.valueOf(key.toString());

        for (int i = 0; i < 200; i++) {
            this.outputKey.set(id + i * 50000);
            context.write(this.outputKey, value);
        }
    }
}

// End DuplicaterMapper.java
