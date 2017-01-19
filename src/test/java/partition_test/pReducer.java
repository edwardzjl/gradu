package partition_test;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/**
 * Created by edwardlol on 2016/12/9.
 */
public class pReducer extends Reducer<Text, Text, Text, IntWritable> {
    //~ Static fields/initializers ---------------------------------------------

    //~ Instance fields --------------------------------------------------------
    private int max = 0;
    //~ Constructors -----------------------------------------------------------

    //~ Methods ----------------------------------------------------------------


    @Override
    protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {

        for (Text value : values) {
            String[] str = value.toString().split("\t", -3);
            if (Integer.parseInt(str[4]) > max) {
                max = Integer.parseInt(str[4]);
            }
        }

        context.write(new Text(key), new IntWritable(max));
    }
}

// End pReducer.java
