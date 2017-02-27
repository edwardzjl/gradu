package writables;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * Created by edwardlol on 2017/2/27.
 */
public class WritableMapper extends Mapper<LongWritable, Text, Text, Text> {
    //~ Static fields/initializers ---------------------------------------------

    //~ Instance fields --------------------------------------------------------

    //~ Constructors -----------------------------------------------------------

    //~ Methods ----------------------------------------------------------------


    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String[] tuple = value.toString().replaceAll("\\(", "").replaceAll("\\)", "").split(",");
        context.write(new Text(tuple[0]), new Text(tuple[1]));
    }
}

// End WritableMapper.java
