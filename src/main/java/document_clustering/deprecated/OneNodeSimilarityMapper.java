package document_clustering.deprecated;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * Created by edwardlol on 2016/12/2.
 */
public class OneNodeSimilarityMapper extends Mapper<LongWritable, Text, IntWritable, Text> {

    private final IntWritable outputKey = new IntWritable(1);

    //~ Methods ----------------------------------------------------------------

    @Override
    public void map(LongWritable key, Text value, Context context)
            throws IOException, InterruptedException {

        // group all documents together
        context.write(this.outputKey, value);
    }
}

// End OneNodeSimilarityMapper.java
