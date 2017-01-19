package document_clustering.deprecated;

import document_clustering.writables.tuple_writables.IntIntTupleWritable;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * Created by edwardlol on 2016/12/2.
 */
public class MergeMapper extends Mapper<LongWritable, Text, IntIntTupleWritable, DoubleWritable> {

    private IntIntTupleWritable outputKey = new IntIntTupleWritable();

    private DoubleWritable outputValue = new DoubleWritable();

    //~ Methods ----------------------------------------------------------------

    @Override
    public void map(LongWritable key, Text value, Context context)
            throws IOException, InterruptedException {

        String[] line = value.toString().split("\t");

        String srcDest = line[0];
        double similarity = Double.parseDouble(line[1]);

        String[] _srcDest = srcDest.split(":");
        int src = Integer.valueOf(_srcDest[0]);
        int dest = Integer.valueOf(_srcDest[1]);

        this.outputKey.set(src, dest);
        this.outputValue.set(similarity);
        context.write(this.outputKey, this.outputValue);
    }
}

// End SimilarityMapper.java
