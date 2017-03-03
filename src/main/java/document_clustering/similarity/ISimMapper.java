package document_clustering.similarity;

import document_clustering.writables.tuple_writables.IntIntTupleWritable;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * Created by edwardlol on 2016/12/2.
 */
public class ISimMapper extends Mapper<Text, Text, IntIntTupleWritable, DoubleWritable> {

    private IntIntTupleWritable outputKey = new IntIntTupleWritable();

    private DoubleWritable outputValue = new DoubleWritable();

    //~ Methods ----------------------------------------------------------------

    /**
     * @param key     src,dest
     * @param value   sim
     * @param context
     * @throws IOException
     * @throws InterruptedException
     */
    @Override
    public void map(Text key, Text value, Context context)
            throws IOException, InterruptedException {

        String[] pair = key.toString().split(",");
        this.outputKey.set(Integer.valueOf(pair[0]), Integer.valueOf(pair[1]));
        this.outputValue.set(Double.valueOf(value.toString()));
        // wrap input and write to reducer
        context.write(this.outputKey, this.outputValue);
    }
}

// End ISimMapper.java
