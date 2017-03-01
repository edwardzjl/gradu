package document_clustering.similarity;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * Created by edwardlol on 2016/12/2.
 */
public class ISimMapper extends Mapper<Text, Text, Text, DoubleWritable> {

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

        this.outputValue.set(Double.valueOf(value.toString()));
        context.write(key, this.outputValue);
    }
}

// End ISimMapper.java
