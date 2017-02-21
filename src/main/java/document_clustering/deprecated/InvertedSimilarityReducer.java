package document_clustering.deprecated;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/**
 * Created by edwardlol on 2016/12/2.
 */
public class InvertedSimilarityReducer extends Reducer<Text, DoubleWritable,
        Text, DoubleWritable> {

    private DoubleWritable outputValue = new DoubleWritable();

    private double outputThreshold;

    //~ Methods ----------------------------------------------------------------

    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        Configuration conf = context.getConfiguration();
        this.outputThreshold = conf.getDouble("output.threshold", 0.05d);
    }

    @Override
    public void reduce(Text key, Iterable<DoubleWritable> values,
                       Context context) throws IOException, InterruptedException {

        double sum = 0.0d;
        for (DoubleWritable value : values) {
            sum += value.get();
        }

        if (sum > this.outputThreshold) {
            this.outputValue.set(Math.abs(1.0d - sum));
            context.write(key, this.outputValue);
        }
    }
}

// End InvertedSimilarityReducer.java
