package document_clustering.similarity;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/**
 * Created by edwardlol on 2016/12/2.
 */
public class InvertedSimilarityCombiner extends Reducer<Text, DoubleWritable,
        Text, DoubleWritable> {

    private DoubleWritable outputValue = new DoubleWritable();

    //~ Methods ----------------------------------------------------------------

    @Override
    public void reduce(Text key, Iterable<DoubleWritable> values,
                       Context context) throws IOException, InterruptedException {

        double sum = 0.0d;
        for (DoubleWritable value : values) {
            sum += value.get();
        }

        this.outputValue.set(sum);
        context.write(key, this.outputValue);
    }
}

// End InvertedSimilarityReducer.java
