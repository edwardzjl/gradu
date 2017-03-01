package document_clustering.similarity;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/**
 * Created by edwardlol on 2016/12/2.
 */
public class ISimCombiner extends Reducer<Text, DoubleWritable, Text, DoubleWritable> {

    private DoubleWritable outputValue = new DoubleWritable();

    //~ Methods ----------------------------------------------------------------

    /**
     * @param key     docId1,docId2
     * @param values  sim
     * @param context
     * @throws IOException
     * @throws InterruptedException
     */
    @Override
    public void reduce(Text key, Iterable<DoubleWritable> values,
                       Context context) throws IOException, InterruptedException {
        double sim = 0.0d;
        for (DoubleWritable value : values) {
            sim += value.get();
        }
        if (sim > 0.001d) {
            this.outputValue.set(sim);
            // docId1,docId2 \t sim
            context.write(key, this.outputValue);
        }
    }
}

// End InvertedSimilarityReducer.java