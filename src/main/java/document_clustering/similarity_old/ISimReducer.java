package document_clustering.similarity_old;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.HashSet;
import java.util.Set;

/**
 * Created by edwardlol on 2016/12/2.
 */
public class ISimReducer extends Reducer<Text, Text, Text, DoubleWritable> {

    private DoubleWritable outputValue = new DoubleWritable();

    private Set<Integer> containedTerms = new HashSet<>();

    //~ Methods ----------------------------------------------------------------

    /**
     * @param key     docId1,docId2
     * @param values  termId:sim
     * @param context
     * @throws IOException
     * @throws InterruptedException
     */
    @Override
    public void reduce(Text key, Iterable<Text> values,
                       Context context) throws IOException, InterruptedException {
        this.containedTerms.clear();
        double sim = 0.0d;
        for (Text value : values) {
            String[] line = value.toString().split(":");
            int termId = Integer.valueOf(line[0]);
            if (this.containedTerms.add(termId)) {
                sim += Double.valueOf(line[1]);
            }
        }
        if (sim > 0.05d) {
            this.outputValue.set(Math.abs(1.0d - sim));
            // docId1,docId2 \t sim
            context.write(key, this.outputValue);
        }
    }
}

// End InvertedSimilarityReducer.java