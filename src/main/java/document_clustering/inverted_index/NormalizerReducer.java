package document_clustering.inverted_index;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

/**
 * Created by edwardlol on 2016/12/3.
 */
public class NormalizerReducer extends Reducer<Text, Text, Text, Text> {
    //~ Instance fields --------------------------------------------------------

    private Text outputKey = new Text();

    private Text outputValue = new Text();

    private Map<String, Double> tf_idfs = new HashMap<>();

    //~ Methods ----------------------------------------------------------------

    /**
     * @param key     entry_id@@g_no@@group_id
     * @param values  term=tf_idf
     * @param context
     * @throws IOException
     * @throws InterruptedException
     */
    @Override
    protected void reduce(Text key, Iterable<Text> values, Context context)
            throws IOException, InterruptedException {

        this.tf_idfs.clear();
        for (Text value : values) {
            String[] termAndTF_IDF = value.toString().split("=");
            this.tf_idfs.put(termAndTF_IDF[0], Double.valueOf(termAndTF_IDF[1]));
        }
        double sum = 0.0d;
        for (double tf_idf : this.tf_idfs.values()) {
            sum += Math.pow(tf_idf, 2);
        }
        final double sq_sum = Math.sqrt(sum);

        this.tf_idfs.replaceAll((k, v) -> v / sq_sum);

        for (Map.Entry<String, Double> entry : this.tf_idfs.entrySet()) {
            String[] id = key.toString().split("@@");

            this.outputKey.set(entry.getKey());
            this.outputValue.set(id[2] + "=" + entry.getValue().toString());
            // term \t group_id=normalized_tf_idf
            context.write(this.outputKey, this.outputValue);
        }
    }
}

// End NormalizerReducer.java
