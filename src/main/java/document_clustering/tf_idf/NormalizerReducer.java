package document_clustering.tf_idf;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

/**
 * D = total number of documents in corpus. This can be passed by the driver as a constant;
 * d = number of documents in corpus where the term appears. It is a counter over the reduced values for each term;
 * TFIDF = n/N * log(D/d);
 * Output: ((word@document), d/D, (n/N), TFIDF)
 * <p>
 * Created by edwardlol on 2016/12/3.
 */
public class NormalizerReducer extends Reducer<Text, Text, Text, Text> {
    //~ Instance fields --------------------------------------------------------

    private Text outputKey = new Text();

    private Text outputValue = new Text();

    private Map<String, Double> tf_idfs = new HashMap<>();

    //~ Methods ----------------------------------------------------------------

    /**
     * @param key     id@@g_no@@line_no
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
        final double _sum = Math.sqrt(sum);

        this.tf_idfs.replaceAll((k, v) -> v / _sum);

        for (Map.Entry<String, Double> entry : this.tf_idfs.entrySet()) {
            String term = entry.getKey();
            Double tf_idf = entry.getValue();
            this.outputKey.set(term + "@@@" + key.toString());
            this.outputValue.set(tf_idf.toString());
            context.write(this.outputKey, this.outputValue);
        }
    }
}

// End NormalizerReducer.java
