package document_clustering.inverted_index;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.text.DecimalFormat;
import java.util.Iterator;

/**
 * the reducer class for calculating inverted index
 * to combine all inverted indexes together
 * <p>
 * Created by edwardlol on 2016/11/25.
 */
public class InvertedIndexReducer extends Reducer<Text, Text, Text, Text> {
    //~ Instance fields --------------------------------------------------------

    private Text outputValue = new Text();

    private StringBuilder stringBuilder = new StringBuilder();

    private DecimalFormat decimalFormat = new DecimalFormat( "0.0000");

    //~ Methods ----------------------------------------------------------------

    /**
     * @param key     term
     * @param values  group_id=tf-idf
     * @param context
     * @throws IOException
     * @throws InterruptedException
     */
    @Override
    public void reduce(Text key, Iterable<Text> values, Context context)
            throws IOException, InterruptedException {

        this.stringBuilder.setLength(0);

        for (Text value : values) {
            String[] idAndTFIDF = value.toString().split("=");
            double tf_idf = Double.valueOf(idAndTFIDF[1]);
            // filter the small tf_idfs
            if (tf_idf > 0.1d) {
                this.stringBuilder.append(idAndTFIDF[0]).append('=')
                        .append(this.decimalFormat.format(tf_idf)).append(',');
            }
        }
        this.stringBuilder.deleteCharAt(this.stringBuilder.length() - 1);

        if (this.stringBuilder.length() > 1) {
            this.outputValue.set(this.stringBuilder.toString());
            // term \t group_id=tf-idf,...
            context.write(key, this.outputValue);
        }
    }
}

// End InvertedIndexReducer.java
