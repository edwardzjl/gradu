package document_clustering.inverted_index;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
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

    //~ Methods ----------------------------------------------------------------

    /**
     * @param key     term
     * @param values  line_no=tf-idf
     * @param context
     * @throws IOException
     * @throws InterruptedException
     */
    @Override
    public void reduce(Text key, Iterable<Text> values, Context context)
            throws IOException, InterruptedException {

        this.stringBuilder.setLength(0);

        for (Text value : values) {
            String[] lnoAndTFIDF = value.toString().split("=");
            if (Double.valueOf(lnoAndTFIDF[1]) > 0.1d) {
                this.stringBuilder.append(value).append(',');
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
