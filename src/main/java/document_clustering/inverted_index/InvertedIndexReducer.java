package document_clustering.inverted_index;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.text.DecimalFormat;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * the reducer class for calculating inverted index
 * to combine all inverted indexes together
 * <p>
 * Created by edwardlol on 2016/11/25.
 */
public class InvertedIndexReducer extends Reducer<Text, Text, Text, Text> {
    //~ Static fields/initializers ---------------------------------------------

    /**
     * used to initialize term ids
     */
    protected static final AtomicInteger count = new AtomicInteger(0);

    //~ Instance fields --------------------------------------------------------

    private Text outputKey = new Text();

    private Text outputValue = new Text();

    private StringBuilder stringBuilder = new StringBuilder();

    private DecimalFormat decimalFormat;

    //~ Methods ----------------------------------------------------------------

    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        Configuration conf = context.getConfiguration();
        int deci_num = conf.getInt("deci.number", 3);
        StringBuilder stringBuilder = new StringBuilder();
        stringBuilder.append("0.");
        for (int i = 0; i < deci_num; i++) {
            stringBuilder.append('0');
        }
        this.decimalFormat = new DecimalFormat(stringBuilder.toString());
    }

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

        int id = count.incrementAndGet();

        /* reset stringBuilder */
        this.stringBuilder.setLength(0);

        /* append the index of this term */
        for (Text value : values) {
            String[] idAndTFIDF = value.toString().split("=");
            double tf_idf = Double.valueOf(idAndTFIDF[1]);
            /* filter the small tf_idfs
            which contributes little to the final similarity */
//            if (tf_idf > 0.1d) {
                this.stringBuilder.append(idAndTFIDF[0]).append('=')
                        .append(this.decimalFormat.format(tf_idf)).append(',');
//            }
        }
        try {
            this.stringBuilder.deleteCharAt(this.stringBuilder.length() - 1);
        } catch (RuntimeException e) {
            System.out.println(this.stringBuilder.toString());
        }


        /* output the terms whose index is bigger than 1
         * it means this term occurs in at least 2 documents
         * and only in this way does this term contribute to the doc similarity */
//        if (this.stringBuilder.length() > 1) {
//            this.outputKey.set(id + ":" + key.toString());
//            this.outputValue.set(this.stringBuilder.toString());
//            // term \t group_id=tf-idf,...
//            context.write(key, this.outputValue);
//        }

        this.outputKey.set(id + ":" + key.toString());
        this.outputValue.set(this.stringBuilder.toString());
        // term \t group_id=tf-idf,...
        context.write(this.outputKey, this.outputValue);
    }
}

// End InvertedIndexReducer.java
