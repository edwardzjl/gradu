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

        Iterator<Text> iterator = values.iterator();
        StringBuilder stringBuilder = new StringBuilder();
//        int elemNum = 0;
        while (iterator.hasNext()) {
            String value = iterator.next().toString();
            String[] lnoAndTFIDF = value.split("=");
            if (Double.valueOf(lnoAndTFIDF[1]) > 0.1d) {
//                elemNum++;
                stringBuilder.append(value).append(',');
//                if (iterator.hasNext()) {
//                    stringBuilder.append(",");
//                }
            }
        }
        stringBuilder.deleteCharAt(stringBuilder.length() - 1);
//        if (elemNum > 1) {
        if (stringBuilder.length() > 1) {
            this.outputValue.set(stringBuilder.toString());
            // term \t line_no=tf-idf,...
            context.write(key, this.outputValue);
        }
    }
}

// End InvertedIndexReducer.java
