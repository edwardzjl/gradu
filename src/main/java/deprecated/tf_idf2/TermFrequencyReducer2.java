package deprecated.tf_idf2;

import document_clustering.util.CollectionUtil;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

/**
 * Created by edwardlol on 2016/12/3.
 */
public class TermFrequencyReducer2 extends Reducer<Text, Text, Text, Text> {
    //~ Instance fields --------------------------------------------------------

    private Text outputKey = new Text();

    private Text outputValue = new Text();

    private double weight;

    private Map<String, Double> tempCounter = new HashMap<>();

    //~ Methods ----------------------------------------------------------------

    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        Configuration conf = context.getConfiguration();
        try {
            this.weight = conf.getDouble("gname.weight", 1.0d);
        } catch (NumberFormatException e) {
            e.printStackTrace();
        }
    }

    /**
     *
     * @param key     id@@g_no@@line_no
     * @param values  position::term=count
     * @param context
     * @throws IOException
     * @throws InterruptedException
     */
    @Override
    protected void reduce(Text key, Iterable<Text> values, Context context)
            throws IOException, InterruptedException {
        int sumOfWordsInDoc = 0;
        tempCounter.clear();

        for (Text val : values) {
            // positionTermCnt[0] = position
            // positionTermCnt[1] = term=count
            String[] positionTermCnt = val.toString().split("::");
            String place = positionTermCnt[0];

            String[] termCnt = positionTermCnt[1].split("=");

            int count = Integer.valueOf(termCnt[1]);
            sumOfWordsInDoc += count;

            double weightedCount = place.equals("title") ?
                    this.weight * count : count;

            // term : weight
            CollectionUtil.updateCountMap(tempCounter, termCnt[0], weightedCount);
        }

        for (Map.Entry<String, Double> entry : tempCounter.entrySet()) {
            // term
            this.outputKey.set(entry.getKey());
            // id@@g_no@@line_no=weight
            double wtf = entry.getValue() / sumOfWordsInDoc;
            this.outputValue.set(key.toString() + "=" + wtf);
            context.write(this.outputKey, this.outputValue);
        }
    }
}

// End TermFrequencyReducer.java
