package deprecated;

import document_clustering.util.CollectionUtil;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

/**
 * Created by edwardlol on 2016/12/3.
 */
public class TermFrequencyReducer extends Reducer<Text, Text, Text, Text> {
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
        this.tempCounter.clear();

        for (Text val : values) {
            // positionTermCnt[0] = position
            // positionTermCnt[1] = term=count
            String[] positionTermCnt = val.toString().split("::");
            String position = positionTermCnt[0];

            String[] termCnt = positionTermCnt[1].split("=");

            int count = Integer.valueOf(termCnt[1]);
            sumOfWordsInDoc += count;

            double weightedCount = position.equals("title") ?
                    this.weight * count : count;

            // term : weight
            CollectionUtil.updateCountMap(this.tempCounter, termCnt[0], weightedCount);
        }

        for (Map.Entry<String, Double> entry : this.tempCounter.entrySet()) {
            // term@@@id@@g_no@@line_no
            this.outputKey.set(entry.getKey() + "@@@" + key.toString());
            // weight / sumOfWordsInDoc
            this.outputValue.set(entry.getValue() + "/" + sumOfWordsInDoc);
            context.write(this.outputKey, this.outputValue);
        }
    }
}

// End TermFrequencyReducer.java
