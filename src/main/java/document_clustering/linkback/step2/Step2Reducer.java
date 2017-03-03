package document_clustering.linkback.step2;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/**
 * Created by edwardlol on 2016/12/2.
 */
public class Step2Reducer extends Reducer<Step2KeyWritable, Text, Text, Text> {
    //~ Instance fields --------------------------------------------------------

    private Text outputKey = new Text();

    private Text outputValue = new Text();

    //~ Methods ----------------------------------------------------------------

    /**
     * @param key     entry id and join order
     * @param values  doc_id or contents
     * @param context
     * @throws IOException
     * @throws InterruptedException
     */
    @Override
    public void reduce(Step2KeyWritable key, Iterable<Text> values, Context context)
            throws IOException, InterruptedException {
        String groupId = "";
        outputKey.set(key.getJoinKey());
        for (Text value : values) {
            if (key.getTag().get() == 1) {
                groupId = value.toString();
            } else {
                outputValue.set(groupId + ":" + value.toString());
                context.write(outputKey, outputValue);
            }
        }
    }
}

// End Step1Reducer.java
