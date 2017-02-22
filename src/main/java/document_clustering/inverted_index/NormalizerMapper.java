package document_clustering.inverted_index;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * Created by edwardlol on 2016/12/3.
 */
public class NormalizerMapper extends Mapper<Text, Text, Text, Text> {
    //~ Instance fields --------------------------------------------------------

    private Text outputKey = new Text();

    private Text outputValue = new Text();

    //~ Methods ----------------------------------------------------------------

    /**
     * @param key     term@@@entry_id@@g_no@@group_id
     * @param value   tf_idf
     * @param context
     * @throws IOException
     * @throws InterruptedException
     */
    @Override
    public void map(Text key, Text value, Context context)
            throws IOException, InterruptedException {

        String[] termAndId = key.toString().split("@@@");

        double tf_idf = Double.valueOf(value.toString());

        // filter stop words
//        if (tf_idf > 0.01) {
        this.outputKey.set(termAndId[1]);
        this.outputValue.set(termAndId[0] + "=" + tf_idf);
        // entry_id@@g_no@@group_id \t term=tf_idf
        context.write(this.outputKey, this.outputValue);
//        }
    }
}

// End NormalizerMapper.java
