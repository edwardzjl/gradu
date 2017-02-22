package document_clustering.tf_idf;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * Created by edwardlol on 2016/12/3.
 */
public class TermFrequencyMapper extends Mapper<Text, Text, Text, Text> {
    //~ Instance fields --------------------------------------------------------

    private Text outputKey = new Text();

    private Text outputValue = new Text();

    //~ Methods ----------------------------------------------------------------

    /**
     * @param key     term@@@entry_id@@g_no@@group_id::position
     * @param value   count
     * @param context
     * @throws IOException
     * @throws InterruptedException
     */
    @Override
    public void map(Text key, Text value, Context context)
            throws IOException, InterruptedException {

        // termAndDoc[0] = term
        // termAndDoc[1] = entry_id@@g_no@@group_id::position
        String[] termAndDoc = key.toString().split("@@@");

        // idAndPlace[0] = entry_id@@g_no@@group_id
        // idAndPlace[1] = position
        String[] idAndPlace = termAndDoc[1].split("::");

        this.outputKey.set(idAndPlace[0]);
        this.outputValue.set(idAndPlace[1] + "::" + termAndDoc[0] + "=" + value.toString());
        // entry_id@@g_no@@group_id \t position::term=count
        context.write(this.outputKey, this.outputValue);
    }
}

// End TermFrequencyMapper.java
