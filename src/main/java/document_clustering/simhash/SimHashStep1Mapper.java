package document_clustering.simhash;

import document_clustering.util.SimHash;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * Created by edwardlol on 2016/12/3.
 */
public class SimHashStep1Mapper extends Mapper<Text, Text, LongWritable, Text> {
    //~ Instance fields --------------------------------------------------------

    private LongWritable outputKey = new LongWritable();

    private Text outputValue = new Text();

    //~ Methods ----------------------------------------------------------------

    /**
     * @param key     entry_id@@g_no
     * @param value   g_name##g_model
     * @param context
     * @throws IOException
     * @throws InterruptedException
     */
    @Override
    protected void map(Text key, Text value, Context context)
            throws IOException, InterruptedException {

        String nameAndModel = value.toString();

        SimHash thisHash = new SimHash(nameAndModel.replace("##", " "));

        this.outputKey.set(thisHash.getHashCode());
        this.outputValue.set(key.toString() + "::" + nameAndModel);
        // simhash \t entry_id@@g_no::g_name##g_model
        context.write(this.outputKey, this.outputValue);
    }
}

// End SimHashStep1Mapper.java
