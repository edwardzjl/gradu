package document_clustering.linkback.pre;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * extract useful information from 8703's end file
 * <p>
 * Created by edwardlol on 2016/12/2.
 */
public class Process8703Mapper extends Mapper<Text, Text, Text, Text> {

    private Text outputKey = new Text();

    private Text outputValue = new Text();

    //~ Methods ----------------------------------------------------------------

    /**
     * @param key     entry_id
     * @param value   g_no \t code_ts \t g_name \t country \t g_name@@g_model
     * @param context
     * @throws IOException
     * @throws InterruptedException
     */
    @Override
    public void map(Text key, Text value, Context context)
            throws IOException, InterruptedException {

        String[] line = value.toString().split("\t");

        this.outputKey.set(key.toString() + "@@" + line[0]);
        this.outputValue.set(line[4]);

        // entry_id@@g_no \t g_name@@g_model
        context.write(outputKey, outputValue);
    }
}
// End Process8703Mapper.java
