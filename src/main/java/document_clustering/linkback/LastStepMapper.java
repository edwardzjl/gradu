package document_clustering.linkback;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * Created by edwardlol on 2016/12/2.
 */
public class LastStepMapper extends Mapper<Text, Text, Text, Text> {

    private Text outputKey = new Text();

    private Text outputValue = new Text();

    //~ Methods ----------------------------------------------------------------

    /**
     * @param key     entry_id
     * @param value   group_id:g_name@@g_model
     * @param context
     * @throws IOException
     * @throws InterruptedException
     */
    @Override
    public void map(Text key, Text value, Context context)
            throws IOException, InterruptedException {

        String[] line = value.toString().split(":");

        outputKey.set(line[0]);
        outputValue.set(key.toString() + "::" + line[1]);
        context.write(outputKey, outputValue);
    }
}
// End LastStepMapper.java
