package document_clustering.linkback;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * Created by edwardlol on 2016/12/2.
 */
public class ProcessClusterMapper extends Mapper<Text, Text, Text, Text> {

    private Text outputKey = new Text();

    //~ Methods ----------------------------------------------------------------

    /**
     * @param key     group_id
     * @param value   entry_id@@g_no::g_name##g_model
     * @param context
     * @throws IOException
     * @throws InterruptedException
     */
    @Override
    public void map(Text key, Text value, Context context)
            throws IOException, InterruptedException {

        String[] line = value.toString().split("::");

        outputKey.set(line[0]);
        context.write(outputKey, key);
    }
}
// End Step1Mapper.java
