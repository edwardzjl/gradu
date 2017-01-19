package deprecated;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * Created by edwardlol on 2016/12/2.
 */
public class ShowResultMapper extends Mapper<Text, Text, Text, Text> {

//    private Text outputKey = new Text();

//    private Text outputValue = new Text();

    //~ Methods ----------------------------------------------------------------

    /**
     *
     * @param key     doc_id
     * @param value   group_id
     * @param context
     * @throws IOException
     * @throws InterruptedException
     */
    @Override
    public void map(Text key, Text value, Context context)
            throws IOException, InterruptedException {

//        String[] inputTokens = value.toString().split("\t");

//        outputKey.set(inputTokens[1]);
//        outputValue.set(inputTokens[0]);

        context.write(value, key);
    }
}
// End Step1Mapper.java
