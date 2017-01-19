package document_clustering.linkback.bas0901;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * Created by edwardlol on 2016/12/2.
 */
public class Process0901Mapper extends Mapper<LongWritable, Text, Text, Text> {

    private Text outputKey = new Text();

    private Text outputValue = new Text();

    //~ Methods ----------------------------------------------------------------

    /**
     * @param key     position
     * @param value   entry_id@@g_no@@code_ts@@country@@g_name@@g_model
     * @param context
     * @throws IOException
     * @throws InterruptedException
     */
    @Override
    public void map(LongWritable key, Text value, Context context)
            throws IOException, InterruptedException {

        String[] line = value.toString().split("@@");

        outputKey.set(line[0] + "@@" + line[1]);

        String output = line[4] + "@@";
        if (line.length == 6) {
            output += line[5];
        }
        outputValue.set(output);
        context.write(outputKey, outputValue);
    }
}
// End Process0901Mapper.java
