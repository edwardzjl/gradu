package document_clustering.linkback.pre;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * extract useful information from 0901's end file
 * <p>
 * Created by edwardlol on 2016/12/2.
 */
public class Process0901Mapper extends Mapper<LongWritable, Text, Text, Text> {

    private Text outputKey = new Text();

    private Text outputValue = new Text();

    //~ Methods ----------------------------------------------------------------

    /**
     * @param key     line offset
     * @param value   entry_id@@g_no@@code_ts@@country@@g_name@@g_model
     * @param context
     * @throws IOException
     * @throws InterruptedException
     */
    @Override
    public void map(LongWritable key, Text value, Context context)
            throws IOException, InterruptedException {

        String[] line = value.toString().split("@@");

        this.outputKey.set(line[0] + "@@" + line[1]);

        String output = line[4] + "@@";
        if (line.length == 6) {
            output += line[5];
        }
        this.outputValue.set(output);
        // entry_id@@g_no \t g_name@@g_model
        context.write(this.outputKey, this.outputValue);
    }
}
// End Process0901Mapper.java
