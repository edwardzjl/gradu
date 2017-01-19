package document_clustering.linkback;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

import java.io.IOException;

/**
 * Created by edwardlol on 2016/12/2.
 */
public class Step1Mapper extends Mapper<Text, Text, Step1KeyWritable, Text> {

    private Step1KeyWritable taggedKey = new Step1KeyWritable();

    private int joinOrder;

    //~ Methods ----------------------------------------------------------------

    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        FileSplit fileSplit = (FileSplit) context.getInputSplit();
        joinOrder = Integer.parseInt(context.getConfiguration().get(fileSplit.getPath().getName()));
    }

    /**
     * @param key     doc_id
     * @param value   group_id or content
     * @param context
     * @throws IOException
     * @throws InterruptedException
     */
    @Override
    public void map(Text key, Text value, Context context)
            throws IOException, InterruptedException {
        int joinKey = Integer.valueOf(key.toString());
        taggedKey.set(joinKey, joinOrder);
        context.write(taggedKey, value);
    }
}
// End Step1Mapper.java
