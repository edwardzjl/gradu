package document_clustering.linkback.step2;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

import java.io.IOException;

/**
 * Created by edwardlol on 2016/12/2.
 */
public class Step2Mapper extends Mapper<Text, Text, Step2KeyWritable, Text> {

    private Step2KeyWritable taggedKey = new Step2KeyWritable();

    private int joinOrder;

    //~ Methods ----------------------------------------------------------------

    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        FileSplit fileSplit = (FileSplit) context.getInputSplit();
        System.err.println(fileSplit.getPath().getParent().toString());
        joinOrder = Integer.parseInt(context.getConfiguration().get(fileSplit.getPath().getParent().toString()));
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

        taggedKey.set(key.toString(), joinOrder);
        context.write(taggedKey, value);
    }
}
// End Step1Mapper.java
