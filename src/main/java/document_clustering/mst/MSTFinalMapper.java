package document_clustering.mst;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * Created by edwardlol on 2016/12/2.
 */
public class MSTFinalMapper extends Mapper<Text, Text, DoubleWritable, Text> {
    //~ Instance fields --------------------------------------------------------

    private DoubleWritable outputKey = new DoubleWritable();

    //~ Methods ----------------------------------------------------------------

    /**
     *
     * @param key     similarity
     * @param value   doc_id1,doc_id2
     * @param context
     * @throws IOException
     * @throws InterruptedException
     */
    @Override
    public void map(Text key, Text value, Context context)
            throws IOException, InterruptedException {

        this.outputKey.set(Double.valueOf(key.toString()));
        context.write(this.outputKey, value);
    }
}
// End MSTFinalMapper.java
