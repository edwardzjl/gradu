package document_clustering.mst;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * Created by edwardlol on 2016/12/2.
 */
public class MSTFinalMapper2 extends Mapper<Text, Text, DoubleWritable, Text> {
    //~ Instance fields --------------------------------------------------------

    private DoubleWritable outputKey = new DoubleWritable();

    //~ Methods ----------------------------------------------------------------

    /**
     *
     * @param key     doc_id1,doc_id2
     * @param value   similarity
     * @param context
     * @throws IOException
     * @throws InterruptedException
     */
    @Override
    public void map(Text key, Text value, Context context)
            throws IOException, InterruptedException {

        this.outputKey.set(Double.valueOf(value.toString()));
        context.write(this.outputKey, key);
    }
}
// End MSTFinalMapper.java
