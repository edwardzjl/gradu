package document_clustering.mst;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;

/**
 * Created by edwardlol on 2016/12/2.
 */
public class MSTChildMapper extends Mapper<Text, Text, DoubleWritable, Text> {

    private DoubleWritable outputKey = new DoubleWritable();

    private Text outputValue = new Text();

    private int docCnt;

    private int docsInSegment;

    //~ Methods ----------------------------------------------------------------

    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        if (context.getCacheFiles() != null
                && context.getCacheFiles().length > 0) {

            FileReader fileReader = new FileReader("./docCnt");
            BufferedReader bufferedReader = new BufferedReader(fileReader);

            this.docCnt = Integer.valueOf(bufferedReader.readLine());

            bufferedReader.close();
            fileReader.close();
        }
        Configuration conf = context.getConfiguration();
        int reduceTaskNum = conf.getInt("reduce.task.num", 1);
        this.docsInSegment = this.docCnt / reduceTaskNum;
    }

    /**
     * @param key     doc_id1,doc_id2
     * @param value   similarity
     * @param context
     * @throws IOException
     * @throws InterruptedException
     */
    @Override
    public void map(Text key, Text value, Context context)
            throws IOException, InterruptedException {

        String srcDestPair = key.toString();
        String[] srcDest = srcDestPair.split(",");

        int srcId = Integer.valueOf(srcDest[0]);
        int destId = Integer.valueOf(srcDest[1]);

        int container1 = (srcId - 1) / docsInSegment;
        int container2 = (destId - 1) / docsInSegment;

        // get the weight
        double weight = Double.valueOf(value.toString());
        this.outputKey.set(weight);

        if (container1 != container2
                && ((destId - 1) % docsInSegment > (docsInSegment / 2))) {
//                || destId % docsInSegment == 0)) {
            this.outputValue.set(srcDestPair + ":" + container2);
        } else {
            this.outputValue.set(srcDestPair + ":" + container1);
        }
        // weight \t (src,dest):containder_id
        context.write(outputKey, this.outputValue);
    }
}
// End MSTChildMapper.java
