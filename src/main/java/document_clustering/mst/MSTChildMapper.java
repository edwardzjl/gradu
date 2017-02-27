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

    private int docsInSeg;

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

        int reduceTaskNum = conf.getInt("reduce.task.num", 3);
        this.docsInSeg = this.docCnt / reduceTaskNum;
        if (this.docCnt % reduceTaskNum != 0) {
            this.docsInSeg++;
        }
    }

    /**
     * @param key     group_id1,group_id2
     * @param value   similarity
     * @param context
     * @throws IOException
     * @throws InterruptedException
     */
    @Override
    public void map(Text key, Text value, Context context)
            throws IOException, InterruptedException {

        String docIdPair = key.toString();
        String[] docIds = docIdPair.split(",");

        int docId1 = Integer.valueOf(docIds[0]);
        int docId2 = Integer.valueOf(docIds[1]);

        int container1 = (docId1 - 1) / this.docsInSeg;
        int container2 = (docId2 - 1) / this.docsInSeg;

        // get the weight
        double weight = Double.valueOf(value.toString());
        this.outputKey.set(weight);

        if (container1 != container2
//                && ((docId2 - 1) % this.docsInSeg > (this.docsInSeg / 2))) {
                && (docId2 % this.docsInSeg >= (this.docsInSeg / 2))) {
//                || docId2 % docsInSeg == 0)) {
            this.outputValue.set(docIdPair + ":" + container2);
        } else {
            this.outputValue.set(docIdPair + ":" + container1);
        }
        // weight \t src,dest:containder_id
        context.write(this.outputKey, this.outputValue);
    }
}
// End MSTChildMapper.java
