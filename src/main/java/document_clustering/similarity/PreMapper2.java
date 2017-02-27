package document_clustering.similarity;

import document_clustering.writables.tuple_writables.IntIntTupleWritable;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * devide and distribute the indexes
 * <p>
 * if the index is short, just send it to a container and the container will do a self join.
 * if the index is long, I first devide it into splitNum splits.
 * there are two strategies.
 * <p>
 * <p>
 * Created by edwardlol on 2016/12/2.
 */
public class PreMapper2 extends Mapper<Text, Text, IntIntTupleWritable, Text> {
    //~ Instance fields --------------------------------------------------------

    private IntIntTupleWritable outputKey = new IntIntTupleWritable();

    private Text outputValue = new Text();

    /**
     * container index for short inverted_indexes
     */
    private int smallIndex = 0;

    /**
     * container index for long inverted_indexes
     */
    private int bigIndex = 0;

    /**
     * number of reducer tasks
     */
//    private int reduceNum;

    /**
     * the number of splits to divide a long index into
     */
    private int splitNum;

    /**
     * threshold of index length to determine
     * whether to divide an index or not
     */
    private int lengthThreshold;

    private StringBuilder sb1 = new StringBuilder();

    //~ Methods ----------------------------------------------------------------

    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        Configuration conf = context.getConfiguration();
//        this.reduceNum = conf.getInt("reducer.num", 5);
        this.splitNum = conf.getInt("split.num", 6);
        this.lengthThreshold = conf.getInt("long.threshold", 1000);
    }

    /**
     * @param key     id:term
     * @param value   group_id=tf-idf,...
     * @param context
     * @throws IOException
     * @throws InterruptedException
     */
    @Override
    public void map(Text key, Text value, Context context)
            throws IOException, InterruptedException {

        String[] idAndTerm = key.toString().split(":");
        String termId = idAndTerm[0];

        String[] docs = value.toString().split(",");

        if (docs.length > this.lengthThreshold) {
            /* set the length of each container */
            int docsInSeg = docs.length / this.splitNum;
            if (docs.length % this.splitNum != 0) {
                docsInSeg++;
            }

            for (int i = 0; i < this.splitNum - 1; i++) {
                this.sb1.setLength(0);

                /*  */
                for (int a = 0; a < docsInSeg; a++) {
                    this.sb1.append(docs[i * docsInSeg + a]).append(',');
                }
                this.sb1.deleteCharAt(this.sb1.length() - 1);
                this.outputKey.set(this.bigIndex++, 0);
                this.outputValue.set(termId + ":" + this.sb1.toString());
                // container_id,flag \t term_id:group_id=tf-idf,...
                context.write(this.outputKey, this.outputValue);
                // TODO: 17-2-23 is it necessary to do %?
//                this.bigIndex = this.bigIndex % this.reduceNum;

                for (int j = i + 1; j < this.splitNum; j++) {
                    /* continue from sb1 */
                    StringBuilder sb2 = new StringBuilder(this.sb1).append('#');
                    for (int b = 0; b < docsInSeg; b++) {
                        int index = j * docsInSeg + b;
                        if (index >= docs.length) {
                            break;
                        }
                        sb2.append(docs[index]).append(',');
                    }
                    sb2.deleteCharAt(sb2.length() - 1);
                    this.outputKey.set(this.bigIndex++, 1);
                    this.outputValue.set(termId + ":" + sb2.toString());

                    // container_id,flag \t term_id:group_id=tf-idf,...
                    context.write(this.outputKey, this.outputValue);
//                    this.bigIndex = this.bigIndex % this.reduceNum;
                }
            }
            this.sb1.setLength(0);
            for (int a = (this.splitNum - 1) * docsInSeg; a < docs.length; a++) {
                this.sb1.append(docs[a]).append(',');
            }
            this.sb1.deleteCharAt(this.sb1.length() - 1);
            this.outputKey.set(this.bigIndex++, 0);
            this.outputValue.set(termId + ":" + this.sb1.toString());
            context.write(this.outputKey, this.outputValue);
//            this.bigIndex = this.bigIndex % this.reduceNum;
        } else if (docs.length > 1) {
            this.outputKey.set(smallIndex++, 0);
            this.outputValue.set(termId + ":" + value.toString());
            // container_id,flag \t term_id:group_id=tf-idf,...
            context.write(this.outputKey, this.outputValue);
//            this.smallIndex = this.smallIndex % this.reduceNum;
        }
    }
}

// End PreMapper2.java
