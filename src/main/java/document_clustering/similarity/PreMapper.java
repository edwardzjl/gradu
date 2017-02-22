package document_clustering.similarity;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;
import java.text.DecimalFormat;

/**
 * Created by edwardlol on 2016/12/2.
 */
public class PreMapper extends Mapper<Text, Text, IntWritable, Text> {

    private IntWritable outputKey = new IntWritable();

    private Text outputValue = new Text();

    private int smallIndex = 0;

    private int bigIndex = 0;

    private int reduceNum;

    private int splitNum;

    private int longThreshold;

    private DecimalFormat decimalFormat = new DecimalFormat( "0.0000");

    //~ Methods ----------------------------------------------------------------

    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        Configuration conf = context.getConfiguration();
        this.reduceNum = conf.getInt("reducer.num", 1);
        this.splitNum = conf.getInt("split.num", 6);
        this.longThreshold = conf.getInt("long.threshold", 1000);
    }

    /**
     * @param key     term
     * @param value   doc_line_no=tf-idf,...
     * @param context
     * @throws IOException
     * @throws InterruptedException
     */
    @Override
    public void map(Text key, Text value, Context context)
            throws IOException, InterruptedException {

        String[] docs = value.toString().split(",");

        if (docs.length > this.longThreshold) {
            /*  */
            int docsInSeg = docs.length / this.splitNum;
            if (docs.length % this.splitNum != 0) {
                docsInSeg++;
            }

            for (int i = 0; i < this.splitNum - 1; i++) {
                StringBuilder output = new StringBuilder();
                for (int a = 0; a < docsInSeg; a++) {
                    // TODO: 17-2-22 make the content shorter
                    output.append(docs[i * docsInSeg + a]).append(',');
                }
                for (int j = i + 1; j < this.splitNum; j++) {
                    for (int b = 0; b < docsInSeg; b++) {
                        int index = j * docsInSeg + b;
                        if (index >= docs.length) {
                            break;
                        }
                        output.append(docs[index]).append(',');
                    }
                    this.outputKey.set(this.bigIndex++);
                    this.outputValue.set(key.toString() + ":"
                            + output.deleteCharAt(output.length() - 1).toString());
                    // container \t term:doc_line_no=tf-idf,...
                    context.write(this.outputKey, this.outputValue);
                    bigIndex = bigIndex % this.reduceNum;
                }
            }
        } else if (docs.length > 1) {
            this.outputKey.set(smallIndex++);
            this.outputValue.set(key.toString() + ":" + value.toString());
            // container \t term:doc_line_no=tf-idf,...
            context.write(this.outputKey, this.outputValue);
            smallIndex = smallIndex % this.reduceNum;
        }
    }
}

// End PreMapper.java
