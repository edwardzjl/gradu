package deprecated.similarity2;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * Created by edwardlol on 2016/12/2.
 */
public class PreMapper extends Mapper<Text, Text, IntWritable, Text> {

    private IntWritable outputKey = new IntWritable();

    private Text outputValue = new Text();

    private int smallIndex = 0;

    private int bigIndex = 0;

    private int reduceNum;

    //~ Methods ----------------------------------------------------------------

    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        Configuration conf = context.getConfiguration();
        this.reduceNum = conf.getInt("reducer.num", 1);
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

        if (docs.length > 1000) {
            int contentsInBlock = docs.length / 6;
            if (docs.length % 6 != 0) {
                contentsInBlock++;
            }

            for (int i = 0; i < 5; i++) {
                StringBuilder output = new StringBuilder();
                for (int a = 0; a < contentsInBlock; a++) {
                    output.append(docs[i * contentsInBlock + a]).append(',');
                }
                for (int j = i + 1; j < 6; j++) {
                    for (int b = 0; b < contentsInBlock; b++) {
                        int index = j * contentsInBlock + b;
                        if (index >= docs.length) {
                            break;
                        }
                        output.append(docs[index]).append(',');
                    }
                    this.outputKey.set(bigIndex++);
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
