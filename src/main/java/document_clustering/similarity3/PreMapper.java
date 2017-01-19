package document_clustering.similarity3;

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

    private int splitNum;

    //~ Methods ----------------------------------------------------------------

    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        Configuration conf = context.getConfiguration();
        this.reduceNum = conf.getInt("reducer.num", 1);
        this.splitNum = conf.getInt("split.num", 6);
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

        if (docs.length > 1 && docs.length < 5000) {
            this.outputKey.set(smallIndex++);
            this.outputValue.set(key.toString() + ":" + value.toString());
            // container \t term:doc_line_no=tf-idf,...
            context.write(this.outputKey, this.outputValue);
            smallIndex = smallIndex % this.reduceNum;
        }
    }
}

// End PreMapper.java
