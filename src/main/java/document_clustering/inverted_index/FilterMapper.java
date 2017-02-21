package document_clustering.inverted_index;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * the mapper class for calculating inverted index
 * <p>
 * Created by edwardlol on 2016/11/24.
 */
public class FilterMapper extends Mapper<Text, Text, IntWritable, Text> {
    //~  Instance fields -------------------------------------------------------

    private IntWritable outputKey = new IntWritable();

    private Text outputValue = new Text();

    //~  Methods ---------------------------------------------------------------

    /**
     * @param key     term
     * @param value   line_no=tf-idf,...
     * @param context
     * @throws IOException
     * @throws InterruptedException
     */
    @Override
    public void map(Text key, Text value, Context context)
            throws IOException, InterruptedException {

        int cnt = value.toString().split(",").length;

        this.outputKey.set(cnt);
        this.outputValue.set(key.toString() + "@@" + value.toString());
        // cnt \t term@@line_no=tf-idf,...
        context.write(this.outputKey, this.outputValue);
    }
}

// End InvertedIndexMapper.java
