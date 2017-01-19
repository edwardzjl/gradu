package document_clustering.debug;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * the mapper class for calculating inverted index
 * <p>
 * Created by edwardlol on 2016/11/24.
 */
public class InvertedIndexMapper3 extends Mapper<Text, Text, Text, IntWritable> {

    //~  Instance fields -------------------------------------------------------

//    private Text outputKey = new Text();
    private String top;

    private Text outputValue = new Text();

    private int max = 0;

    //~  Methods ---------------------------------------------------------------

    /**
     * @param key     id::term
     * @param value   line_no=tf-idf,...
     * @param context
     * @throws IOException
     * @throws InterruptedException
     */
    @Override
    public void map(Text key, Text value, Context context)
            throws IOException, InterruptedException {

//        String[] idAndTerm = key.toString().split("::");
//        String id = idAndTerm[0];
//        String term = idAndTerm[1];

        String[] tf_idfs = value.toString().split(",");
        if (tf_idfs.length > max) {
            max = tf_idfs.length;
            top = key.toString() + ":" + max;
        }
        if (tf_idfs.length > 1000) {
            context.write(new Text("1000"), new IntWritable(1));
        }
        if (tf_idfs.length > 1100) {
            context.write(new Text("1100"), new IntWritable(1));
        }
        if (tf_idfs.length > 1200) {
            context.write(new Text("1200"), new IntWritable(1));
        }
        if (tf_idfs.length > 1300) {
            context.write(new Text("1300"), new IntWritable(1));
        }
        if (tf_idfs.length > 1400) {
            context.write(new Text("1400"), new IntWritable(1));
        }
        if (tf_idfs.length > 1500) {
            context.write(new Text("1500"), new IntWritable(1));
        }

//        String tf_idf = value.toString();
//
//        String[] id_array = id.split("@@");
//
//        // term
//        this.outputKey.set(term);
//        // entry_id@@g_no@@line_no=tf-idf
//        this.outputValue.set(id_array[2] + "=" + tf_idf);
//        context.write(this.outputKey, this.outputValue);
    }

    @Override
    protected void cleanup(Context context) throws IOException, InterruptedException {
        System.err.println(top);
    }
}

// End InvertedIndexMapper3.java
