package document_clustering.deprecated;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.Iterator;

/**
 * Created by edwardlol on 2016/12/2.
 */
// TODO: 2016/12/27 finish this
public class PreCombiner extends Reducer<IntWritable, Text, IntWritable, Text> {

    //~ Methods ----------------------------------------------------------------

    /**
     * @param key     container
     * @param values  term:doc_line_no=tf-idf,...
     * @param context
     * @throws IOException
     * @throws InterruptedException
     */
    @Override
    public void reduce(IntWritable key, Iterable<Text> values,
                       Context context) throws IOException, InterruptedException {

    }
}

// End InvertedSimilarityReducer.java
