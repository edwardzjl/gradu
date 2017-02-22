package document_clustering.deprecated;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * the mapper class for calculating inverted index
 * <p>
 * Created by edwardlol on 2016/11/24.
 */
public class InvertedIndexMapper extends Mapper<Text, Text, Text, Text> {
    //~  Instance fields -------------------------------------------------------

    private Text outputKey = new Text();

    private Text outputValue = new Text();

    //~  Methods ---------------------------------------------------------------

    /**
     * @param key     term@@@entry_id@@g_no@@group_id
     * @param value   tf-idf
     * @param context
     * @throws IOException
     * @throws InterruptedException
     */
    @Override
    public void map(Text key, Text value, Context context)
            throws IOException, InterruptedException {

        String[] termAndId = key.toString().split("@@@");
        String term = termAndId[0];
        String id = termAndId[1];
        String tf_idf = value.toString();

        String[] id_array = id.split("@@");

        this.outputKey.set(term);
        this.outputValue.set(id_array[2] + "=" + tf_idf);
        // term \t group_id=tf-idf
        context.write(this.outputKey, this.outputValue);
    }
}

// End InvertedIndexMapper.java
