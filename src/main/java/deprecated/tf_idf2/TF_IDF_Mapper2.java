package deprecated.tf_idf2;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * Created by edwardlol on 2016/12/3.
 */
public class TF_IDF_Mapper2 extends Mapper<Text, Text, Text, Text> {
    //~ Instance fields --------------------------------------------------------

    private Text outputKey = new Text();

    private Text outputValue = new Text();

    //~ Methods ----------------------------------------------------------------

    /**
     * @param key     term
     * @param value   id@@g_no@@line_no=weight
     * @param context
     * @throws IOException
     * @throws InterruptedException
     */
    @Override
    public void map(Text key, Text value, Context context)
            throws IOException, InterruptedException {

        context.write(key, value);
    }
}

// End TF_IDF_Mapper.java
