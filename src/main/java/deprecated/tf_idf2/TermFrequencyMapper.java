package deprecated.tf_idf2;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * Created by edwardlol on 2016/12/3.
 */
public class TermFrequencyMapper extends Mapper<Text, Text, Text, Text> {
    //~ Instance fields --------------------------------------------------------

    private Text outputKey = new Text();

    private Text outputValue = new Text();

    //~ Methods ----------------------------------------------------------------

    /**
     * @param key     term@@@id@@g_no@@line_no::position
     * @param value   count
     * @param context
     * @throws IOException
     * @throws InterruptedException
     */
    @Override
    public void map(Text key, Text value, Context context)
            throws IOException, InterruptedException {

        // termDoc[0] = term
        // termDoc[1] = id@@g_no@@line_no::position
        String[] termDoc = key.toString().split("@@@");

        // idAndPlace[0] = id@@g_no@@line_no
        // idAndPlace[1] = position
        String[] idAndPlace = termDoc[1].split("::");

        this.outputKey.set(idAndPlace[0]);
        this.outputValue.set(idAndPlace[1] + "::" + termDoc[0] + "=" + value.toString());
        // id@@g_no@@line_no \t position::term=count
        context.write(this.outputKey, this.outputValue);
    }
}

// End TermFrequencyMapper.java
