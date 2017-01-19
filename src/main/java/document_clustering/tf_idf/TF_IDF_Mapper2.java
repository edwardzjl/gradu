package document_clustering.tf_idf;

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
     * @param key     term@@@id@@g_no@@line_no
     * @param value   weight
     * @param context
     * @throws IOException
     * @throws InterruptedException
     */
    @Override
    public void map(Text key, Text value, Context context)
            throws IOException, InterruptedException {

        String[] termAndDoc = key.toString().split("@@@");

        // term
        this.outputKey.set(termAndDoc[0]);
        // id@@g_no@@line_no=weight / sumOfWordsInDocument
        this.outputValue.set(termAndDoc[1] + "=" + value.toString());
        context.write(this.outputKey, this.outputValue);
    }
}

// End TF_IDF_Mapper.java
