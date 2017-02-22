package document_clustering.tf_idf;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

/**
 * Created by edwardlol on 2016/12/3.
 */
public class TF_IDF_Reducer extends Reducer<Text, Text, Text, DoubleWritable> {
    //~ Instance fields --------------------------------------------------------

    private int documentNumber;

    private Text outputKey = new Text();

    private DoubleWritable outputValue = new DoubleWritable();

    private Map<String, String> termAndWTF = new HashMap<>();

    //~ Methods ----------------------------------------------------------------

    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        if (context.getCacheFiles() != null
                && context.getCacheFiles().length > 0) {

            FileReader fileReader = new FileReader("./docCnt");
            BufferedReader bufferedReader = new BufferedReader(fileReader);

            String line = bufferedReader.readLine();
            this.documentNumber = Integer.parseInt(line);

            bufferedReader.close();
            fileReader.close();
        }
    }

    /**
     * @param key     term
     * @param values  entry_id@@g_no@@group_id=weighted_tf
     * @param context
     * @throws IOException
     * @throws InterruptedException
     */
    @Override
    protected void reduce(Text key, Iterable<Text> values, Context context)
            throws IOException, InterruptedException {

        this.termAndWTF.clear();
        // total appear times of this term
        int appearInAll = 0;
        /* count the total appear times of each term
         * and store them with their weighted tf in a map  */
        for (Text value : values) {
            appearInAll++;
            // docAndFreq[0] = entry_id@@g_no@@group_id
            // docAndFreq[1] = weighted_tf
            String[] docAndFreq = value.toString().split("=");
            this.termAndWTF.put(docAndFreq[0], docAndFreq[1]);
        }

        for (Map.Entry<String, String> entry : termAndWTF.entrySet()) {
            double wtf = Double.valueOf(entry.getValue());

            double idf = Math.log((double) this.documentNumber
                    / (double) (appearInAll + 1));

            this.outputKey.set(key + "@@@" + entry.getKey());
            this.outputValue.set(wtf * idf);
            // term@@@entry_id@@g_no@@group_id \t tf-idf
            context.write(this.outputKey, this.outputValue);
        }
    }
}

// End TF_IDF_Reducer.java
