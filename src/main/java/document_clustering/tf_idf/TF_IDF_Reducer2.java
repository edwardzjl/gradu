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
public class TF_IDF_Reducer2 extends Reducer<Text, Text, Text, DoubleWritable> {
    //~ Instance fields --------------------------------------------------------

    private int documentNumber;

    private Text outputKey = new Text();

    private DoubleWritable outputValue = new DoubleWritable();

    private Map<String, String> termFrequencies = new HashMap<>();

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
     *
     * @param key     term
     * @param values  id@@g_no@@line_no=weight
     * @param context
     * @throws IOException
     * @throws InterruptedException
     */
    @Override
    protected void reduce(Text key, Iterable<Text> values, Context context)
            throws IOException, InterruptedException {

        this.termFrequencies.clear();
        // total frequency of this word
        int appearInAll = 0;

        for (Text value : values) {
            // docAndFreq[0] = id@@g_no@@line_no
            // docAndFreq[1] = weight / sumOfWordsInDocument
            String[] docAndFreq = value.toString().split("=");
            appearInAll++;
            this.termFrequencies.put(docAndFreq[0], docAndFreq[1]);
        }

        for (Map.Entry<String, String> entry : termFrequencies.entrySet()) {

            double tf = Double.valueOf(entry.getValue());

            double idf = Math.log((double) this.documentNumber
                    / (double) (appearInAll + 1));

            // term@@@id@@g_no@@line_no
            this.outputKey.set(key + "@@@" + entry.getKey());
            this.outputValue.set(tf * idf);

            context.write(this.outputKey, this.outputValue);
        }
    }
}

// End TF_IDF_Reducer.java
