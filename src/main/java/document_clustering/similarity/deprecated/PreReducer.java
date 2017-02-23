package document_clustering.similarity.deprecated;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/**
 * Created by edwardlol on 2016/12/2.
 */
public class PreReducer extends Reducer<IntWritable, Text, Text, Text> {

    private Text outputKey = new Text();

    private Text outputValue = new Text();

    //~ Methods ----------------------------------------------------------------

    /**
     * @param key     container_id, meaningless
     * @param values  term_id:docId=TF-IDF,docId=TF-IDF...
     * @param context
     * @throws IOException
     * @throws InterruptedException
     */
    @Override
    public void reduce(IntWritable key, Iterable<Text> values,
                       Context context) throws IOException, InterruptedException {

        for (Text value : values) {
            String[] line = value.toString().split(":");

            String[] contents = line[1].split(",");
            if (contents.length < 2) {
                continue;
            }
            for (int i = 0; i < contents.length; i++) {
                for (int j = i + 1; j < contents.length; j++) {
                    String[] idAndWeighti = contents[i].split("=");
                    String[] idAndWeightj = contents[j].split("=");
                    if (Integer.valueOf(idAndWeighti[0]) > Integer.valueOf(idAndWeightj[0])) {
                        this.outputKey.set(idAndWeightj[0] + "," + idAndWeighti[0]);
                    } else {
                        this.outputKey.set(idAndWeighti[0] + "," + idAndWeightj[0]);
                    }

                    String out = format(Double.valueOf(idAndWeighti[1]) * Double.valueOf(idAndWeightj[1]));
                    this.outputValue.set(line[0] + ":" + out);
                    // doc1,doc2 \t term:sim
                    context.write(this.outputKey, this.outputValue);
                }
            }
        }
    }

    private static String format(double value) {
        return String.format("%.2f", value);
    }
}

// End PreReducer.java
