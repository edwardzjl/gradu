package deprecated.similarity2;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/**
 * Created by edwardlol on 2016/12/2.
 */
// TODO: 2016/12/27 finish this
public class PreCombiner extends Reducer<IntWritable, Text, Text, Text> {

    private Text outputKey = new Text();

    private Text outputValue = new Text();

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

        for (Text value : values) {
            String[] line = value.toString().split(":");

            String[] contents = line[1].split(",");
            if (contents.length > 1) {
                for (int i = 0; i < contents.length; i++) {
                    for (int j = i + 1; j < contents.length; j++) {
                        String[] idAndWeight1 = contents[i].split("=");
                        String[] idAndWeight2 = contents[j].split("=");
                        if (Integer.valueOf(idAndWeight1[0]) > Integer.valueOf(idAndWeight2[0])) {
                            this.outputKey.set(idAndWeight2[0] + "," + idAndWeight1[0]);
                        } else {
                            this.outputKey.set(idAndWeight1[0] + "," + idAndWeight2[0]);
                        }
                        this.outputValue.set(line[0] + ":" + idAndWeight1[1] + "*" + idAndWeight2[1]);
                        // doc1,doc2 \t term:w1*w2
                        context.write(this.outputKey, this.outputValue);
                    }
                }
            }
        }
    }
}

// End InvertedSimilarityReducer.java
