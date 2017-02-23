package document_clustering.similarity;

import document_clustering.writables.tuple_writables.IntIntTupleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/**
 * Created by edwardlol on 2016/12/2.
 */
public class PreReducer2 extends Reducer<IntIntTupleWritable, Text, Text, Text> {

    private Text outputKey = new Text();

    private Text outputValue = new Text();

    //~ Methods ----------------------------------------------------------------

    /**
     * @param key     container_id, flag
     * @param values  term_id:docId=TF-IDF,docId=TF-IDF...
     * @param context
     * @throws IOException
     * @throws InterruptedException
     */
    @Override
    public void reduce(IntIntTupleWritable key, Iterable<Text> values,
                       Context context) throws IOException, InterruptedException {

        if (key.getRightValue() == 0) {
            /* flag == 0, do self join */
            try {
                for (Text value : values) {
                    // termId, docId=TF-IDF,docId=TF-IDF...
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
            } catch (Exception e) {
                e.printStackTrace();
                System.out.println("key: " + key.toString());
                System.out.println("right: "+ key.getRightValue());
            }


        } else {
            /* flag == 1, do cross join */
            for (Text value : values) {
                // termId, docId=TF-IDF,docId=TF-IDF...
                String[] line = value.toString().split(":");
                String[] parts = line[1].split("#");
                String[] part1 = parts[0].split(",");
                String[] part2 = parts[1].split(",");

                for (int i = 0; i < part1.length; i++) {
                    for (int j = 0; j < part2.length; j++) {
                        String[] idAndWeighti = part1[i].split("=");
                        String[] idAndWeightj = part2[j].split("=");
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
    }

    private static String format(double value) {
        return String.format("%.2f", value);
    }
}

// End PreReducer2.java
