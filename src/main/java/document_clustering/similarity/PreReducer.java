package document_clustering.similarity;

import document_clustering.writables.tuple_writables.IntIntTupleWritable;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.text.DecimalFormat;

/**
 * Created by edwardlol on 2016/12/2.
 */
public class PreReducer extends Reducer<IntIntTupleWritable, Text, Text, Text> {

    private Text outputKey = new Text();

    private Text outputValue = new Text();

    private DecimalFormat decimalFormat;

    //~ Methods ----------------------------------------------------------------

    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        Configuration conf = context.getConfiguration();
        int deci_num = conf.getInt("deci.number", 3);
        StringBuilder stringBuilder = new StringBuilder();
        stringBuilder.append("0.");
        for (int i = 0; i < deci_num; i++) {
            stringBuilder.append('0');
        }
        this.decimalFormat = new DecimalFormat(stringBuilder.toString());
    }

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
            selfJoin(key, values, context);
        } else {
            /* flag == 1, do cross join */
            crossJoin(key, values, context);
        }
    }

    /**
     * @param key     container_id, flag
     * @param values  term_id:docId=TF-IDF,docId=TF-IDF...
     * @param context
     */
    private void selfJoin(IntIntTupleWritable key, Iterable<Text> values, Context context)
            throws IOException, InterruptedException {
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

                        setOutputKey(idAndWeighti[0], idAndWeightj[0]);
                        setOutputValue(line[0], idAndWeighti[1], idAndWeightj[1]);

                        // doc1,doc2 \t termId:sim
                        context.write(this.outputKey, this.outputValue);
                    }
                }
            }
        } catch (RuntimeException e) {
            e.printStackTrace();
            System.out.println("key: " + key.toString());
            System.out.println("right: " + key.getRightValue());
        }
    }

    /**
     * @param key     container_id, flag
     * @param values  term_id:docId=TF-IDF,docId=TF-IDF...
     * @param context
     */
    private void crossJoin(IntIntTupleWritable key, Iterable<Text> values, Context context)
            throws IOException, InterruptedException {
        try {
            for (Text value : values) {
                // termId, docId=TF-IDF,docId=TF-IDF...
                String[] line = value.toString().split(":");
                String[] sets = line[1].split("#");
                String[] set1 = sets[0].split(",");
                String[] set2 = sets[1].split(",");

                for (String aPart1 : set1) {
                    for (String aPart2 : set2) {
                        String[] idAndWeight1 = aPart1.split("=");
                        String[] idAndWeight2 = aPart2.split("=");

                        setOutputKey(idAndWeight1[0], idAndWeight2[0]);
                        setOutputValue(line[0], idAndWeight1[1], idAndWeight2[1]);

                        // doc1,doc2 \t term:sim
                        context.write(this.outputKey, this.outputValue);
                    }
                }
            }
        } catch (RuntimeException e) {
            e.printStackTrace();
            System.out.println("key: " + key.toString());
            System.out.println("right: " + key.getRightValue());
        }
    }

    private void setOutputKey(String id1, String id2) {
        int _id1 = Integer.valueOf(id1);
        int _id2 = Integer.valueOf(id2);
        if (_id1 > _id2) {
            this.outputKey.set(id2 + "," + id1);
        } else {
            this.outputKey.set(id1 + "," + id2);
        }
    }

    private void setOutputValue(String termId, String tf_idf1, String tf_idf2) {
        String out = format(Double.valueOf(tf_idf1) * Double.valueOf(tf_idf2));
        this.outputValue.set(termId + ":" + out);
    }

    private String format(double value) {
        return this.decimalFormat.format(value);
    }
}

// End PreReducer.java
