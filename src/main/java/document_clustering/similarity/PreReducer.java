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

    private String formatBase;

    private DecimalFormat decimalFormat;

    //~ Methods ----------------------------------------------------------------

    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        Configuration conf = context.getConfiguration();
        int deciNum = conf.getInt("deci.number", 3);
        StringBuilder stringBuilder = new StringBuilder();
        stringBuilder.append("0.");
        for (int i = 0; i < deciNum; i++) {
            stringBuilder.append('0');
        }
        this.formatBase = stringBuilder.toString();
        this.decimalFormat = new DecimalFormat(this.formatBase);
    }

    /**
     * @param key    container_id, flag
     * @param values term_id:docId=TF-IDF,docId=TF-IDF...
     *               {@inheritDoc}
     */
    @Override
    public void reduce(IntIntTupleWritable key, Iterable<Text> values,
                       Context context) throws IOException, InterruptedException {

        if (key.getRightValue() == 0) {
            /* flag == 0, do self join */
            selfJoin(values, context);
        } else {
            /* flag == 1, do cross join */
            crossJoin(values, context);
        }
    }

    /**
     * @param values docId=TF-IDF,docId=TF-IDF...
     */
    private void selfJoin(Iterable<Text> values, Context context)
            throws IOException, InterruptedException {

        for (Text value : values) {
            String[] contents = value.toString().split(",");
            // this term only appears in one doc, skip calculation
            if (contents.length > 1) {
                for (int i = 0; i < contents.length; i++) {
                    for (int j = i + 1; j < contents.length; j++) {
                        String[] idAndWeighti = contents[i].split("=");
                        String[] idAndWeightj = contents[j].split("=");

                        output(idAndWeighti[0], idAndWeightj[0], idAndWeighti[1], idAndWeightj[1], context);
                    }
                }
            }
        }
    }

    /**
     * @param values docId=TF-IDF,docId=TF-IDF...
     */
    private void crossJoin(Iterable<Text> values, Context context)
            throws IOException, InterruptedException {

        for (Text value : values) {
            // termId, docId=TF-IDF,docId=TF-IDF...
            String[] sets = value.toString().split("#");
            String[] set1 = sets[0].split(",");
            String[] set2 = sets[1].split(",");

            for (String aPart1 : set1) {
                for (String aPart2 : set2) {
                    String[] idAndWeight1 = aPart1.split("=");
                    String[] idAndWeight2 = aPart2.split("=");

                    output(idAndWeight1[0], idAndWeight2[0], idAndWeight1[1], idAndWeight2[1], context);
                }
            }
        }
    }

    /**
     * @param id1
     * @param id2
     * @param tf_idf1
     * @param tf_idf2
     * @param context
     * @throws IOException
     * @throws InterruptedException
     */
    private void output(String id1, String id2,
                        String tf_idf1, String tf_idf2, Context context)
            throws IOException, InterruptedException {
        double result = Double.valueOf(tf_idf1) * Double.valueOf(tf_idf2);

        String out = this.decimalFormat.format(result);
        if (!this.formatBase.equals(out)) {
            if (Integer.valueOf(id1) > Integer.valueOf(id2)) {
                this.outputKey.set(id2 + "," + id1);
            } else {
                this.outputKey.set(id1 + "," + id2);
            }
            this.outputValue.set(out);
            // doc1,doc2 \t sim
            context.write(this.outputKey, this.outputValue);
        }
    }
}

// End PreReducer.java
