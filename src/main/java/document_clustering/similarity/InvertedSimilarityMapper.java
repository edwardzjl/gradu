package document_clustering.similarity;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * Created by edwardlol on 2016/12/2.
 */
public class InvertedSimilarityMapper extends Mapper<LongWritable, Text, Text, DoubleWritable> {

    private Text outputKey = new Text();

    private DoubleWritable outputValue = new DoubleWritable();

//    private List<String> list = new ArrayList<>();

    //~ Methods ----------------------------------------------------------------

    /**
     * @param key     position
     * @param value   term_line_no::term \t doc_line_no=tf-idf,...
     * @param context
     * @throws IOException
     * @throws InterruptedException
     */
    @Override
    public void map(LongWritable key, Text value, Context context)
            throws IOException, InterruptedException {

        System.err.println("start");
        String[] line = value.toString().split("\t");
        String[] contents = line[1].split(",");

        if (contents.length > 1) {
            System.err.println(line[0]);
            for (int i = 0; i < contents.length; i++) {
                for (int j = i + 1; j < contents.length; j++) {
                    String[] idAndWeight1 = contents[i].split("=");
                    String[] idAndWeight2 = contents[j].split("=");
                    if (Integer.valueOf(idAndWeight1[0]) > Integer.valueOf(idAndWeight2[0])) {
                        this.outputKey.set(idAndWeight2[0] + "," + idAndWeight1[0]);
                    } else {
                        this.outputKey.set(idAndWeight1[0] + "," + idAndWeight2[0]);
                    }
                    double similarity = Double.valueOf(idAndWeight1[1]) * Double.valueOf(idAndWeight2[1]);
                    this.outputValue.set(similarity);

                    context.write(this.outputKey, this.outputValue);
                }
            }
        }
    }
}

// End InvertedSimilarityMapper.java
