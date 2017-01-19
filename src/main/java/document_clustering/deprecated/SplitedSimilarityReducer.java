package document_clustering.deprecated;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.mahout.common.distance.CosineDistanceMeasure;
import org.apache.mahout.math.RandomAccessSparseVector;
import org.apache.mahout.math.Vector;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * Created by edwardlol on 2016/12/2.
 */
public class SplitedSimilarityReducer extends Reducer<IntWritable, Text, Text, DoubleWritable> {

    private final CosineDistanceMeasure cdm = new CosineDistanceMeasure();
    private Text outputKey = new Text();
    private DoubleWritable outputValue = new DoubleWritable();
    private double threshold = 1.0d;

//    private static final Logger logger = Logger.getLogger(SplitedSimilarityReducer.class);

    //~ Methods ----------------------------------------------------------------

    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        Configuration conf = context.getConfiguration();
        try {
            threshold = Double.parseDouble(conf.get("output.threshold"));
        } catch (NumberFormatException e) {
            e.printStackTrace();
        }
    }

    @Override
    public void reduce(IntWritable key, Iterable<Text> values,
                       Context context) throws IOException, InterruptedException {

        List<String> vectors = new ArrayList<>();

        // Storing each key-value pair (document) in a List
        for (Text value : values) {
            vectors.add(value.toString());
        }

        // Generating all the possible combinations of documents
        if (vectors.size() > 0) {
            for (int i = 0; i < vectors.size() - 1; i++) {
                for (int j = i + 1; j < vectors.size(); j++) {
                    String[] strVec1 = vectors.get(i).split("\t");
                    String[] strVec2 = vectors.get(j).split("\t");
                    int id1 = Integer.valueOf(strVec1[0]);
                    int id2 = Integer.valueOf(strVec2[0]);

                    String[] tmp1 = strVec1[1].split("\\{");
                    String[] tmp2 = strVec2[1].split("\\{");
                    int vectorLen = Integer.parseInt(tmp1[0]);

                    String[] contents1 = tmp1[1].replace("}", "").split(",");
                    String[] contents2 = tmp2[1].replace("}", "").split(",");

                    Vector vector1 = new RandomAccessSparseVector(vectorLen);
                    Vector vector2 = new RandomAccessSparseVector(vectorLen);

                    for (String content : contents1) {
                        String[] kv = content.split(":");
                        vector1.setQuick(Integer.parseInt(kv[0]), Double.parseDouble(kv[1]));
                    }
                    for (String content : contents2) {
                        String[] kv = content.split(":");
                        vector2.setQuick(Integer.parseInt(kv[0]), Double.parseDouble(kv[1]));
                    }

                    double cosine = cdm.distance(vector1, vector2);

                    if (cosine <= threshold) {
                        this.outputKey.set((id1 < id2 ? id1 : id2)
                                + ":"
                                + (id1 < id2 ? id2 : id1));
                        this.outputValue.set(cosine);
                        context.write(this.outputKey, this.outputValue);
                    }
                }
            }
        }
    }
}

// End SimilarityReducer.java
