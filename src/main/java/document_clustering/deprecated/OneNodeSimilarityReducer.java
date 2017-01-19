package document_clustering.deprecated;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.mahout.common.distance.CosineDistanceMeasure;
import org.apache.mahout.math.RandomAccessSparseVector;
import org.apache.mahout.math.Vector;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

/**
 * Created by edwardlol on 2016/12/2.
 */
public class OneNodeSimilarityReducer extends Reducer<IntWritable, Text, Text, DoubleWritable> {

    private Text outputKey = new Text();

    private DoubleWritable outputValue = new DoubleWritable();

    //~ Methods ----------------------------------------------------------------

    @Override
    public void reduce(IntWritable key, Iterable<Text> values,
                       Context context) throws IOException, InterruptedException {

        Map<Integer, String> vectors = new HashMap<>();
        CosineDistanceMeasure cdm = new CosineDistanceMeasure();

        // Storing each key-value pair (document) in a Map
        for (Text value : values) {
            String[] idAndVector = value.toString().split("\t");
            vectors.put(Integer.parseInt(idAndVector[0]), idAndVector[1]);
        }

        // Generating all the possible combinations of documents
        if (vectors.size() > 0) {
            for (int i = 1; i < vectors.size(); i++) {
                for (int j = i + 1; j < vectors.size() + 1; j++) {

                    String strVector1 = vectors.get(i);
                    String strVector2 = vectors.get(j);

                    Vector vector1 = makeVector(strVector1);
                    Vector vector2 = makeVector(strVector2);

                    double cosine = cdm.distance(vector1, vector2);

                    if (cosine < 0.8) {
                        this.outputKey.set(i + ":" + j);
                        this.outputValue.set(cosine);
                        context.write(this.outputKey, this.outputValue);
                    }
                }
            }
        }
    }


    private Vector makeVector(String strVector) {
        String[] tmp1 = new String[2];
        try {
            tmp1 = strVector.split("\\{");
        } catch (Exception e) {
            System.err.println("vec: " + strVector);
            e.printStackTrace();
        }
        int vectorLen = Integer.parseInt(tmp1[0]);

        String[] contents = tmp1[1].replace("}", "").split(",");
        Vector vector = new RandomAccessSparseVector(vectorLen);

        for (String content : contents) {
            String[] kv = content.split(":");
            vector.setQuick(Integer.parseInt(kv[0]), Double.parseDouble(kv[1]));
        }
        return vector;
    }
}

// End OneNodeSimilarityReducer.java
