package document_clustering.mst;

import document_clustering.util.UnionFind;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;

/**
 * Created by edwardlol on 2016/12/2.
 */
public class MSTFinalReducer extends Reducer<DoubleWritable, Text, IntWritable, IntWritable> {
    //~ Instance fields --------------------------------------------------------

    private IntWritable outputKey = new IntWritable();

    private IntWritable outputValue = new IntWritable();

    private UnionFind unionFind;

    private double threshold;

    //~ Methods ----------------------------------------------------------------

    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        Configuration conf = context.getConfiguration();
        this.threshold = conf.getDouble("final.threshold", 0.5d);
        if (context.getCacheFiles() != null
                && context.getCacheFiles().length > 0) {

            FileReader fileReader = new FileReader("./docCnt");
            BufferedReader bufferedReader = new BufferedReader(fileReader);
            String line = bufferedReader.readLine();
            unionFind = new UnionFind(Integer.parseInt(line) + 1);

            bufferedReader.close();
            fileReader.close();
        }
    }

    /**
     * @param inputKey similarity
     * @param values   doc_id1,doc_id2
     * @param context
     * @throws IOException
     * @throws InterruptedException
     */
    @Override
    public void reduce(DoubleWritable inputKey, Iterable<Text> values, Context context)
            throws IOException, InterruptedException {

        if (inputKey.get() < this.threshold) {
            for (Text val : values) {
                String[] srcDest = val.toString().split(",");

                int src = Integer.valueOf(srcDest[0]);
                int dest = Integer.valueOf(srcDest[1]);

                unionFind.union(src, dest);
            }
        }
    }

    @Override
    protected void cleanup(Context context) throws IOException, InterruptedException {
        int[] uf = unionFind.getId();
        for (int i = 0; i < uf.length; i++) {
            this.outputKey.set(i);
            this.outputValue.set(unionFind.find(i));
            // doc_id \t group_id
            context.write(this.outputKey, this.outputValue);
        }
    }
}

// End MSTFinalReducer.java
