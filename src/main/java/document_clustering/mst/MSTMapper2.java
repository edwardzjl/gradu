package document_clustering.mst;

import document_clustering.util.CollectionUtil;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;

/**
 * Created by edwardlol on 2016/12/2.
 */
public class MSTMapper2 extends Mapper<Object, Text, DoubleWritable, Text> {

    private Text srcDestPair = new Text();

    private DoubleWritable iwWeight = new DoubleWritable();

    private int totalDocumentNumber;

    private int docsInSegment;

    private Map<String, Set<Integer>> idMap = new HashMap<>();

    //~ Methods ----------------------------------------------------------------

    @Override
    protected void setup(Context context) throws IOException, InterruptedException {

        if (context.getCacheFiles() != null
                && context.getCacheFiles().length > 0) {

            FileReader fileReader = new FileReader("./docCnt");
            BufferedReader bufferedReader = new BufferedReader(fileReader);

            this.totalDocumentNumber = Integer.valueOf(bufferedReader.readLine());

            bufferedReader.close();
            fileReader.close();
        }

        Configuration conf = context.getConfiguration();
        int splitNum = conf.getInt("split.num", 1);
        this.docsInSegment = this.totalDocumentNumber / splitNum;

        int containers = 0;
        for (int i = 0; i < splitNum; i++) {
            String ii = String.valueOf(i) + i;
            for (int j = i + 1; j < splitNum; j++) {
                String ij = String.valueOf(i) + j;
                String jj = String.valueOf(j) + j;

                CollectionUtil.updateSetMap(idMap, ii, containers);
                CollectionUtil.updateSetMap(idMap, ij, containers);
                CollectionUtil.updateSetMap(idMap, jj, containers);
                containers++;
            }
        }
    }

    /**
     * @param key     (doc_line_no1,doc_line_no2)
     * @param value   similarity
     * @param context
     * @throws IOException
     * @throws InterruptedException
     */
    public void map(Object key, Text value, Context context)
            throws IOException, InterruptedException {

        String[] inputTokens = value.toString().split("\t");
        String[] srcDest = inputTokens[0].replace("(", "").replace(")", "").split(",");

        // get the weight
        double weight = Double.valueOf(inputTokens[1]);
        iwWeight.set(weight);

        int srcId = Integer.valueOf(srcDest[0]);
        int destId = Integer.valueOf(srcDest[1]);

        int seg1 = (srcId - 1) / docsInSegment;
        int seg2 = (destId - 1) / docsInSegment;

        Set<Integer> containers = idMap.get(String.valueOf(seg1) + seg2);
        for (int container : containers) {
            srcDestPair.set(inputTokens[0] + ":" + container);
            context.write(iwWeight, srcDestPair);
//            System.err.println("w:" + iwWeight.toString() + " pair:" + srcDestPair.toString() + " cont:" + container);
        }
    }
}
// End MSTChildMapper.java
