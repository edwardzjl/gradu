package document_clustering.deprecated;

import document_clustering.writables.tuple_writables.IntDoubleTupleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

/**
 * Created by edwardlol on 2016/11/30.
 */
public class VectorMapper extends Mapper<LongWritable, Text, LongWritable, IntDoubleTupleWritable> {
    //~ Instance fields --------------------------------------------------------

    private LongWritable outputKey = new LongWritable();

    private IntDoubleTupleWritable outputValue = new IntDoubleTupleWritable();

//    private List<String> globalKeywords = new ArrayList<>();

    private Map<String, Integer> _globalKeywords = new HashMap<>();

    //~ Methods ----------------------------------------------------------------

    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        if (context.getCacheFiles() != null
                && context.getCacheFiles().length > 0) {

            FileReader fileReader = new FileReader("./invertedIndex");
            BufferedReader bufferedReader = new BufferedReader(fileReader);
            String line = bufferedReader.readLine();
            while (line != null) {
                String[] contents = line.split("\t");
//                this.globalKeywords.add(contents[0]);
                String[] idAndWords = contents[0].split("::");
                System.out.println(idAndWords[0] + "::" + idAndWords[1]);
                this._globalKeywords.put(idAndWords[1], Integer.valueOf(idAndWords[0]));
                line = bufferedReader.readLine();
            }
        }
        super.setup(context);
    }

    @Override
    protected void map(LongWritable key, Text value, Context context)
            throws IOException, InterruptedException {

        // line[0] = term@@@id@@g_no@@line_no
        // line[1] = tf-idf
        String[] line = value.toString().split("\t");

        // termAndId[0] = term
        // termAndId[1] = id@@g_no@@line_no
        String[] termAndId = line[0].split("@@@");

        String term = termAndId[0];
        String idAndLineNo = termAndId[1];

        // tmp[0] = id
        // tmp[1] = g_no
        // tmp[2] = line_no
        String[] tmp = idAndLineNo.split("@@");
        String lineNo = tmp[2];

//        int index = globalKeywords.indexOf(term);

        if (!_globalKeywords.containsKey(term)) {
            System.err.println(term);
        }

        int index = _globalKeywords.get(term);

//        if (index > -1) {
        this.outputKey.set(Long.parseLong(lineNo));
        this.outputValue.set(index, Double.parseDouble(line[1]));

        context.write(this.outputKey, this.outputValue);
//        }
    }
}

// End VectorMapper.java
