package deprecated;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

/**
 * Created by edwardlol on 2016/12/2.
 */
public class ShowResultReducer extends Reducer<Text, Text, NullWritable, Text> {
    //~ Instance fields --------------------------------------------------------

    private Text outputValue = new Text();

    private Map<Integer, String> corpus = new HashMap<>();

    //~ Methods ----------------------------------------------------------------

    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        if (context.getCacheFiles() != null
                && context.getCacheFiles().length > 0) {

            FileReader fileReader = new FileReader("./simhashed");
            BufferedReader bufferedReader = new BufferedReader(fileReader);
            String line = bufferedReader.readLine();
            while (line != null) {
                String[] contents = line.split("\t");
                String[] document = contents[1].split("::");
                corpus.put(Integer.valueOf(contents[0]), document[1]);

                line = bufferedReader.readLine();
            }
            bufferedReader.close();
            fileReader.close();
        }
    }

    /**
     * values represent the source, destination pairs that have inputKey as its edge weight
     *
     * @param inputKey group_id
     * @param values   doc_id
     * @param context
     * @throws IOException
     * @throws InterruptedException
     */
    @Override
    public void reduce(Text inputKey, Iterable<Text> values, Context context)
            throws IOException, InterruptedException {

        StringBuilder stringBuilder = new StringBuilder();
        for (Text value : values) {
            int id = Integer.valueOf(value.toString());
            stringBuilder.append(corpus.get(id)).append("\n");
        }
        this.outputValue.set(stringBuilder.toString() + "\n");
        context.write(NullWritable.get(), this.outputValue);
    }
}

// End Step1Reducer.java
