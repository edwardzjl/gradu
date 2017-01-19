package document_clustering.deprecated;

import document_clustering.writables.tuple_writables.IntDoubleTupleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.util.Iterator;

/**
 * Created by edwardlol on 2016/12/2.
 */
public class VectorReducer extends Reducer<LongWritable, IntDoubleTupleWritable, LongWritable, Text> {
    //~ Instance fields --------------------------------------------------------

    private LongWritable outputKey = new LongWritable();

    private Text outputValue = new Text();

    private int featureSize;

    //~ Methods ----------------------------------------------------------------

    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        if (context.getCacheFiles() != null
                && context.getCacheFiles().length > 0) {

            File file = new File("./totalWordsCount");
            FileReader fileReader = new FileReader(file);
            BufferedReader bufferedReader = new BufferedReader(fileReader);

            String line = bufferedReader.readLine();
            this.featureSize = Integer.parseInt(line);

            bufferedReader.close();
            fileReader.close();
        }
        super.setup(context);
    }

    @Override
    protected void reduce(LongWritable key, Iterable<IntDoubleTupleWritable> values, Context context)
            throws IOException, InterruptedException {

        this.outputKey.set(key.get());

        StringBuilder output = new StringBuilder();
        output.append(featureSize).append('{');

        Iterator<IntDoubleTupleWritable> iterator = values.iterator();
        while (iterator.hasNext()) {
            IntDoubleTupleWritable tuple = iterator.next();
            output.append(tuple.getLeftValue()).append(':').append(tuple.getRightValue());
            if (iterator.hasNext()) {
                output.append(',');
            }
        }
        output.append('}');

        this.outputValue.set(output.toString());
        context.write(this.outputKey, this.outputValue);
    }

}

// End VectorReducer3.java
