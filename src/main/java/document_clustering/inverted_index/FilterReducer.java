package document_clustering.inverted_index;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * the reducer class for calculating inverted index
 * to combine all inverted indexes together
 * <p>
 * Created by edwardlol on 2016/11/25.
 */
public class FilterReducer extends Reducer<IntWritable, Text, Text, Text> {

    protected static final AtomicInteger count = new AtomicInteger(0);

    //~ Instance fields --------------------------------------------------------

    private Text outputKey = new Text();

    private Text outputValue = new Text();

    private int lineCnt;

    //~ Methods ----------------------------------------------------------------


    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        if (context.getCacheFiles() != null
                && context.getCacheFiles().length > 0) {

            FileReader fileReader = new FileReader("./lineCnt");
            BufferedReader bufferedReader = new BufferedReader(fileReader);

            String line = bufferedReader.readLine();
            this.lineCnt = Integer.parseInt(line);

            bufferedReader.close();
            fileReader.close();
        }
    }

    /**
     * @param key     cnt
     * @param values  term@@line_no=tf-idf
     * @param context
     * @throws IOException
     * @throws InterruptedException
     */
    @Override
    public void reduce(IntWritable key, Iterable<Text> values, Context context)
            throws IOException, InterruptedException {

        if (count.get() < lineCnt * 0.99) {

            for (Text value : values) {
                String[] content = value.toString().split("@@");

                this.outputKey.set(content[0]);
                this.outputValue.set(content[1]);
                // term \t line_no=tf-idf,...
                context.write(this.outputKey, this.outputValue);
                count.incrementAndGet();
            }
        }
    }
}

// End InvertedIndexReducer.java
