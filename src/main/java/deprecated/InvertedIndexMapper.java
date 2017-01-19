package deprecated;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;
import java.util.StringTokenizer;

/**
 * the mapper class for calculating inverted index
 * <p>
 * Created by edwardlol on 2016/11/24.
 */
public class InvertedIndexMapper extends Mapper<LongWritable, Text, Text, LongWritable> {

    //~  Instance fields -------------------------------------------------------

    private Text outputKey = new Text();

    private LongWritable outputValue = new LongWritable();

    //~  Methods ---------------------------------------------------------------

    /**
     * @param key     document id
     * @param value   document content
     * @param context m-r context
     * @throws IOException
     * @throws InterruptedException
     */
    @Override
    public void map(LongWritable key, Text value, Context context)
            throws IOException, InterruptedException {

        // id@@g_no::g_name##g_model
        String line = value.toString();

        String[] content = line.split("::");


        String[] nameAndModel = content[1].split("##");

        StringTokenizer nameItr = new StringTokenizer(nameAndModel[0]);
        while (nameItr.hasMoreTokens()) {
            String term = nameItr.nextToken();

            this.outputKey.set(term);
            this.outputValue.set(key.get());
            context.write(this.outputKey, this.outputValue);
        }

        if (nameAndModel.length > 1) {
            StringTokenizer modelItr = new StringTokenizer(nameAndModel[1]);
            while (modelItr.hasMoreTokens()) {
                String term = modelItr.nextToken();

                this.outputKey.set(term);
                this.outputValue.set(key.get());
                context.write(this.outputKey, this.outputValue);
            }
        }
    }
}

// End InvertedIndexMapper.java
