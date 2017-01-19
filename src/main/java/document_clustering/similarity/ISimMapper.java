package document_clustering.similarity;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * Created by edwardlol on 2016/12/2.
 */
public class ISimMapper extends Mapper<Text, Text, Text, Text> {

    private Text outputValue = new Text();

    //~ Methods ----------------------------------------------------------------

    /**
     * @param key     src,dest
     * @param value   term:sim
     * @param context
     * @throws IOException
     * @throws InterruptedException
     */
    @Override
    public void map(Text key, Text value, Context context)
            throws IOException, InterruptedException {

        String[] content = value.toString().split(":");
        String[] w1w2 = content[1].split("\\*");

        double sim = Double.valueOf(w1w2[0]) * Double.valueOf(w1w2[1]);

        this.outputValue.set(content[0] + ":" + sim);
//         src,dest \t term:sim
        context.write(key, this.outputValue);
//        context.write(key, value);
    }
}

// End PreMapper.java
