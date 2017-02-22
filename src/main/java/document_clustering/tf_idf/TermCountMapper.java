package document_clustering.tf_idf;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * Created by edwardlol on 2016/12/3.
 */
public class TermCountMapper extends Mapper<Text, Text, Text, IntWritable> {
    //~ Instance fields --------------------------------------------------------

    private Text outputKey = new Text();

    private final IntWritable outputValue = new IntWritable(1);

    //~ Methods ----------------------------------------------------------------

    /**
     * @param key     group_id
     * @param value   entry_id@@g_no::g_name##[g_model]
     * @param context
     * @throws IOException
     * @throws InterruptedException
     */
    @Override
    public void map(Text key, Text value, Context context)
            throws IOException, InterruptedException {

        String[] line = value.toString().split("::");

        String[] nameAndModel = line[1].split("##");

        if (nameAndModel.length >= 1) {
            String[] name = nameAndModel[0].split(" ");
            for (String term : name) {
                this.outputKey.set(term + "@@@" + line[0] + "@@" + key.toString() + "::title");
                // term@@@entry_id@@g_no@@group_id::title \t 1
                context.write(this.outputKey, this.outputValue);
            }
            if (nameAndModel.length == 2) {
                String[] model = nameAndModel[1].split(" ");
                for (String term : model) {
                    this.outputKey.set(term + "@@@" + line[0] + "@@" + key.toString() + "::content");
                    // term@@@entry_id@@g_no@@group_id::content \t 1
                    context.write(this.outputKey, this.outputValue);
                }
            }
        }
    }
}

// End TermCountMapper.java
