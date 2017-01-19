package document_clustering.deprecated;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;

/**
 * Created by edwardlol on 2016/12/2.
 */
public class SplitedSimilarityMapper extends Mapper<LongWritable, Text, IntWritable, Text> {

    private final IntWritable outputKey = new IntWritable();

    private int splitNumber;

    private int docsInSegment;

    //~ Methods ----------------------------------------------------------------

    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        Configuration conf = context.getConfiguration();
        this.splitNumber = Integer.parseInt(conf.get("split.number"));

        FileReader fileReader = new FileReader("./docs");
        BufferedReader bufferedReader = new BufferedReader(fileReader);
        String line = bufferedReader.readLine();

        this.docsInSegment = Integer.parseInt(line) / this.splitNumber + 1;

        bufferedReader.close();
        fileReader.close();
    }

    @Override
    public void map(LongWritable key, Text value, Context context)
            throws IOException, InterruptedException {

        String[] line = value.toString().split("\t");

        int docId = Integer.parseInt(line[0]);


        for (int i = 1; i < splitNumber; i++) {
            for (int j = i + 1; j < splitNumber; j++) {

            }
        }


        int segmentNo_i = (docId - 1) / this.docsInSegment;
        int segmentNo_j = (segmentNo_i + 1) % this.splitNumber;

        this.outputKey.set(segmentNo_i);
        context.write(this.outputKey, value);
        this.outputKey.set(segmentNo_j);
        context.write(this.outputKey, value);
    }
}

// End SimilarityMapper.java
