package document_clustering.deprecated;

import document_clustering.util.Util;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * init the corpus, read the defect lib csv file and extract the "description" column as documents
 * put all documents in one big file, not messive small files, because that's what hadoop is good at
 * each line is a document, in form of 'id\tcontent\n'.
 * giving each document a unique id using {@link AtomicInteger#getAndIncrement()}
 * it would be better if I can get documents' id from defect lib, so that I don't need to
 * use {@link AtomicInteger#getAndIncrement()}, and the mapper would be more pararrel and more fast.
 * but I can't
 * <p>
 * Created by edwardlol on 2016/11/25.
 */
public class InitSgridMapper extends Mapper<LongWritable, Text, Text, Text> {
    //~ Static fields/initializers ---------------------------------------------

//    protected static final AtomicInteger count = new AtomicInteger(0);

    //~  Instance fields -------------------------------------------------------

    private Text outputKey = new Text();

    private Text outputValue = new Text();

    //~  Methods ---------------------------------------------------------------

    @Override
    public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
//        int id = count.incrementAndGet();

        String line = value.toString();
        String[] contents = line.split(",");
        this.outputKey.set(contents[0] + "@" + contents[1]);
        List<String> seperatedDescriptionList = Util.seperate(contents[16]);
        StringBuilder seperatedDescription = new StringBuilder();
        for (int i = 0; i < seperatedDescriptionList.size(); i++) {
            seperatedDescription.append(seperatedDescriptionList.get(i));
            if (i < seperatedDescriptionList.size() - 1) {
                seperatedDescription.append(" ");
            }
        }
//            this.outputKey.set(id);
        this.outputValue.set(seperatedDescription.toString());
        context.write(this.outputKey, this.outputValue);

    }
}

// End InitMapper.java
