package document_clustering.deprecated;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

/**
 * read the defect lib csv file and get the contents in "description" field, each line as a document
 * then seperate the terms of each document, writables them in the format of id, term...
 * <p>
 * Created by edwardlol on 2016/11/27.
 */
public class InitDriverSougou extends Configured implements Tool {
    //~  Methods ---------------------------------------------------------------

    public static void main(String[] args) throws Exception {
        Configuration configuration = new Configuration();
        System.exit(ToolRunner.run(configuration, new InitDriverSougou(), args));
    }

    //~  Entrance --------------------------------------------------------------

    @Override
    public int run(String[] args) throws Exception {
        if (args.length != 2) {
            System.err.printf("usage: %s [] <io file> <output dir>\n", getClass().getSimpleName());
            System.exit(1);
        }

        Configuration conf = new Configuration();
        conf.set("fs.defaultFS", "hdfs://localhost:9000/user/edwardlol");

        Job job = Job.getInstance(conf, "init job");

        job.setMapperClass(InitSougouMapper.class);
        job.setNumReduceTasks(0);
        job.setOutputKeyClass(LongWritable.class);
        job.setOutputValueClass(Text.class);

        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileInputFormat.setInputDirRecursive(job, true);

        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        return job.waitForCompletion(true) ? 0 : 1;
    }
}

// End InitDriver.java
