package partition_test;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

//import java.nio.file.Path;

/**
 * Created by edwardlol on 2016/12/9.
 */
public class pDriver extends Configured implements Tool {
    //~ Methods ----------------------------------------------------------------

    @Override
    public int run(String[] args) throws Exception {

        if (args.length != 2) {
            System.err.printf("usage: %s [] <io file> <output dir>\n", getClass().getSimpleName());
            System.exit(1);
        }

        Configuration conf = new Configuration();
        conf.set("fs.defaultFS", "hdfs://localhost:9000/user/edwardlol");

        Job job = Job.getInstance(conf, "topsal");
//        job.setJarByClass(PartitionerExample.class);

        FileInputFormat.setInputPaths(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        job.setMapperClass(pMapper.class);

        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);

        //set partitioner statement
        job.setPartitionerClass(pPartitioner.class);
        job.setReducerClass(pReducer.class);
        job.setNumReduceTasks(3);
        job.setInputFormatClass(TextInputFormat.class);

        job.setOutputFormatClass(TextOutputFormat.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        System.exit(job.waitForCompletion(true) ? 0 : 1);
        return 0;
    }

    public static void main(String[] args) throws Exception {
        Configuration configuration = new Configuration();
        System.exit(ToolRunner.run(configuration, new pDriver(), args));
    }

}

// End pDriver.java
