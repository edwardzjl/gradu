package document_clustering.linkback.step2;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.KeyValueTextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

/**
 * Created by edwardlol on 2016/12/2.
 */
public class Step2Driver extends Configured implements Tool {
    //~  Methods ---------------------------------------------------------------

    @Override
    public int run(String[] args) throws Exception {
        if (args.length < 3) {
            System.err.printf("usage: %s result_processed_dir bas_processed_dir output_dir\n"
                    , getClass().getSimpleName());
            System.exit(1);
        }

        Configuration conf = getConf();
        if (conf == null) {
            conf = new Configuration();
            conf.set("fs.defaultFS", "hdfs://localhost:9000/user/edwardlol");
        } else {
            conf.set("fs.hdfs.impl",
                    org.apache.hadoop.hdfs.DistributedFileSystem.class.getName());
            conf.set("fs.file.impl",
                    org.apache.hadoop.fs.LocalFileSystem.class.getName()
            );
        }

        // step 1 output
        conf.set("hdfs://localhost:9000" + args[0], "1");
        // pre step output
        conf.set("hdfs://localhost:9000" + args[1], "2");

        Job job = Job.getInstance(conf, "result job");
        job.setJarByClass(Step2Driver.class);

        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileInputFormat.addInputPath(job, new Path(args[1]));

        job.setInputFormatClass(KeyValueTextInputFormat.class);

        job.setMapperClass(Step2Mapper.class);
        job.setMapOutputKeyClass(Step2KeyWritable.class);
        job.setMapOutputValueClass(Text.class);

        job.setPartitionerClass(Step2Partitioner.class);
        job.setGroupingComparatorClass(Step2GroupingComparator.class);

        job.setReducerClass(Step2Reducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        FileOutputFormat.setOutputPath(job, new Path(args[2]));

        return job.waitForCompletion(true) ? 0 : 1;
    }

    //~  Entrance --------------------------------------------------------------

    public static void main(String[] args) throws Exception {
        Configuration configuration = new Configuration();
        System.exit(ToolRunner.run(configuration, new Step2Driver(), args));
    }
}

// End Step1Driver.java
