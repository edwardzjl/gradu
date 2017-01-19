package document_clustering.deprecated;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import java.net.URI;

/**
 * Created by edwardlol on 2016/12/7.
 */
public class SplitedSimilarityDriver extends Configured implements Tool {
    //~  Methods ---------------------------------------------------------------

    public static void main(String[] args) throws Exception {
        Configuration configuration = new Configuration();
        System.exit(ToolRunner.run(configuration, new SplitedSimilarityDriver(), args));
    }

    //~  Entrance --------------------------------------------------------------

    @Override
    public int run(String[] args) throws Exception {
        if (args.length < 3) {
            System.err.printf("usage: %s vector_result_dir document_count_dir output_dir " +
                            "[split_number] [output_threshold]\n",
                    getClass().getSimpleName());
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

        String splitNumber = args.length > 3 ? args[3] : "3";
        conf.set("split.number", splitNumber);

        String outputThreshold = args.length > 4 ? args[4] : "0.99";
        conf.set("output.threshold", outputThreshold);

        Job job = Job.getInstance(conf, "split similaruty job");
        job.setJobName("split similaruty job");
        job.setJarByClass(SplitedSimilarityMapper.class);

        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[2]));

        job.addCacheFile(new URI(args[1] + "/part-r-00000#docs"));

        job.setMapperClass(SplitedSimilarityMapper.class);
        job.setMapOutputKeyClass(IntWritable.class);
        job.setMapOutputValueClass(Text.class);

        job.setReducerClass(SplitedSimilarityReducer.class);
        job.setNumReduceTasks(Integer.parseInt(splitNumber));
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(DoubleWritable.class);

        long starttime = System.currentTimeMillis();
        boolean complete = job.waitForCompletion(true);
        long endtime = System.currentTimeMillis();
        System.out.println("split similarity job finished in: " + (endtime - starttime) / 1000 + " seconds");

        return complete ? 0 : 1;
    }
}

// End SplitedSimilarityDriver.java
