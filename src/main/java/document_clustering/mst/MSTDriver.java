package document_clustering.mst;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.KeyValueTextInputFormat;
import org.apache.hadoop.mapreduce.lib.jobcontrol.ControlledJob;
import org.apache.hadoop.mapreduce.lib.jobcontrol.JobControl;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import java.net.URI;

/**
 * Created by edwardlol on 2016/12/2.
 */
public class MSTDriver extends Configured implements Tool {
    //~  Methods ---------------------------------------------------------------

    @Override
    public int run(String[] args) throws Exception {
        if (args.length < 3) {
            System.err.printf("usage: %s similarity_result_dir document_count_file output_dir " +
                            "[split_number] [cluster_threshold]\n"
                    , getClass().getSimpleName());
            System.exit(1);
        }

        Path step1_OutputDir = new Path(args[2] + "/step1");
        Path resultDir = new Path(args[2] + "/result");

        URI docCntFile = new URI(args[1] + "/part-r-00000#docCnt");

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

        conf.set("mapreduce.map.memory.mb", "1024");
        conf.set("mapreduce.reduce.memory.mb", "4096");
        conf.set("mapred.child.java.opts", "-Xmx768m");

        if (args.length > 3) {
            conf.setInt("reduce.task.num", Integer.valueOf(args[3]));
        } else {
            conf.setInt("reduce.task.num", 3);
        }
        if (args.length > 4) {
            conf.setDouble("final.threshold", Double.valueOf(args[4]));
        } else {
            conf.setDouble("final.threshold", 0.2d);
        }

        JobControl jobControl = new JobControl("mst jobs");

        /* step 1, split and calculate the child msts */

        Job childJob = Job.getInstance(conf, "mst child job");
        childJob.setJarByClass(MSTDriver.class);

        childJob.addCacheFile(docCntFile);

        FileInputFormat.addInputPath(childJob, new Path(args[0]));
        FileOutputFormat.setOutputPath(childJob, step1_OutputDir);

        childJob.setInputFormatClass(KeyValueTextInputFormat.class);

        childJob.setMapperClass(MSTChildMapper.class);
        childJob.setMapOutputKeyClass(DoubleWritable.class);
        childJob.setMapOutputValueClass(Text.class);

        childJob.setPartitionerClass(MSTChildPartitioner.class);

        childJob.setReducerClass(MSTChildReducer.class);
        childJob.setNumReduceTasks(conf.getInt("reduce.task.num", 1));
        childJob.setOutputKeyClass(DoubleWritable.class);
        childJob.setOutputValueClass(Text.class);

        ControlledJob controlledChildJob = new ControlledJob(conf);
        controlledChildJob.setJob(childJob);
        jobControl.addJob(controlledChildJob);

        /* step 2, merge step 1's output and calculate final mst */

        Job finalJob = Job.getInstance(conf, "mst final job");
        finalJob.setJarByClass(MSTFinalReducer.class);

        finalJob.addCacheFile(docCntFile);

        FileInputFormat.addInputPath(finalJob, step1_OutputDir);
        finalJob.setInputFormatClass(KeyValueTextInputFormat.class);

        finalJob.setMapperClass(MSTFinalMapper.class);
        finalJob.setMapOutputKeyClass(DoubleWritable.class);
        finalJob.setMapOutputValueClass(Text.class);


        finalJob.setReducerClass(MSTFinalReducer.class);
        finalJob.setOutputKeyClass(IntWritable.class);
        finalJob.setOutputValueClass(IntWritable.class);

        FileOutputFormat.setOutputPath(finalJob, resultDir);

        ControlledJob finalControlledJob = new ControlledJob(conf);
        finalControlledJob.setJob(finalJob);
        finalControlledJob.addDependingJob(controlledChildJob);
        jobControl.addJob(finalControlledJob);

        // run jobs
        runJobs(jobControl);

        return finalJob.waitForCompletion(true) ? 0 : 1;
    }

    private void runJobs(JobControl jobControl) {
        Thread jobRunnerThread = new Thread(jobControl);
        long starttime = System.currentTimeMillis();
        jobRunnerThread.start();

        while (!jobControl.allFinished()) {
            try {
                Thread.sleep(1000);
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
        long endtime = System.currentTimeMillis();
        System.out.println("mst job finished in: " + (endtime - starttime) / 1000 + " seconds");

        jobControl.stop();
    }

    //~  Entrance --------------------------------------------------------------

    public static void main(String[] args) throws Exception {
        Configuration configuration = new Configuration();
        System.exit(ToolRunner.run(configuration, new MSTDriver(), args));
    }
}

// End MSTDriver.java
