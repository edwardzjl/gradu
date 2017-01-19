package document_clustering.simhash;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.KeyValueTextInputFormat;
import org.apache.hadoop.mapreduce.lib.jobcontrol.ControlledJob;
import org.apache.hadoop.mapreduce.lib.jobcontrol.JobControl;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

/**
 * Created by edwardlol on 2016/12/3.
 */
public class SimHashDriver extends Configured implements Tool {
    //~  Methods ---------------------------------------------------------------

    @Override
    public int run(String[] args) throws Exception {
        if (args.length < 2) {
            System.err.printf("usage: %s init_result_dir output_dir [simhash_threshold]\n",
                    getClass().getSimpleName());
            System.exit(1);
        }

        Path step1_outputDir = new Path(args[1] + "/step1");

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

        conf.set("mapreduce.reduce.memory.mb", "4096");
        conf.set("mapred.child.java.opts", "-Xmx3072m");

        if (args.length > 2) {
            conf.setInt("simhash.threshold", Integer.valueOf(args[2]));
        } else {
            conf.setInt("simhash.threshold", 3);
        }

        JobControl jobControl = new JobControl("simhash jobs");

        Job job1 = Job.getInstance(conf, "simhash step1 job");
        job1.setJarByClass(SimHashDriver.class);

        FileInputFormat.addInputPath(job1, new Path(args[0]));
        job1.setInputFormatClass(KeyValueTextInputFormat.class);

        job1.setMapperClass(SimHashStep1Mapper.class);
        job1.setMapOutputKeyClass(LongWritable.class);
        job1.setMapOutputValueClass(Text.class);

        job1.setReducerClass(SimHashStep1Reducer.class);
        job1.setOutputKeyClass(IntWritable.class);
        job1.setOutputValueClass(Text.class);

        FileOutputFormat.setOutputPath(job1, step1_outputDir);

        ControlledJob controlledJob1 = new ControlledJob(conf);
        controlledJob1.setJob(job1);
        jobControl.addJob(controlledJob1);


        Job job2 = Job.getInstance(conf, "simhash step2 job");
        job2.setJarByClass(SimHashDriver.class);

        FileInputFormat.addInputPath(job2, step1_outputDir);
        job2.setInputFormatClass(KeyValueTextInputFormat.class);

        job2.setMapperClass(SimHashStep2Mapper.class);
        job2.setMapOutputKeyClass(IntWritable.class);
        job2.setMapOutputValueClass(Text.class);

        job2.setReducerClass(SimHashStep2Reducer.class);
        job2.setOutputKeyClass(IntWritable.class);
        job2.setOutputValueClass(Text.class);

        FileOutputFormat.setOutputPath(job2, new Path(args[1] + "/result"));

        ControlledJob controlledJob2 = new ControlledJob(conf);
        controlledJob2.setJob(job2);
        controlledJob2.addDependingJob(controlledJob1);
        jobControl.addJob(controlledJob2);

        runJobs(jobControl);

        long starttime = System.currentTimeMillis();
        boolean complete = job2.waitForCompletion(true);
        long endtime = System.currentTimeMillis();
        System.out.println("simhash job finished in: " + (endtime - starttime) / 1000 + " seconds");

        return complete ? 0 : 1;
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
        System.out.println("tf-idf jobs finished in: " + (endtime - starttime) / 1000 + " seconds");

        jobControl.stop();
    }

    //~  Entrance --------------------------------------------------------------

    public static void main(String[] args) throws Exception {
        Configuration configuration = new Configuration();
        System.exit(ToolRunner.run(configuration, new SimHashDriver(), args));
    }
}

// End SimHashDriver.java
