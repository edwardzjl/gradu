package document_clustering.deprecated;

import document_clustering.util.LineCountMapper;
import document_clustering.util.LineCountReducer;
import document_clustering.writables.tuple_writables.IntDoubleTupleWritable;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.jobcontrol.ControlledJob;
import org.apache.hadoop.mapreduce.lib.jobcontrol.JobControl;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import java.net.URI;

/**
 * Created by edwardlol on 2016/11/30.
 */
public class VectorDriver extends Configured implements Tool {
    //~  Methods ---------------------------------------------------------------

    public static void main(String[] args) throws Exception {
        Configuration configuration = new Configuration();
        System.exit(ToolRunner.run(configuration, new VectorDriver(), args));
    }

    //~  Entrance --------------------------------------------------------------

    @Override
    public int run(String[] args) throws Exception {
        if (args.length < 3) {
            System.err.printf("usage: %s tf_idf_result_dir inverted_index_result_dir output_dir\n",
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

        String totalWordsCountFile = args[2] + "/totalWordsCount";

        JobControl jobControl = new JobControl("vector jobs");

        // pre step configuration
        Job preJob = Job.getInstance(conf, "vector pre step job");
        preJob.setJobName("vector pre step job");
        preJob.setJarByClass(LineCountMapper.class);

        FileInputFormat.addInputPath(preJob, new Path(args[1]));
        FileOutputFormat.setOutputPath(preJob, new Path(totalWordsCountFile));

        preJob.setMapperClass(LineCountMapper.class);
        preJob.setMapOutputKeyClass(Text.class);
        preJob.setMapOutputValueClass(IntWritable.class);

        preJob.setReducerClass(LineCountReducer.class);
        preJob.setOutputKeyClass(NullWritable.class);
        preJob.setOutputValueClass(IntWritable.class);

        ControlledJob controlledPreJob = new ControlledJob(conf);
        controlledPreJob.setJob(preJob);
        jobControl.addJob(controlledPreJob);

        // job configuration
        Job job = Job.getInstance(conf, "vector job");
        job.setJobName("vector job");
        job.setJarByClass(VectorMapper.class);

        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[2] + "/result"));

        job.addCacheFile(new URI(args[1] + "/part-r-00000#invertedIndex"));
        job.addCacheFile(new URI(totalWordsCountFile + "/part-r-00000#totalWordsCount"));

        job.setMapperClass(VectorMapper.class);
        job.setMapOutputKeyClass(LongWritable.class);
        job.setMapOutputValueClass(IntDoubleTupleWritable.class);

        job.setReducerClass(VectorReducer.class);
        job.setOutputKeyClass(LongWritable.class);
        job.setOutputValueClass(Text.class);

        ControlledJob controlledJob1 = new ControlledJob(conf);
        controlledJob1.setJob(job);
        controlledJob1.addDependingJob(controlledPreJob);

        jobControl.addJob(controlledJob1);

        Thread jobRunnerThread = new Thread(jobControl);
        long starttime = System.currentTimeMillis();
        jobRunnerThread.start();

        while (!jobControl.allFinished()) {
            System.out.println("Jobs in waiting state: ");
            for (ControlledJob controlledJob : jobControl.getWaitingJobList()) {
                System.out.println("\t" + controlledJob.toString());
            }
            System.out.println("Jobs in ready state: ");
            for (ControlledJob controlledJob : jobControl.getReadyJobsList()) {
                System.out.println("\t" + controlledJob.toString());
            }
            System.out.println("Jobs in running state: ");
            for (ControlledJob controlledJob : jobControl.getRunningJobList()) {
                System.out.println("\t" + controlledJob.toString());
            }
            System.out.println("Jobs in success state: ");
            for (ControlledJob controlledJob : jobControl.getSuccessfulJobList()) {
                System.out.println("\t" + controlledJob.toString());
            }
            System.out.println("Jobs in failed state: ");
            for (ControlledJob controlledJob : jobControl.getFailedJobList()) {
                System.out.println("\t" + controlledJob.toString());
            }
            try {
                Thread.sleep(5000);
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
        long endtime = System.currentTimeMillis();
        System.out.println("vector job finished in: " + (endtime - starttime) / 1000 + " seconds");

        jobControl.stop();

        return job.waitForCompletion(true) ? 0 : 1;
    }
}

// End VectorDriver.java
