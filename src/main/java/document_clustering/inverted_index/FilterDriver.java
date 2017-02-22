package document_clustering.inverted_index;

import document_clustering.util.LineCountMapper;
import document_clustering.util.LineCountReducer;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
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
 * Created by edwardlol on 2016/12/5.
 */
public class FilterDriver extends Configured implements Tool {
    //~  Methods ---------------------------------------------------------------

    @Override
    public int run(String[] args) throws Exception {
        if (args.length < 2) {
            System.err.printf("usage: %s inverted_index_result_dir output_dir\n",
                    getClass().getSimpleName());
            System.exit(1);
        }

        Path lineCntDir = new Path(args[1] + "/totalTerms");

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

        JobControl jobControl = new JobControl("iindex filter jobs");

        // step 1, count all terms in this corpus
        Job cntJob = Job.getInstance(conf, "term count step");
        cntJob.setJarByClass(FilterDriver.class);

        FileInputFormat.addInputPath(cntJob, new Path(args[0]));

        cntJob.setMapperClass(LineCountMapper.class);
        cntJob.setCombinerClass(LineCountReducer.class);

        cntJob.setReducerClass(LineCountReducer.class);
        cntJob.setOutputKeyClass(NullWritable.class);
        cntJob.setOutputValueClass(IntWritable.class);

        FileOutputFormat.setOutputPath(cntJob, lineCntDir);

        ControlledJob controlledPreJob = new ControlledJob(conf);
        controlledPreJob.setJob(cntJob);
        jobControl.addJob(controlledPreJob);

        // step 2 configuration
        Job job1 = Job.getInstance(conf, "filter job");
        job1.setJarByClass(FilterDriver.class);

        FileInputFormat.addInputPath(job1, new Path(args[0]));
        job1.addCacheFile(new URI(lineCntDir + "/part-r-00000#totalTerms"));
        job1.setInputFormatClass(KeyValueTextInputFormat.class);

        job1.setMapperClass(FilterMapper.class);
        job1.setMapOutputKeyClass(IntWritable.class);
        job1.setMapOutputValueClass(Text.class);

        job1.setReducerClass(FilterReducer.class);
        job1.setOutputKeyClass(Text.class);
        job1.setOutputValueClass(Text.class);

        FileOutputFormat.setOutputPath(job1, new Path(args[1] + "/result"));

        ControlledJob controlledJob1 = new ControlledJob(conf);
        controlledJob1.setJob(job1);
        controlledJob1.addDependingJob(controlledPreJob);
        jobControl.addJob(controlledJob1);

        // run jobs
        runJobs(jobControl);
        return job1.waitForCompletion(true) ? 0 : 1;
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
        System.exit(ToolRunner.run(configuration, new FilterDriver(), args));
    }
}

// End FilterDriver.java
