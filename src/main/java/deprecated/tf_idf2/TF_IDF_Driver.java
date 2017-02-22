package deprecated.tf_idf2;

import document_clustering.util.LineCountMapper;
import document_clustering.util.LineCountReducer;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.KeyValueTextInputFormat;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileAsTextInputFormat;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.jobcontrol.ControlledJob;
import org.apache.hadoop.mapreduce.lib.jobcontrol.JobControl;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import java.net.URI;

/**
 * Created by edwardlol on 2016/12/5.
 */
public class TF_IDF_Driver extends Configured implements Tool {
    //~  Methods ---------------------------------------------------------------

    @Override
    public int run(String[] args) throws Exception {
        if (args.length < 2) {
            System.err.printf("usage: %s simhash_result_dir output_dir " +
                            "[gname_weight] [docCnt_output_dir]\n",
                    getClass().getSimpleName());
            System.exit(1);
        }

        Path step1_outputDir = new Path(args[1] + "/step1");
        Path step2_outputDir = new Path(args[1] + "/step2");
        Path step3_outputDir = new Path(args[1] + "/step3");

        String docCntDir = args.length > 3 ? args[3] : args[1] + "/docCount";

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

        conf.set("yarn.app.mapreduce.am.resource.mb", "1024");
        conf.set("yarn.app.mapreduce.am.command-opts", "-Xmx768m");

        conf.set("mapreduce.reduce.memory.mb", "4096");

        conf.set("mapreduce.task.io.sort.mb", "300");
        conf.set("mapreduce.task.io.sort.factor", "30");

        conf.setBoolean("mapreduce.map.output.compress", true);
        conf.set("mapreduce.map.output.compress.codec", "com.hadoop.compression.lzo.LzoCodec");

        String gNameWeight = args.length > 2 ? args[2] : "1.0";
        conf.setDouble("gname.weight", Double.valueOf(gNameWeight));

        JobControl jobControl = new JobControl("tf-idf jobs");

        // pre step configuration
        Job preJob = Job.getInstance(conf, "tf idf pre step");
        preJob.setJarByClass(LineCountMapper.class);

        FileInputFormat.addInputPath(preJob, new Path(args[0]));
        FileOutputFormat.setOutputPath(preJob, new Path(docCntDir));

        preJob.setMapperClass(LineCountMapper.class);

        preJob.setCombinerClass(LineCountReducer.class);

        preJob.setReducerClass(LineCountReducer.class);
        preJob.setOutputKeyClass(NullWritable.class);
        preJob.setOutputValueClass(IntWritable.class);

        ControlledJob controlledPreJob = new ControlledJob(conf);
        controlledPreJob.setJob(preJob);
        jobControl.addJob(controlledPreJob);

        // step 1 configuration
        Job job1 = Job.getInstance(conf, "tf idf step1 job");
        job1.setJarByClass(TermCountMapper.class);

        FileInputFormat.addInputPath(job1, new Path(args[0]));

        job1.setInputFormatClass(KeyValueTextInputFormat.class);

        job1.setMapperClass(TermCountMapper.class);
        job1.setMapOutputKeyClass(Text.class);
        job1.setMapOutputValueClass(IntWritable.class);

        job1.setCombinerClass(TermCountReducer.class);

        job1.setReducerClass(TermCountReducer.class);
        job1.setOutputKeyClass(Text.class);
        job1.setOutputValueClass(IntWritable.class);

//        FileOutputFormat.setOutputPath(job1, step1_outputDir);

        job1.setOutputFormatClass(SequenceFileOutputFormat.class);
        SequenceFileOutputFormat.setOutputCompressionType(job1, SequenceFile.CompressionType.BLOCK);
        SequenceFileOutputFormat.setOutputCompressorClass(job1, com.hadoop.compression.lzo.LzoCodec.class);
        SequenceFileOutputFormat.setOutputPath(job1, step1_outputDir);

        ControlledJob controlledJob1 = new ControlledJob(conf);
        controlledJob1.setJob(job1);
        jobControl.addJob(controlledJob1);

        // step 2 configuration
        Job job2 = Job.getInstance(conf, "tf idf step2 job");
        job2.setJarByClass(TermFrequencyMapper.class);

//        FileInputFormat.addInputPath(job2, step1_outputDir);
//        job2.setInputFormatClass(KeyValueTextInputFormat.class);
        SequenceFileInputFormat.addInputPath(job2, step1_outputDir);
        job2.setInputFormatClass(SequenceFileAsTextInputFormat.class);

        job2.setMapperClass(TermFrequencyMapper.class);
        job2.setMapOutputKeyClass(Text.class);
        job2.setMapOutputValueClass(Text.class);

        // TODO: 2016/12/21 maybe add a combiner here
        job2.setReducerClass(TermFrequencyReducer2.class);
        job2.setOutputKeyClass(Text.class);
        job2.setOutputValueClass(Text.class);

        job2.setOutputFormatClass(SequenceFileOutputFormat.class);
        SequenceFileOutputFormat.setOutputCompressionType(job2, SequenceFile.CompressionType.BLOCK);
        SequenceFileOutputFormat.setOutputCompressorClass(job2, com.hadoop.compression.lzo.LzoCodec.class);
        SequenceFileOutputFormat.setOutputPath(job2, step2_outputDir);

        ControlledJob controlledJob2 = new ControlledJob(conf);
        controlledJob2.setJob(job2);
        controlledJob2.addDependingJob(controlledJob1);
        jobControl.addJob(controlledJob2);

        // step 3 configuration
        Job job3 = Job.getInstance(conf, "tf idf step3 job");
        job3.setJarByClass(TF_IDF_Reducer2.class);

        job3.addCacheFile(new URI(docCntDir + "/part-r-00000#docCnt"));

        SequenceFileInputFormat.addInputPath(job3, step2_outputDir);
        job3.setInputFormatClass(SequenceFileAsTextInputFormat.class);

//        job3.setMapperClass(TF_IDF_Mapper.class);
//        job3.setMapOutputKeyClass(Text.class);
//        job3.setMapOutputValueClass(Text.class);

        job3.setReducerClass(TF_IDF_Reducer2.class);
        job3.setNumReduceTasks(5);
        job3.setOutputKeyClass(Text.class);
        job3.setOutputValueClass(DoubleWritable.class);

        FileOutputFormat.setOutputPath(job3, step3_outputDir);

        ControlledJob controlledJob3 = new ControlledJob(conf);
        controlledJob3.setJob(job3);
        controlledJob3.addDependingJob(controlledJob2);
        controlledJob3.addDependingJob(controlledPreJob);

        jobControl.addJob(controlledJob3);

        // normalizer job
        Job job4 = Job.getInstance(conf, "tf idf normalizer job");
        job4.setJarByClass(NormalizerMapper.class);

        FileInputFormat.addInputPath(job4, step3_outputDir);
        job4.setInputFormatClass(KeyValueTextInputFormat.class);

        job4.setMapperClass(NormalizerMapper.class);

        job4.setReducerClass(NormalizerReducer.class);
        job4.setOutputKeyClass(Text.class);
        job4.setOutputValueClass(Text.class);

        FileOutputFormat.setOutputPath(job4, new Path(args[1] + "/result"));

        ControlledJob controlledJob4 = new ControlledJob(conf);
        controlledJob4.setJob(job4);
        controlledJob4.addDependingJob(controlledJob3);

        jobControl.addJob(controlledJob4);

        // run jobs
        runJobs(jobControl);

        return job4.waitForCompletion(true) ? 0 : 1;
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
        System.exit(ToolRunner.run(configuration, new TF_IDF_Driver(), args));
    }
}

// End TF_IDF_Driver.java