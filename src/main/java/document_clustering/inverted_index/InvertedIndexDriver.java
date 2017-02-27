package document_clustering.inverted_index;

import document_clustering.util.Util;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.KeyValueTextInputFormat;
import org.apache.hadoop.mapreduce.lib.jobcontrol.ControlledJob;
import org.apache.hadoop.mapreduce.lib.jobcontrol.JobControl;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;

/**
 * read the big document file and calculate the inverted index
 * the inverted index is formatted like: term \t [document_id...]
 * the document id is represented as line number in the big document file
 * <p>
 * Created by edwardlol on 2016/11/27.
 */
public class InvertedIndexDriver extends Configured implements Tool {
    //~  Methods ---------------------------------------------------------------

    @Override
    public int run(String[] args) throws Exception {
        if (args.length != 2) {
            System.err.printf("usage: %s tf_idf_result_dir output_dir\n",
                    getClass().getSimpleName());
            System.exit(1);
        }

        Path normDir = new Path(args[1] + "/normed");
        Path resultDir = new Path(args[1] + "/result");

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

        /* load configs */
        Properties prop = new Properties();
        InputStream input = null;
        try {
            String filename = "iindex.properties";
            input = InvertedIndexDriver.class.getClassLoader().getResourceAsStream(filename);
            if (input == null) {
                System.out.println("Sorry, unable to find " + filename);
            }
            prop.load(input);
            conf.setInt("deci.number", Integer.valueOf(prop.getProperty("deci.number")));
            conf.setBoolean("filter.tf_idf", Boolean.valueOf(prop.getProperty("filter.tf_idf")));
        } catch (IOException ex) {
            ex.printStackTrace();
        } finally {
            if (input != null) {
                try {
                    input.close();
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
        }

        JobControl jobControl = new JobControl("inverted-index jobs");

        /* step 1, normalize the vector lenth of each document */

        Job job1 = Job.getInstance(conf, "tf idf normalizer job");
        job1.setJarByClass(NormalizerMapper.class);

        FileInputFormat.addInputPath(job1, new Path(args[0]));
        job1.setInputFormatClass(KeyValueTextInputFormat.class);

        job1.setMapperClass(NormalizerMapper.class);

        job1.setReducerClass(NormalizerReducer.class);
        job1.setOutputKeyClass(Text.class);
        job1.setOutputValueClass(Text.class);

        FileOutputFormat.setOutputPath(job1, normDir);

        ControlledJob controlledJob1 = new ControlledJob(conf);
        controlledJob1.setJob(job1);
        jobControl.addJob(controlledJob1);

        /* step 2, calculate inverted index */

        Job job2 = Job.getInstance(conf, "inverted index job");
        job2.setJarByClass(InvertedIndexDriver.class);

        FileInputFormat.addInputPath(job2, normDir);

        job2.setInputFormatClass(KeyValueTextInputFormat.class);

        job2.setMapperClass(Mapper.class);

        job2.setReducerClass(InvertedIndexReducer.class);
        job2.setOutputKeyClass(Text.class);
        job2.setOutputValueClass(Text.class);

        FileOutputFormat.setOutputPath(job2, resultDir);

        ControlledJob controlledJob2 = new ControlledJob(conf);
        controlledJob2.setJob(job2);
        controlledJob2.addDependingJob(controlledJob1);
        jobControl.addJob(controlledJob2);

        Util.runJobs(jobControl);

        return job2.waitForCompletion(true) ? 0 : 1;
    }

    //~  Entrance --------------------------------------------------------------

    public static void main(String[] args) throws Exception {
        Configuration configuration = new Configuration();
        System.exit(ToolRunner.run(configuration, new InvertedIndexDriver(), args));
    }
}

// End InvertedIndexDriver.java
