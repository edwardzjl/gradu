package document_clustering.debug;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.KeyValueTextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

/**
 * read the big document file and calculate the inverted index
 * the inverted index is formatted like: term \t [document_id...]
 * the document id is represented as line number in the big document file
 * <p>
 * Created by edwardlol on 2016/11/27.
 */
public class InvertedIndexDriver2 extends Configured implements Tool {
    //~  Methods ---------------------------------------------------------------

    public static void main(String[] args) throws Exception {
        Configuration configuration = new Configuration();
        System.exit(ToolRunner.run(configuration, new InvertedIndexDriver2(), args));
    }

    //~  Entrance --------------------------------------------------------------

    @Override
    public int run(String[] args) throws Exception {
        if (args.length != 2) {
            System.err.printf("usage: %s tf_idf_result_dir output_dir\n", getClass().getSimpleName());
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

        Job job = Job.getInstance(conf, "inverted index job");
        job.setJobName("inverted index job");
        job.setJarByClass(InvertedIndexMapper3.class);

        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        job.setInputFormatClass(KeyValueTextInputFormat.class);

        job.setMapperClass(InvertedIndexMapper3.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(IntWritable.class);

        job.setReducerClass(InvertedIndexReducer3.class);
        job.setOutputKeyClass(NullWritable.class);
        job.setOutputValueClass(NullWritable.class);

        long starttime = System.currentTimeMillis();
        boolean complete = job.waitForCompletion(true);
        long endtime = System.currentTimeMillis();
        System.out.println("inverted index job finished in: " + (endtime - starttime) / 1000 + " seconds");

        return complete ? 0 : 1;
    }
}

// End InvertedIndexDriver2.java
