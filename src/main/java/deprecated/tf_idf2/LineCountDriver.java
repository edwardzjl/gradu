package deprecated.tf_idf2;

import document_clustering.util.LineCountMapper;
import document_clustering.util.LineCountReducer;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

/**
 * Created by edwardlol on 2016/12/5.
 */
public class LineCountDriver extends Configured implements Tool {
    //~  Methods ---------------------------------------------------------------

    @Override
    public int run(String[] args) throws Exception {
        if (args.length < 2) {
            System.err.printf("usage: %s simhash_result_dir output_dir " +
                            "[gname_weight] [docCnt_output_dir]\n",
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

        conf.setBoolean("mapreduce.map.output.compress", true);
        conf.set("mapreduce.map.output.compress.codec", "com.hadoop.compression.lzo.LzoCodec");

        // pre step configuration
        Job preJob = Job.getInstance(conf, "tf idf pre step");
        preJob.setJarByClass(LineCountMapper.class);

        FileInputFormat.addInputPath(preJob, new Path(args[0]));

        preJob.setMapperClass(LineCountMapper.class);

        preJob.setCombinerClass(LineCountReducer.class);

        preJob.setReducerClass(LineCountReducer.class);
        preJob.setOutputKeyClass(NullWritable.class);
        preJob.setOutputValueClass(IntWritable.class);

        FileOutputFormat.setOutputPath(preJob, new Path(args[1]));

        return preJob.waitForCompletion(true) ? 0 : 1;
    }

    //~  Entrance --------------------------------------------------------------

    public static void main(String[] args) throws Exception {
        Configuration configuration = new Configuration();
        System.exit(ToolRunner.run(configuration, new LineCountDriver(), args));
    }
}

// End LineCountDriver.java
