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
public class MSTDriver2 extends Configured implements Tool {
    //~  Methods ---------------------------------------------------------------

    @Override
    public int run(String[] args) throws Exception {
        if (args.length < 2) {
            System.err.printf("usage: %s similarity_result_dir document_count_file output_dir " +
                            "[cluster_threshold]\n"
                    , getClass().getSimpleName());
            System.exit(1);
        }

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
            conf.setDouble("final.threshold", Double.valueOf(args[3]));
        } else {
            conf.setDouble("final.threshold", 0.2d);
        }

        Job finalJob = Job.getInstance(conf, "mst final job");
        finalJob.setJarByClass(MSTFinalReducer.class);

        finalJob.addCacheFile(docCntFile);

        FileInputFormat.addInputPath(finalJob, new Path(args[0]));
        finalJob.setInputFormatClass(KeyValueTextInputFormat.class);

        finalJob.setMapperClass(MSTFinalMapper2.class);
        finalJob.setMapOutputKeyClass(DoubleWritable.class);
        finalJob.setMapOutputValueClass(Text.class);

        finalJob.setReducerClass(MSTFinalReducer.class);
        finalJob.setOutputKeyClass(IntWritable.class);
        finalJob.setOutputValueClass(IntWritable.class);

        FileOutputFormat.setOutputPath(finalJob, new Path(args[2]));

        return finalJob.waitForCompletion(true) ? 0 : 1;
    }

    //~  Entrance --------------------------------------------------------------

    public static void main(String[] args) throws Exception {
        Configuration configuration = new Configuration();
        System.exit(ToolRunner.run(configuration, new MSTDriver2(), args));
    }
}

// End MSTDriver.java
