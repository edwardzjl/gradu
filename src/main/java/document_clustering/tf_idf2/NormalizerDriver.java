package document_clustering.tf_idf2;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.KeyValueTextInputFormat;
import org.apache.hadoop.mapreduce.lib.jobcontrol.JobControl;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

/**
 * Created by edwardlol on 2016/12/5.
 */
public class NormalizerDriver extends Configured implements Tool {
    //~  Methods ---------------------------------------------------------------

    @Override
    public int run(String[] args) throws Exception {
        if (args.length < 2) {
            System.err.printf("usage: %s simhash_result_dir output_dir " +
                            "[gname_weight] [docCnt_output_dir]\n",
                    getClass().getSimpleName());
            System.exit(1);
        }

        Path step3_outputDir = new Path(args[1] + "/step3");

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

        conf.setBoolean("mapreduce.map.output.compress", true);
        conf.set("mapreduce.map.output.compress.codec", "com.hadoop.compression.lzo.LzoCodec");

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


        return job4.waitForCompletion(true) ? 0 : 1;
    }

    //~  Entrance --------------------------------------------------------------

    public static void main(String[] args) throws Exception {
        Configuration configuration = new Configuration();
        System.exit(ToolRunner.run(configuration, new NormalizerDriver(), args));
    }
}

// End TF_IDF_Driver.java
