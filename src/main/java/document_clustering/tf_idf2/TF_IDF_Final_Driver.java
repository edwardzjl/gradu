package document_clustering.tf_idf2;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileAsTextInputFormat;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import java.net.URI;

/**
 * Created by edwardlol on 2016/12/5.
 */
public class TF_IDF_Final_Driver extends Configured implements Tool {
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

        conf.set("yarn.app.mapreduce.am.resource.mb", "1024");
        conf.set("yarn.app.mapreduce.am.command-opts", "-Xmx768m");

        conf.set("yarn.scheduler.minimum-allocation-mb", "1024");
        conf.set("yarn.scheduler.maximum-allocation-mb", "6144");

        conf.set("mapred.child.java.opts", "-Xmx1536m");
//        conf.set("mapreduce.map.cpu.vcores", "1");
        conf.set("mapreduce.map.memory.mb", "2048");
        conf.set("mapreduce.reduce.memory.mb", "4096");

//        conf.set("mapreduce.task.io.sort.mb", "300");
//        conf.set("mapreduce.task.io.sort.factor", "30");

        conf.setBoolean("mapreduce.map.output.compress", true);
        conf.set("mapreduce.map.output.compress.codec", "com.hadoop.compression.lzo.LzoCodec");

        // step 3 configuration
        Job job3 = Job.getInstance(conf, "tf idf step3 job");
        job3.setJarByClass(TF_IDF_Reducer2.class);

        job3.addCacheFile(new URI(args[1] + "/part-r-00000#docCnt"));

        SequenceFileInputFormat.addInputPath(job3, new Path(args[0]));
        job3.setInputFormatClass(SequenceFileAsTextInputFormat.class);

        job3.setMapperClass(Mapper.class);
        job3.setMapOutputKeyClass(Text.class);
        job3.setMapOutputValueClass(Text.class);

        job3.setReducerClass(TF_IDF_Reducer2.class);
        job3.setNumReduceTasks(5);
        job3.setOutputKeyClass(Text.class);
        job3.setOutputValueClass(DoubleWritable.class);

        FileOutputFormat.setOutputPath(job3, new Path(args[2]));

        return job3.waitForCompletion(true) ? 0 : 1;
    }

    //~  Entrance --------------------------------------------------------------

    public static void main(String[] args) throws Exception {
        Configuration configuration = new Configuration();
        System.exit(ToolRunner.run(configuration, new TF_IDF_Final_Driver(), args));
    }
}

// End TF_IDF_Driver.java
