package document_clustering.deprecated.similarity_trick;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.KeyValueTextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

/**
 * Created by edwardlol on 2016/12/2.
 */
public class PreDriver extends Configured implements Tool {
    //~  Methods ---------------------------------------------------------------

    @Override
    public int run(String[] args) throws Exception {
        if (args.length < 2) {
            System.err.printf("usage: %s inverted_index_result_dir output_dir [compress] [split number]\n"
                    , getClass().getSimpleName());
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
                    org.apache.hadoop.fs.LocalFileSystem.class.getName());
        }

        // The amount of memory the MR AppMaster needs.
        conf.set("yarn.app.mapreduce.am.resource.mb", "1024");
        conf.set("yarn.app.mapreduce.am.command-opts", "-Xmx768m");

        conf.set("mapred.child.java.opts", "-Xmx1536m");
        // The amount of memory to request from the scheduler for each map task.
        conf.set("mapreduce.map.memory.mb", "2048");

        conf.set("mapreduce.reduce.memory.mb", "2048");

        conf.setInt("reducer.num", 15);

        conf.setInt("split.num", 10);

        Job job = Job.getInstance(conf, "pre job");
        job.setJarByClass(PreMapper.class);

        FileInputFormat.addInputPath(job, new Path(args[0]));

        job.setInputFormatClass(KeyValueTextInputFormat.class);

        job.setMapperClass(PreMapper.class);
        job.setMapOutputKeyClass(IntWritable.class);
        job.setMapOutputValueClass(Text.class);

        job.setNumReduceTasks(conf.getInt("reducer.num", 15));
        job.setReducerClass(PreReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        job.setOutputFormatClass(SequenceFileOutputFormat.class);
        if (args.length > 2 && args[2].equals("1")) {
            SequenceFileOutputFormat.setOutputCompressionType(job, SequenceFile.CompressionType.BLOCK);
            SequenceFileOutputFormat.setOutputCompressorClass(job, com.hadoop.compression.lzo.LzoCodec.class);
        }
        SequenceFileOutputFormat.setOutputPath(job, new Path(args[1]));

        long starttime = System.currentTimeMillis();
        boolean complete = job.waitForCompletion(true);
        long endtime = System.currentTimeMillis();
        System.out.println("inverted similarity job finished in: "
                + (endtime - starttime) / 1000 + " seconds");

        return complete ? 0 : 1;
    }

    //~  Entrance --------------------------------------------------------------

    public static void main(String[] args) throws Exception {
        Configuration configuration = new Configuration();
        System.exit(ToolRunner.run(configuration, new PreDriver(), args));
    }
}

// End PreDriver.java
