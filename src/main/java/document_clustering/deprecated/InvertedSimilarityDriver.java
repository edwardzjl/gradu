package document_clustering.deprecated;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.NLineInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

/**
 * Created by edwardlol on 2016/12/2.
 */
public class InvertedSimilarityDriver extends Configured implements Tool {
    //~  Methods ---------------------------------------------------------------

    @Override
    public int run(String[] args) throws Exception {
        if (args.length < 2) {
            System.err.printf("usage: %s inverted_index_result_dir output_dir" +
                    " [line_number_per_map]\n", getClass().getSimpleName());
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
//        conf.set("yarn.app.mapreduce.am.resource.mb", "1024");
//        conf.set("yarn.app.mapreduce.am.command-opts", "-Xmx768m");
//
//        // The minimum allocation for every container request at the RM, in MBs.
//        // Memory requests lower than this will throw a InvalidResourceRequestException.
//        conf.set("yarn.scheduler.minimum-allocation-mb", "1024");
//        // The maximum allocation for every container request at the RM, in MBs.
//        // Memory requests higher than this will throw a InvalidResourceRequestException.
////        conf.set("yarn.scheduler.maximum-allocation-mb", "4096");
//        // The maximum allocation for every container request at the RM, in terms of virtual CPU cores.
//        // Requests higher than this will throw a InvalidResourceRequestException.
//        conf.set("yarn.scheduler.minimum-allocation-vcores", "1");
////        conf.set("yarn.scheduler.maximum-allocation-vcores", "2");
//
//
//        conf.set("mapred.child.java.opts", "-Xmx1536m");
//        // The amount of memory to request from the scheduler for each map task.
//        conf.set("mapreduce.map.memory.mb", "2048");
//        // The number of virtual cores to request from the scheduler for each map task.
//        conf.set("mapreduce.map.cpu.vcores", "1");
//
//        conf.set("mapreduce.reduce.memory.mb", "4096");
//
//        conf.set("mapreduce.task.io.sort.mb", "200");
//        conf.set("mapreduce.task.io.sort.factor", "20");

        // set compress
//        conf.setBoolean("mapreduce.map.output.compress", true);
//        conf.set("mapreduce.map.output.compress.codec", "com.hadoop.compression.lzo.LzoCodec");

        Job job = Job.getInstance(conf, "inverted similarity job");
        job.setJarByClass(InvertedSimilarityMapper.class);

        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        if (args.length > 2) {
            NLineInputFormat.setNumLinesPerSplit(job, Integer.valueOf(args[2]));
        }

        job.setInputFormatClass(NLineInputFormat.class);

        job.setMapperClass(InvertedSimilarityMapper.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(DoubleWritable.class);

        job.setCombinerClass(InvertedSimilarityCombiner.class);

        job.setReducerClass(InvertedSimilarityReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(DoubleWritable.class);

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
        System.exit(ToolRunner.run(configuration, new InvertedSimilarityDriver(), args));
    }
}

// End InvertedSimilarityDriver.java
