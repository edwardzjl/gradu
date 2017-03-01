package document_clustering.similarity;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.KeyValueTextInputFormat;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileAsTextInputFormat;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

/**
 * Created by edwardlol on 2016/12/2.
 */
public class ISimDriver extends Configured implements Tool {
    //~  Methods ---------------------------------------------------------------

    @Override
    public int run(String[] args) throws Exception {
        if (args.length < 2) {
            System.err.printf("usage: %s simpre_dir output_dir [compression]\n",
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
                    org.apache.hadoop.fs.LocalFileSystem.class.getName());

            conf.set("yarn.app.mapreduce.am.resource.mb", "1024");
            conf.set("yarn.app.mapreduce.am.command-opts", "-Xmx768m");

            conf.set("mapred.child.java.opts", "-Xmx768m");
            conf.set("mapreduce.reduce.memory.mb", "2048");

            conf.set("mapreduce.reduce.shuffle.input.buffer.percent", "0.2");

            conf.set("mapreduce.task.io.sort.mb", "300");
            conf.set("mapreduce.task.io.sort.factor", "30");
        }

        Job job = Job.getInstance(conf, "isim job");
        job.setJarByClass(ISimDriver.class);

        if (args.length > 2 && args[2].equals("1")) {
            conf.setBoolean("mapreduce.map.output.compress", true);
            conf.set("mapreduce.map.output.compress.codec", "com.hadoop.compression.lzo.LzoCodec");
            SequenceFileInputFormat.addInputPath(job, new Path(args[0]));
            job.setInputFormatClass(SequenceFileAsTextInputFormat.class);
        } else {
            FileInputFormat.addInputPath(job, new Path(args[0]));
            job.setInputFormatClass(KeyValueTextInputFormat.class);
        }

        job.setMapperClass(ISimMapper.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(DoubleWritable.class);

        job.setCombinerClass(ISimCombiner.class);

        job.setNumReduceTasks(Integer.valueOf(args[3]));

        job.setReducerClass(ISimReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(DoubleWritable.class);

        FileOutputFormat.setOutputPath(job, new Path(args[1]));

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
        System.exit(ToolRunner.run(configuration, new ISimDriver(), args));
    }
}

// End ISimDriver.java
