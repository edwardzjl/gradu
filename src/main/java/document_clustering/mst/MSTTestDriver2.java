package document_clustering.mst;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import java.net.URI;

/**
 * Created by edwardlol on 2016/12/3.
 */
public class MSTTestDriver2 extends Configured implements Tool {
    //~  Methods ---------------------------------------------------------------

    public static void main(String[] args) throws Exception {
        Configuration configuration = new Configuration();
        System.exit(ToolRunner.run(configuration, new MSTTestDriver2(), args));
    }

    //~  Entrance --------------------------------------------------------------

    @Override
    public int run(String[] args) throws Exception {
        if (args.length < 2) {
            System.err.printf("usage: %s input_dir output_dir\n", getClass().getSimpleName());
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

        conf.setInt("split.num", 3);
        Job job = Job.getInstance(conf, "split test job");
        job.setJarByClass(MSTMapper2.class);

        job.addCacheFile(new URI(args[2] + "#docCnt"));

        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        job.setMapperClass(MSTMapper2.class);
        job.setMapOutputKeyClass(DoubleWritable.class);
        job.setMapOutputValueClass(Text.class);

        job.setPartitionerClass(MSTChildPartitioner.class);

        job.setReducerClass(MSTChildReducer.class);
        int reduceTasts = conf.getInt("split.num", 3);
        reduceTasts = reduceTasts * (reduceTasts - 1) / 2;
        job.setNumReduceTasks(reduceTasts);

        job.setOutputKeyClass(DoubleWritable.class);
        job.setOutputValueClass(Text.class);

        long starttime = System.currentTimeMillis();
        boolean complete = job.waitForCompletion(true);
        long endtime = System.currentTimeMillis();
        System.out.println("split test job finished in: " + (endtime - starttime) / 1000 + " seconds");

        return complete ? 0 : 1;
    }
}

// End MSTTestDriver2.java
