package document_clustering.init;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

/**
 * Created by edwardlol on 2016/12/3.
 */
public class Init8703Driver extends Configured implements Tool {
    //~  Methods ---------------------------------------------------------------

    @Override
    public int run(String[] args) throws Exception {
        if (args.length < 2) {
            System.err.printf("usage: %s input_dir output_dir [dict_path]\n", getClass().getSimpleName());
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

        if (args.length > 2) {
            conf.set("dict.path", args[2]);
        } else {
            conf.set("dict.path", "./dicts");
        }

        Job job = Job.getInstance(conf, "8703 init job");
        job.setJarByClass(Init8703Driver.class);

        FileInputFormat.addInputPath(job, new Path(args[0]));

        job.setMapperClass(Init8703Mapper2.class);

        job.setNumReduceTasks(0);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        long starttime = System.currentTimeMillis();
        boolean complete = job.waitForCompletion(true);
        long endtime = System.currentTimeMillis();
        System.out.println("8703 init job finished in: " + (endtime - starttime) / 1000 + " seconds");

        return complete ? 0 : 1;
    }

    //~  Entrance --------------------------------------------------------------

    public static void main(String[] args) throws Exception {
        Configuration configuration = new Configuration();
        System.exit(ToolRunner.run(configuration, new Init8703Driver(), args));
    }

}

// End Init8703Driver.java
