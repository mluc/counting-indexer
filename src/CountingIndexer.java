import Job1.IntSumReducer;
import Job1.MapClass;
import Job1.MyInputFormat;
import Job2.*;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class CountingIndexer extends Configured implements Tool {

    public static void main(String[] args) throws Exception {
        int ret = ToolRunner.run(new CountingIndexer(), args);
        System.exit(ret);
    }

    public int run(String[] args) throws Exception {

        if(args.length < 2) {
            printUsage();
            return 2;
        }

        Configuration conf = getConf();
        /************
         JOB 1
         ***********/

        Job job = new Job(conf);
        job.setJobName("CountingIndexer");
        job.setJarByClass(CountingIndexer.class);

        //set the InputFormat of the job to our InputFormat
        job.setInputFormatClass(MyInputFormat.class);

        // the keys are words (strings)
        job.setOutputKeyClass(Text.class);
        // the values are counts (ints)
        job.setOutputValueClass(IntWritable.class);

        //use the defined mapper
        job.setMapperClass(MapClass.class);
        //use the WordCount Reducer
        job.setCombinerClass(IntSumReducer.class);
        job.setReducerClass(IntSumReducer.class);

        FileInputFormat.addInputPaths(job, args[1]);
        Path outputPathTemp = new Path(args[2] +"Temp");
        FileOutputFormat.setOutputPath(job, outputPathTemp);

        job.waitForCompletion(true);

        /************
        JOB 2
         ***********/
        Job job2 = new Job(conf);
        job2.setJobName("secondarysort");
        job2.setJarByClass(CountingIndexer.class);
        job2.setMapperClass(SortMapClass.class);
        job2.setReducerClass(SortReducer.class);

        // group and partition by the word int in the pair
        job2.setPartitionerClass(FirstPartitioner.class);
        job2.setGroupingComparatorClass(FirstGroupingComparator.class);

        // the map output is StringStringPair, IntWritable
        job2.setMapOutputKeyClass(StringStringPair.class);
        job2.setMapOutputValueClass(StringIntPair.class);

        // the reduce output is Text, IntWritable
        job2.setOutputKeyClass(Text.class);
        job2.setOutputValueClass(StringIntPair.class);

        FileInputFormat.addInputPath(job2, outputPathTemp);
        FileOutputFormat.setOutputPath(job2, new Path(args[2]));

        return job2.waitForCompletion(true) ? 0 : 1;
    }

    private void printUsage() {
        System.out.println("Usage : CountingIndexer <input_dir> <output>" );
    }
}
