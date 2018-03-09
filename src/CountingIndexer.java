
import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.input.CombineFileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.CombineFileRecordReader;
import org.apache.hadoop.mapreduce.lib.input.CombineFileSplit;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.LineReader;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;


public class CountingIndexer extends Configured implements Tool {

    /**
     * This method is from Apache -> MultiFileWordCount.java
     * This record keeps &lt;filename,offset&gt; pairs.
     */
    public static class WordOffset implements WritableComparable {

        private long offset;
        private String fileName;

        public void readFields(DataInput in) throws IOException {
            this.offset = in.readLong();
            this.fileName = Text.readString(in);
        }

        public void write(DataOutput out) throws IOException {
            out.writeLong(offset);
            Text.writeString(out, fileName);
        }

        public int compareTo(Object o) {
            WordOffset that = (WordOffset)o;

            int f = this.fileName.compareTo(that.fileName);
            if(f == 0) {
                return (int)Math.signum((double)(this.offset - that.offset));
            }
            return f;
        }
        @Override
        public boolean equals(Object obj) {
            if(obj instanceof WordOffset)
                return this.compareTo(obj) == 0;
            return false;
        }
        @Override
        public int hashCode() {
            assert false : "hashCode not designed";
            return 42; //an arbitrary constant
        }
    }


    /**
     * This method is from Apache -> MultiFileWordCount.java
     * To use {@link CombineFileInputFormat}, one should extend it, to return a
     * (custom) {@link RecordReader}. CombineFileInputFormat uses
     * {@link CombineFileSplit}s.
     */
    public static class MyInputFormat
            extends CombineFileInputFormat<WordOffset, Text>  {

        public RecordReader<WordOffset,Text> createRecordReader(InputSplit split,
                                                                TaskAttemptContext context) throws IOException {
            return new CombineFileRecordReader<WordOffset, Text>(
                    (CombineFileSplit)split, context, CombineFileLineRecordReader.class);
        }
    }

    /**
     * This method is from Apache -> MultiFileWordCount.java
     * RecordReader is responsible from extracting records from a chunk
     * of the CombineFileSplit.
     */
    public static class CombineFileLineRecordReader
            extends RecordReader<WordOffset, Text> {

        private long startOffset; //offset of the chunk;
        private long end; //end of the chunk;
        private long pos; // current pos
        private FileSystem fs;
        private Path path;
        private WordOffset key;
        private Text value;

        private FSDataInputStream fileIn;
        private LineReader reader;

        public CombineFileLineRecordReader(CombineFileSplit split,
                                           TaskAttemptContext context, Integer index) throws IOException {

            this.path = split.getPath(index);
            fs = this.path.getFileSystem(context.getConfiguration());
            this.startOffset = split.getOffset(index);
            this.end = startOffset + split.getLength(index);
            boolean skipFirstLine = false;

            //open the file
            fileIn = fs.open(path);
            if (startOffset != 0) {
                skipFirstLine = true;
                --startOffset;
                fileIn.seek(startOffset);
            }
            reader = new LineReader(fileIn);
            if (skipFirstLine) {  // skip first line and re-establish "startOffset".
                startOffset += reader.readLine(new Text(), 0,
                        (int)Math.min((long)Integer.MAX_VALUE, end - startOffset));
            }
            this.pos = startOffset;
        }

        public void initialize(InputSplit split, TaskAttemptContext context)
                throws IOException, InterruptedException {
        }

        public void close() throws IOException { }

        public float getProgress() throws IOException {
            if (startOffset == end) {
                return 0.0f;
            } else {
                return Math.min(1.0f, (pos - startOffset) / (float)(end - startOffset));
            }
        }

        public boolean nextKeyValue() throws IOException {
            if (key == null) {
                key = new WordOffset();
                key.fileName = path.getName();
            }
            key.offset = pos;
            if (value == null) {
                value = new Text();
            }
            int newSize = 0;
            if (pos < end) {
                newSize = reader.readLine(value);
                pos += newSize;
            }
            if (newSize == 0) {
                key = null;
                value = null;
                return false;
            } else {
                return true;
            }
        }

        public WordOffset getCurrentKey()
                throws IOException, InterruptedException {
            return key;
        }

        public Text getCurrentValue() throws IOException, InterruptedException {
            return value;
        }
    }

    /**
     * This Mapper is similar to the one in {@link CountingIndexer.MapClass}.
     */
    public static class MapClass extends Mapper<WordOffset, Text, Text, IntWritable> {
        private final static IntWritable one = new IntWritable(1);
        private Text word = new Text();

        public void map(WordOffset key, Text value, Context context) throws IOException, InterruptedException {

            String line = value.toString();

            StringTokenizer itr = new StringTokenizer(line);
            while (itr.hasMoreTokens()) {
                String str = itr.nextToken();

                String newStr = str.toLowerCase().replaceAll("[^a-z]", " ");
                StringTokenizer itr2 = new StringTokenizer(newStr);
                while (itr2.hasMoreTokens()) {
                    String str2 = itr2.nextToken();

                    word.set(str2 + "-" + key.fileName );
                    context.write(word, one);
                }
            }
        }
    }

    private void printUsage() {
        System.out.println("Usage : CountingIndexer <input_dir> <output>" );
    }

    public static class IntSumReducer extends Reducer<Text,IntWritable,Text,IntWritable> {
        private IntWritable result = new IntWritable();

        public void reduce(Text key, Iterable<IntWritable> values, Context context
        ) throws IOException, InterruptedException {
            int sum = 0;
            for (IntWritable val : values) {
                sum += val.get();
            }

            result.set(sum);
            context.write(key, result);
        }
    }






    public static class StringIntPair implements Writable, WritableComparable<StringIntPair> {
        private String wordToFileName;
        private int count = 0;

        @Override
        public String toString() {
            return count + ">";
        }
        /**
         * Set the left and right values.
         */
        public void set(String left, int right) {
            wordToFileName = left;
            count = right;
        }
        public String getWordToFileName() {
            return wordToFileName;
        }
        public int getCount() {
            return count;
        }
        /**
         * Read the two integers.
         * Encoded as: MIN_VALUE -> 0, 0 -> -MIN_VALUE, MAX_VALUE-> -1
         */
        @Override
        public void readFields(DataInput in) throws IOException {
            wordToFileName = Text.readString(in);
            count = in.readInt() + Integer.MIN_VALUE;
        }
        @Override
        public void write(DataOutput out) throws IOException {
            Text.writeString(out, wordToFileName);
            out.writeInt(count - Integer.MIN_VALUE);
        }
        @Override
        public int hashCode() {
            return wordToFileName.hashCode() * 157 + count;
        }
        @Override
        public boolean equals(Object right) {
            if (right instanceof StringIntPair) {
                StringIntPair r = (StringIntPair) right;
                return r.wordToFileName == wordToFileName && r.count == count;
            } else {
                return false;
            }
        }

        @Override
        public int compareTo(StringIntPair o) {
            int result = wordToFileName.compareTo(wordToFileName);
            if(result == 0){
                result = Integer.compare(o.count, count);
            }
            return result;
        }


    }
    /**

     * Define a pair of integers that are writable.
     * They are serialized in a byte comparable format.
     */
    public static class StringStringPair implements Writable, WritableComparable<StringStringPair> {
        private String word;
        private String countToFileName;

        @Override
        public String toString() {
            return "(" + word + ", " + countToFileName + ")";
        }

        /**
         * Set the left and right values.
         */
        public void set(String left, String right) {
            word = left;
            countToFileName = right;
        }
        public String getWord() {
            return word;
        }
        public String getCountToFileName() {
            return countToFileName;
        }
        /**
         * Read the two integers.
         * Encoded as: MIN_VALUE -> 0, 0 -> -MIN_VALUE, MAX_VALUE-> -1
         */
        @Override
        public void readFields(DataInput in) throws IOException {
            word = Text.readString(in);
            countToFileName = Text.readString(in);
        }
        @Override
        public void write(DataOutput out) throws IOException {
            Text.writeString(out, word);
            Text.writeString(out, countToFileName);
        }
        @Override
        public int hashCode() {
            return word.hashCode() * 157 + countToFileName.hashCode();
        }
        @Override
        public boolean equals(Object right) {
            if (right instanceof StringStringPair) {
                StringStringPair r = (StringStringPair) right;
                return r.word == word && r.countToFileName == countToFileName;
            } else {
                return false;
            }
        }

        @Override
        public int compareTo(StringStringPair o) {

            //1, compare by word increasingly
            int result = word.compareTo(o.word);
            if(result == 0){
                String[] count_FileName = countToFileName.split("-");
                String[] count_FileName_o = o.countToFileName.split("-");
                int count_o = Integer.parseInt(count_FileName_o[0]);
                int count = Integer.parseInt(count_FileName[0]);
                //word is the same, compare by count decreasingly
                result = Integer.compare(count_o, count);
                if(result == 0){
                    String fileName_o = count_FileName_o[1];
                    String fileName = count_FileName[1];
                    //both word and count are the same, compare by file name increasingly
                    result = fileName.compareTo(fileName_o);
                }
            }
            return result;
        }
    }

    /**
     *
     * Partition based on the word part of the pair.
     */
    public static class FirstPartitioner extends Partitioner<StringStringPair,StringIntPair>{
        @Override
        public int getPartition(StringStringPair key, StringIntPair value, int numPartitions) {
            return key.getWord().hashCode() * 127 % numPartitions;
        }
    }

    /**
     *
     * Compare only the word part of the pair, so that reduce is called once
     * for each value of the word part.
     */
    public static class FirstGroupingComparator extends WritableComparator {
        protected FirstGroupingComparator() {
            super(StringStringPair.class, true);
        }

        @Override
        public int compare(WritableComparable  o1, WritableComparable  o2) {
            StringStringPair key1 = (StringStringPair) o1;
            StringStringPair key2 = (StringStringPair) o2;
            return key1.word.compareTo(key2.word);
        }
    }

    /**
     *
     *
     *
     *
     *
     *
     *
     *
     *
     *
     * Read two integers from each line and generate a key, value pair
     * as ((left, right), right).
     */
    public static class SortMapClass extends Mapper<LongWritable, Text, StringStringPair, StringIntPair> {

        private final StringStringPair key = new StringStringPair();
        private final StringIntPair value = new StringIntPair();

        @Override
        public void map(LongWritable inKey, Text inValue, Context context) throws IOException, InterruptedException {
            StringTokenizer itr = new StringTokenizer(inValue.toString());
            String left;
            int right = 0;
            if (itr.hasMoreTokens()) {
                left = itr.nextToken();
                if (itr.hasMoreTokens()) {
                    right = Integer.parseInt(itr.nextToken());
                }
                String[] wordToFileName = left.split("-");
                key.set(wordToFileName[0], right+"-"+wordToFileName[1]);
                value.set(left, right);
                context.write(key, value);
            }
        }
    }

    /**
     * A reducer class that just emits the sum of the input values.
     */
    public static class SortReducer extends Reducer<StringStringPair, StringIntPair, Text, StringIntPair> {

        private final Text word = new Text();


        @Override
        public void reduce(StringStringPair key, Iterable<StringIntPair> values, Context context ) throws IOException, InterruptedException {
            word.set(key.getWord());
            context.write(word, null);

            for(StringIntPair value: values) {
                String[] word_FileName = value.wordToFileName.split("-");
                String fileName = word_FileName[1];

                Text first = new Text();
                first.set("<" + fileName +",");
                context.write(first, value);
            }
        }
    }


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

        //return job.waitForCompletion(true) ? 0 : 1;
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



}
