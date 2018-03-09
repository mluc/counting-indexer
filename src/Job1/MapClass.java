package Job1;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;
import java.util.StringTokenizer;

public class MapClass extends Mapper<WordOffset, Text, Text, IntWritable> {
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

                word.set(str2 + "-" + key.fileName);
                context.write(word, one);
            }
        }
    }
}
