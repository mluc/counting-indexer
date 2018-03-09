package Job2;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;
import java.util.StringTokenizer;

/**
 * Read two integers from each line and generate a key, value pair
 * as ((left, right), right).
 */
public class SortMapClass extends Mapper<LongWritable, Text, StringStringPair, StringIntPair> {

    private final StringStringPair key = new StringStringPair();
    private final StringIntPair value = new StringIntPair();

    @Override
    public void map(LongWritable inKey, Text inValue, Context context)
            throws IOException, InterruptedException {
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
