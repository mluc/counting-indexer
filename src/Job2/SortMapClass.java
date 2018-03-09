package Job2;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;
import java.util.StringTokenizer;

/*
Intermediate result (Job 2's input) has the following format:
a-chap01	21
...
abode-chap14	2
...
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
            left = itr.nextToken(); //Ex: left is 'abode-chap14'
            if (itr.hasMoreTokens()) {
                right = Integer.parseInt(itr.nextToken()); //Ex: right is 2
            }
            String[] wordToFileName = left.split("-"); //Ex: wordToFileName = ['abode', 'chap14']
            key.set(wordToFileName[0], right+"-"+wordToFileName[1]);//key: word is 'abode', countToFileName is '2-chap14'
            value.set(left, right);//value: wordToFileName is 'abode-chap14', count is 2
            context.write(key, value);
        }
    }
}
