package Job2;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/**
 * A reducer class that just emits the sum of the input values.
 */
public class SortReducer extends Reducer<StringStringPair, StringIntPair, Text, StringIntPair> {

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
