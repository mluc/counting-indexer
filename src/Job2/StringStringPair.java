package Job2;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Objects;

/**
 * Define a pair of integers that are writable.
 * They are serialized in a byte comparable format.
 */
public class StringStringPair implements Writable, WritableComparable<StringStringPair> {
    String word;
    private String countToFileName;

    @Override
    public String toString() {
        return "(" + word + ", " + countToFileName + ")";
    }

    /**
     * Set the left and right values.
     */
    void set(String left, String right) {
        word = left;
        countToFileName = right;
    }
    String getWord() {
        return word;
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
            return Objects.equals(r.word, word) && Objects.equals(r.countToFileName, countToFileName);
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
