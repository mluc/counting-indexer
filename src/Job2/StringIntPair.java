package Job2;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Objects;

public class StringIntPair implements Writable, WritableComparable<StringIntPair> {
    String wordToFileName;
    private int count = 0;

    @Override
    public String toString() {
        return count + ">";
    }

    /**
     * Set the left and right values.
     */
    void set(String left, int right) {
        wordToFileName = left;
        count = right;
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
            return Objects.equals(r.wordToFileName, wordToFileName) && r.count == count;
        } else {
            return false;
        }
    }

    @Override
    public int compareTo(StringIntPair o) {
        int result = wordToFileName.compareTo(wordToFileName);
        if (result == 0) {
            result = Integer.compare(o.count, count);
        }
        return result;
    }


}
