package Job2;

import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

/**
 *
 * Compare only the word part of the pair, so that reduce is called once
 * for each value of the word part.
 */
public class FirstGroupingComparator extends WritableComparator {
    protected FirstGroupingComparator() {
        super(StringStringPair.class, true);
    }

    @Override
    public int compare(WritableComparable o1, WritableComparable  o2) {
        StringStringPair key1 = (StringStringPair) o1;
        StringStringPair key2 = (StringStringPair) o2;
        return key1.word.compareTo(key2.word);
    }
}
