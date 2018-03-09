package Job2;

import org.apache.hadoop.mapreduce.Partitioner;

/**
 *
 * Partition based on the word part of the pair.
 */
public class FirstPartitioner extends Partitioner<StringStringPair,StringIntPair> {
    @Override
    public int getPartition(StringStringPair key, StringIntPair value, int numPartitions) {
        return key.getWord().hashCode() * 127 % numPartitions;
    }
}
