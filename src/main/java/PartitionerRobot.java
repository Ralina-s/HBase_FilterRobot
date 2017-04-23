import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Partitioner;

public class PartitionerRobot  extends Partitioner<Text, Text> {
    @Override
    public int getPartition(Text key, Text val, int numPartitions) {
        return Math.abs(key.toString().substring(1).hashCode() * 127 & Integer.MAX_VALUE) % numPartitions;
    }
}
