import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

public class SortRobotFirst extends WritableComparator {
    protected SortRobotFirst () {
        super(Text.class, true);
    }

    @Override
    public int compare(WritableComparable a, WritableComparable b) {
        String a_mark = ((Text)a).toString();
        String b_mark = ((Text)b).toString();
        return a_mark.compareTo(b_mark);
    }
}
