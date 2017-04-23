import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

public class GroupSite  extends WritableComparator {
    protected GroupSite() {
        super(Text.class, true);
    }

    @Override
    public int compare(WritableComparable a, WritableComparable b) {
        String a_site = ((Text)a).toString().substring(1);
        String b_site = ((Text)b).toString().substring(1);
        return a_site.compareTo(b_site);
    }
}
