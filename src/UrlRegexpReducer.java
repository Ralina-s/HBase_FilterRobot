import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableReducer;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.Text;

import java.util.ArrayList;
import java.util.List;

public class UrlRegexpReducer extends TableReducer<ImmutableBytesWritable, Text, ImmutableBytesWritable>{

    @Override
    public void reduce(ImmutableBytesWritable key, Iterable<Text> values, Context context)  {
        List<String> urls = new ArrayList<String>();
        String rules = "";

        Integer len_url_pref = "url#".length();
        Integer len_rule_pref = "rules#".length();


        for (Text value: values) {
            if (value.toString().startsWith("url#")) {
                urls.add(value.toString().substring(len_url_pref));
            } else {
                rules = value.toString().substring(len_rule_pref);
            }
        }

        try {
            RobotsFilter robot = new RobotsFilter(rules);
            for (String url: urls) {
                if (robot.IsAllowed(url.substring(1))) {

                    if (url.charAt(0) == 'Y') {
                        Delete delete = new Delete(key.copyBytes());
                        delete.addColumn(Bytes.toBytes("docs"), Bytes.toBytes("disallow"));
                        context.write(null, delete);
                    }

                } else {

                    if (url.charAt(0) == 'N') {
                        Put put = new Put(key.copyBytes());
                        put.add(Bytes.toBytes("docs"), Bytes.toBytes("disallow"), Bytes.toBytes("Y"));
                        context.write(null, put);
                    }
                }
            }

        } catch (Exception e) {
            e.printStackTrace();
        }


    }
}
