import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableMapper;
import org.apache.hadoop.hbase.mapreduce.TableSplit;
import org.apache.hadoop.hbase.util.Bytes;

import org.apache.hadoop.hbase.util.MD5Hash;
import org.apache.hadoop.io.Text;

import java.net.URL;
import java.util.regex.Pattern;

public class UrlRegexpMapper extends TableMapper<ImmutableBytesWritable, Text>{

    final Pattern webpeges_pattern = Pattern.compile("^webpages");
    final Pattern websites_pattern = Pattern.compile("^websites");

    byte [] cf_pages = Bytes.toBytes("docs");
    byte [] cf_sites = Bytes.toBytes("info");
    byte [] column_url = Bytes.toBytes("url");
    byte [] column_site = Bytes.toBytes("site");
    byte [] column_robots = Bytes.toBytes("robots");

    @Override
    public void map(ImmutableBytesWritable key, Result columns, Context context)  {
        String table_name = ((TableSplit)context.getInputSplit()).getTableName().toString();

        try {
            // -- table is "webpages"
            if (webpeges_pattern.matcher(table_name).find()) {
                String url = columns.getValue(cf_pages, column_url).toString();
                byte [] mark_byte = columns.getValue(cf_pages, Bytes.toBytes("disabled"));
                String mark;

                if (mark_byte == Bytes.toBytes("Y")) {
                    mark = "Y";
                } else {
                    mark = "N";
                }

                String site = (new URL(url)).getHost();

                String key_new = MD5Hash.getMD5AsHex(Bytes.toBytes(site));
                context.write(new ImmutableBytesWritable(Bytes.toBytes(key_new)), new Text("url#" + mark + url.split(site)[1]));
            }
            // -- table is "webdites"
            else {
                String site = columns.getValue(cf_sites, column_site).toString();
                String rules = columns.getValue(cf_pages, column_robots).toString();

                String key_new = MD5Hash.getMD5AsHex(Bytes.toBytes(site));
                context.write(new ImmutableBytesWritable(Bytes.toBytes(key_new)), new Text("rules#" + rules));
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

}
