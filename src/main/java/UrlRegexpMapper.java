import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableMapper;
import org.apache.hadoop.hbase.mapreduce.TableSplit;
import org.apache.hadoop.hbase.util.Bytes;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Counter;

import java.net.URL;
import java.util.regex.Pattern;

public class UrlRegexpMapper extends TableMapper<Text, Text>{

    final Pattern webpeges_pattern = Pattern.compile("^webpages");

    byte [] cf_pages = Bytes.toBytes("docs");
    byte [] cf_sites = Bytes.toBytes("info");
    byte [] column_url = Bytes.toBytes("url");
    byte [] column_site = Bytes.toBytes("site");
    byte [] column_robots = Bytes.toBytes("robots");

    @Override
    public void map(ImmutableBytesWritable key, Result columns, Context context)  {
        String table_name = new String(((TableSplit)context.getInputSplit()).getTableName());

        Counter counter_urls = context.getCounter(FilterURLsRobot.CountersEnum.class.getName(),
                FilterURLsRobot.CountersEnum.COUNT_URLS.toString());

        Counter counter_robots = context.getCounter(FilterURLsRobot.CountersEnum.class.getName(),
                FilterURLsRobot.CountersEnum.COUNT_ROBOTS.toString());

        Counter counter_mapper = context.getCounter(FilterURLsRobot.CountersEnum.class.getName(),
                FilterURLsRobot.CountersEnum.COUNT_TOTAL_MAPPERS.toString());

        Counter counter_values = context.getCounter(FilterURLsRobot.CountersEnum.class.getName(),
                FilterURLsRobot.CountersEnum.COUNT_REDUCE_VALUES.toString());

        Counter counter_error = context.getCounter(FilterURLsRobot.CountersEnum.class.getName(),
                FilterURLsRobot.CountersEnum.COUNT_ERROR.toString());

//        counter_mapper.increment(1);


        try {
            // -- table is "webpages"
//            if (webpeges_pattern.matcher(table_name).find()) {
            if (table_name.compareTo("webpages_ralina") == 0) {

                String url = new String(columns.getValue(cf_pages, column_url));
                byte [] mark_byte = columns.getValue(cf_pages, Bytes.toBytes("disabled"));
                String mark;

                if (mark_byte == Bytes.toBytes("Y")) {
                    mark = "Y";
                } else {
                    mark = "N";
                }

                String site = (new URL(url)).getHost();

                if (counter_urls.getValue() < 5) {
                    System.out.println("_________Url__________");
                    System.out.println(site);
                }

                context.write(new Text("1" + site), new Text(mark + url));

                counter_urls.increment(1);
            }
            // -- table is "webdites"
            else if (table_name.compareTo("websites_ralina") == 0) {

                String site = new String(columns.getValue(cf_sites, column_site));
                String rules;

                if (counter_robots.getValue() < 5) {
                    System.out.println("_________Robot__________");
                    System.out.println(site);
                }

                byte[] rules_byte = columns.getValue(cf_sites, column_robots);
                if (rules_byte != null) {
                    rules = new String(rules_byte);
                } else {
                    rules = new String("");
                }

                context.write(new Text("0" + site), new Text(rules));

                counter_robots.increment(1);

            } else {
                counter_error.increment(1);
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

}
