import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableReducer;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.MD5Hash;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Counter;

import java.util.Iterator;


public class UrlRegexpReducer extends TableReducer<Text, Text, ImmutableBytesWritable>{



    @Override
    public void reduce(Text key, Iterable<Text> values, Context context)  {

        Counter counter_urls = context.getCounter(FilterURLsRobot.CountersEnum.class.getName(),
                FilterURLsRobot.CountersEnum.COUNT_URLS.toString());

        Counter counter_robots = context.getCounter(FilterURLsRobot.CountersEnum.class.getName(),
                FilterURLsRobot.CountersEnum.COUNT_ROBOTS.toString());

        Counter counter_mapper = context.getCounter(FilterURLsRobot.CountersEnum.class.getName(),
                FilterURLsRobot.CountersEnum.COUNT_TOTAL_MAPPERS.toString());

        Counter counter_reducer = context.getCounter(FilterURLsRobot.CountersEnum.class.getName(),
                FilterURLsRobot.CountersEnum.COUNT_TOTAL_REDUCERS.toString());

        Counter counter_values = context.getCounter(FilterURLsRobot.CountersEnum.class.getName(),
                FilterURLsRobot.CountersEnum.COUNT_REDUCE_VALUES.toString());

//        for (Text vall: values) {
//            counter_values.increment(1);
//        }

        counter_reducer.increment(1);

        Iterator<Text> val = values.iterator();
        String rules = new String(val.next().toString());
        String url;
        char mark;

        if (rules.compareTo("Disallow: /medicine/urolog") == 0) {
            System.out.println("___________ My debug ___________");
            System.out.print("rules:  ");
            System.out.println(rules);
            System.out.println("________________________________");
        }

        try {
            counter_values.increment(1);
            RobotsFilter robot = new RobotsFilter(rules);
            counter_mapper.increment(1);

            while (val.hasNext()) {
                url = new String(val.next().toString());
                mark = url.charAt(0);
                url = url.substring(1);

//                counter_urls.increment(1);

//                counter.increment(1);
                if (url.compareTo("http://56nv.ru/obyavleniya/obyavleniya-chastnyh-lic-ot-10082016") == 0) {
                    System.out.println("___________ My debug ___________");
                    System.out.print("url: ");
                    System.out.println(url);
                    System.out.print("rules:  ");
                    System.out.println(rules);
                    System.out.println("IsAllowed:  ");
                    System.out.println(robot.IsAllowed(url));
                    System.out.println("docs:disable:  ");
                    System.out.println(mark);
                    System.out.println("________________________________");
                }

                if (robot.IsAllowed(url)) {

                    if (mark == 'Y') {
                        MD5Hash hash_url = (new MD5Hash()).digest(url);
                        Delete delete = new Delete(hash_url.getDigest());
                        delete.addColumn(Bytes.toBytes("docs"), Bytes.toBytes("disallow"));
                        context.write(null, delete);
                    }

                } else {

                    if (mark == 'N') {
                        MD5Hash hash_url = (new MD5Hash()).digest(url);
                        Put put = new Put(hash_url.getDigest());
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
