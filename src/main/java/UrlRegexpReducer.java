import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableReducer;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.MD5Hash;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Counter;

import java.net.URL;
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


        counter_reducer.increment(1);

//        Iterator<Text> val = values.iterator();
//        String rules = new String(val.next().toString());
        String rules = "";
        String url;
        char mark;

        RobotsFilter robot = new RobotsFilter();

        try {
//            counter_values.increment(1);
//            RobotsFilter robot = new RobotsFilter(rules);
//            counter_mapper.increment(1);
//
//            while (val.hasNext()) {
            for (Text val: values) {

                if (key.toString().charAt(0) == '0') {
                    counter_values.increment(1);
                    rules = val.toString();
                    robot = new RobotsFilter(rules);

//                    if (rules.length() != 0) {
//                        counter_mapper.increment(1);
//
//                        if (counter_mapper.getValue() < 20) {
//                            System.out.println("_________Robot__________");
//                            System.out.println(rules);
//                        }
//
//                        if (rules.compareTo("Disallow: */4.html$") == 0) {
//                            System.out.println("___________ My debug ___________");
//                            System.out.print("rules:  ");
//                            System.out.println(rules);
//                            System.out.println("________________________________");
//                        }
//                    }

                    continue;
                }

                url = val.toString();
                mark = url.charAt(0);
                url = url.substring(1);
                URL url_url = (new URL(url));
                String host = url_url.getProtocol() + "://" + url_url.getHost();
                String path = url.substring(host.length());

//                counter_urls.increment(1);

//                counter.increment(1);
                if (url.compareTo("http://ahatan.uol.ua/active/?filter=videosaction") == 0) {
                    System.out.println("___________ My debug ___________");
                    System.out.print("url: ");
                    System.out.println(url);
                    System.out.print("rules:  ");
                    System.out.println(rules);
                    System.out.println("IsAllowed:  ");
                    System.out.println(robot.IsAllowed(path));
                    System.out.println("docs:disable:  ");
                    System.out.println(mark);
                    System.out.println("________________________________");
                }

                if (robot.IsAllowed(path)) {

                    if (mark == 'Y') {
                        Delete delete = new Delete(Bytes.toBytes(MD5Hash.digest(url).toString()));
                        delete.addColumn(Bytes.toBytes("docs"), Bytes.toBytes("disabled"));
                        context.write(null, delete);
                    }

                } else {

                    if (mark == 'N') {
                        Put put = new Put(Bytes.toBytes(MD5Hash.digest(url).toString()));
                        put.addColumn(Bytes.toBytes("docs"), Bytes.toBytes("disabled"), Bytes.toBytes("Y"));
                        context.write(null, put);
                    }
                }
            }
        } catch (Exception e) {
            e.printStackTrace();
        }


    }
}
