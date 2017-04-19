import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;


public class FilterURLsRobot extends Configured implements Tool {

    @Override
    public int run(String[] args) throws Exception {
        Job job = GetJobConf(args);
        return job.waitForCompletion(true) ? 0 : 1;
    }

    Job GetJobConf(String[] args) throws IOException {
        String webpages = args[0];
        String websites = args[1];

        Job job = Job.getInstance(getConf(), FilterURLsRobot.class.getCanonicalName());
        job.setJarByClass(FilterURLsRobot.class);

        List<Scan> scans = new ArrayList<Scan>();

        scans.add(new Scan().setAttribute("scan.attributes.table.name", Bytes.toBytes(webpages)));
        scans.add(new Scan().setAttribute("scan.attributes.table.name", Bytes.toBytes(websites)));

        TableMapReduceUtil.initTableMapperJob(
                scans,
                UrlRegexpMapper.class,
                ImmutableBytesWritable.class, Text.class,
                job
        );

        TableMapReduceUtil.initTableReducerJob(
                webpages,
                UrlRegexpReducer.class,
                job
        );

        return job;
    }

    public static void main(String[] args) throws Exception {
        int rc = ToolRunner.run(HBaseConfiguration.create(), new FilterURLsRobot(), args);
        System.exit(rc);
    }
}
