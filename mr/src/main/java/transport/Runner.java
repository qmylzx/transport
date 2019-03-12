package transport;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.util.Properties;

public class Runner {
    private String deviceType;
    private String batchId;

    public Runner(String deviceType,String batchId) {
        this.deviceType = deviceType;
        this.batchId = batchId;
    }

    public Boolean call() throws Exception {
        Configuration config = new Configuration();

        config.set("batchId", batchId);

        Job job = Job.getInstance(config);
        job.setJarByClass(Runner.class);
        job.setMapperClass(FlieHandlerMapper.class);
        job.setReducerClass(FileHandlerReducer.class);

        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(NullWritable.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(NullWritable.class);
        //要处理的数据输入与输出地址
        FileInputFormat.setInputPaths(job,  new Path("hdfs://115.156.128.241:10190/tabletemp/"+deviceType+"/"+batchId));   //自定义路径
        FileOutputFormat.setOutputPath(job, new Path("hdfs://115.156.128.241:10190/result/"+deviceType+"/"+batchId));   //自定义路径
        return job.waitForCompletion(true);
    }
}
