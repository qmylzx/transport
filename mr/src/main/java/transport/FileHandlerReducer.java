package transport;

import java.io.IOException;

import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Reducer;

public class FileHandlerReducer extends Reducer<Text, NullWritable, Text, NullWritable> {
    @Override
    protected void reduce(Text key, Iterable<NullWritable> values, Context context) throws IOException, InterruptedException {
        String batchId = context.getConfiguration().get("batchId");
        context.write(new Text(batchId + "," + key), NullWritable.get());
    }
}
