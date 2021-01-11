package matchDataAnalysis;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

import java.io.IOException;

public class DrawnMatchesCount {

    public static class MapperClass extends
            Mapper<Object, Text, Text, IntWritable> {

        private final static IntWritable one = new IntWritable(1);
        private final Text drawnMatchCount = new Text();

        public void map(Object key, Text value, Context context)
                throws IOException, InterruptedException {

            String valueString = value.toString();
            String[] numArray = valueString.split(",(?=([^\"]*\"[^\"]*\")*[^\"]*$)");
            if (numArray.length < 9) {
                return;
            }
            String homeScore = numArray[3];
            String awayScore = numArray[4];

            if(homeScore.equals(awayScore)) {
                drawnMatchCount.set("Drawn matches ");
                context.write(drawnMatchCount, one);
            }
        }
    }

    public static class SumByKeyReducer extends
            Reducer<Text, IntWritable, Text, IntWritable> {
        private final IntWritable result = new IntWritable();

        public void reduce(Text key, Iterable<IntWritable> values,
                           Context context) throws IOException, InterruptedException {
            int sum = 0;
            for (IntWritable val : values) {
                sum += val.get();
            }

            result.set(sum);
            context.write(key, result);

        }
    }

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        String[] otherArgs = new GenericOptionsParser(conf, args)
                .getRemainingArgs();
        if (otherArgs.length != 2) {
            System.err.println("Usage: Enter <in> <out> data locations");
            System.exit(2);
        }
        Job job = Job.getInstance(conf, "DrawnMatchesCount");
        job.setJarByClass(DrawnMatchesCount.class);
        job.setMapperClass(MapperClass.class);
        job.setCombinerClass(SumByKeyReducer.class);
        job.setReducerClass(SumByKeyReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);
        FileInputFormat.addInputPath(job, new Path(otherArgs[0]));
        FileOutputFormat.setOutputPath(job, new Path(otherArgs[1]));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }

}
