package average;

import java.io.IOException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Map;

import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import util.MRDPUtils;

public class Average extends Configured implements Tool{
	
	public static class AverageMapper extends Mapper<LongWritable, Text, IntWritable, CountAverageTuple>
	{		
		private IntWritable outHour = new IntWritable();
		private CountAverageTuple outCountAverage = new CountAverageTuple();
		private final static SimpleDateFormat frmt = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSS");
		@SuppressWarnings("deprecation")
		@Override
		protected void map(LongWritable key, Text value, Context context)
				throws IOException, InterruptedException {
			// TODO Auto-generated method stub
			Map<String, String> parsed = MRDPUtils.transformXmlToMap(value.toString());
			String strDate = parsed.get("CreationDate");
			String text = parsed.get("Text");
			if (strDate == null || text == null) {
				return;
			}
			Date creationDate;
			try {
				creationDate = frmt.parse(strDate);
			} catch (ParseException e) {
				return;
			}
			outHour.set(creationDate.getHours());
			outCountAverage.setCount(1);
			outCountAverage.setAverage(text.length());
			context.write(outHour, outCountAverage);			
		}		
	}
	
	public static class AverageReducer extends Reducer<IntWritable, CountAverageTuple, IntWritable, CountAverageTuple>{
		
		private CountAverageTuple result = new CountAverageTuple();
		
		protected void reduce(IntWritable key,	Iterable<CountAverageTuple> value, Context context)
				throws IOException, InterruptedException {
			float sum = 0;
			float count = 0;
			for(CountAverageTuple cntAvgTpl : value){
				sum += cntAvgTpl.getCount() * cntAvgTpl.getAverage();
				count += cntAvgTpl.getCount();				
			}
			result.setCount(count);
			result.setAverage(sum / count);
			context.write(key, result);
		}
		
		
	}
	

	@Override
	public int run(String[] arg0) throws Exception {
		Job job = new Job(getConf(), "Average");
		job.setJarByClass(Average.class);
		
		job.setMapperClass(AverageMapper.class);
		//job.setNumReduceTasks(0);
		job.setCombinerClass(AverageReducer.class);		
		job.setReducerClass(AverageReducer.class);		
		job.setOutputKeyClass(IntWritable.class);
		job.setOutputValueClass(CountAverageTuple.class);
		
		FileInputFormat.setInputPaths(job, new Path(arg0[0]));
		FileOutputFormat.setOutputPath(job, new Path(arg0[1]));
		boolean success = job.waitForCompletion(true);

		return success ? 0 : 1;
	}
	
	public static void main(String [] args) throws Exception{
		int res = ToolRunner.run(new Average(), args);
		System.exit(res);
	}
	
	
}
