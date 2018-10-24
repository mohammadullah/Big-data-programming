import java.io.IOException;
import java.util.StringTokenizer;
import java.util.regex.Pattern;
import java.util.regex.Matcher;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.hadoop.mapreduce.lib.reduce.LongSumReducer;

public class WikipediaPopular extends Configured implements Tool {

	public static class TokenizerMapper
	extends Mapper<LongWritable, Text, Text, LongWritable>{

		private Text word = new Text();

		@Override
		public void map(LongWritable key, Text value, Context context
				) throws IOException, InterruptedException {
                        Pattern word_sep = Pattern.compile("\\s+");
                        String term1 = "Main_Page";
                        String pattern1 = "\\b"+term1+"\\b";
                        Pattern p = Pattern.compile(pattern1);
                        String[] itr = word_sep.split(value.toString());
                        Matcher m = p.matcher(itr[2]);                      
                        
                        if (itr[1].equals("en")) {
                           if (!m.find() && !itr[2].startsWith("Special:")) {
                               word.set(itr[0]);
                               LongWritable one = new LongWritable(Long.parseLong(itr[3]));
                               context.write(word, one);
                           } 
                       
                        }
		}
	}


       /*
         Reducer   
       */ 

        public static class WikiReducer
        extends Reducer<Text, LongWritable, Text, LongWritable> {
        
    	        LongWritable result = new LongWritable();
 
                @Override
                public void reduce(Text key, Iterable<LongWritable> values,
                         Context context
                         ) throws IOException, InterruptedException {
                    long maxval = 0;
                    long newval;
                    for (LongWritable val : values) {
                              newval = val.get();
                              if (newval > maxval) {
                                 maxval = newval;
                              }
                    }
                    result.set(maxval);
                    context.write(key, result);
                }
         } 


	public static void main(String[] args) throws Exception {
		int res = ToolRunner.run(new Configuration(), new WikipediaPopular(), args);
		System.exit(res);
	}

	@Override
	public int run(String[] args) throws Exception {
		Configuration conf = this.getConf();
		Job job = Job.getInstance(conf, "Wiki_Pedia_Popular");
		job.setJarByClass(WikipediaPopular.class);

		job.setInputFormatClass(TextInputFormat.class);

		job.setMapperClass(TokenizerMapper.class);
		job.setReducerClass(WikiReducer.class);

		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(LongWritable.class);
		job.setOutputFormatClass(TextOutputFormat.class);
		TextInputFormat.addInputPath(job, new Path(args[0]));
		TextOutputFormat.setOutputPath(job, new Path(args[1]));

		return job.waitForCompletion(true) ? 0 : 1;
	}
}
