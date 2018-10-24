import java.io.IOException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.json.JSONObject;

public class RedditAverage extends Configured implements Tool {
 
    public static class RadditMapper
    extends Mapper<LongWritable, Text, Text, LongPairWritable>{
 
        private Text word = new Text();
         
        @Override
        public void map(LongWritable key, Text value, Context context
                ) throws IOException, InterruptedException {       	        	
        	String valueString = value.toString();  
                JSONObject record = new JSONObject(valueString);
        	LongPairWritable pair = new LongPairWritable();  
        	pair.set(1, (Integer) record.get("score"));
        	word.set((String) record.get("subreddit"));
        	context.write(word, pair);
        }
    }
    
        
   
    /*
       Combiner       
    */
    public static class RedditCombiner
    extends Reducer<Text, LongPairWritable, Text, LongPairWritable> {
    	
    	LongPairWritable addpair = new LongPairWritable(); 
    	
        @Override
        public void reduce(Text key, Iterable<LongPairWritable> values,
                Context context
                ) throws IOException, InterruptedException {
            long addscore = 0;  
            long addcomment = 0; 
            for (LongPairWritable val : values) {
            	addcomment += val.get_0();
            	addscore += val.get_1();
            }
            addpair.set(addscore, addcomment);
            context.write(key, addpair); 
        }
    } 
    
    
    /*
       Reducer   
    */ 
    public static class RedditReducer
    extends Reducer<Text, LongPairWritable, Text, DoubleWritable> {
        
    	DoubleWritable result = new DoubleWritable();
 
        @Override
        public void reduce(Text key, Iterable<LongPairWritable> values,
                Context context
                ) throws IOException, InterruptedException {
            long addscore = 0;
            long addcomment = 0;
            for (LongPairWritable val : values) {
            	addcomment += val.get_0();
            	addscore += val.get_1();
            }
            double mean = (double)addcomment/(double)addscore;
            
            result.set(mean);
            context.write(key, result);
        }
    } 
    
     
    public static void main(String[] args) throws Exception {
        int res = ToolRunner.run(new Configuration(), new RedditAverage(), args);
        System.exit(res);
    }
 
    
    public int run(String[] args) throws Exception {
        Configuration conf = this.getConf();
        Job job = Job.getInstance(conf, "Reddit Average");
        job.setJarByClass(RedditAverage.class);
        job.setInputFormatClass(TextInputFormat.class);
 
        job.setMapperClass(RadditMapper.class);
        job.setCombinerClass(RedditCombiner.class);   
        job.setReducerClass(RedditReducer.class);
        
 
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(LongPairWritable.class);
        job.setOutputFormatClass(TextOutputFormat.class);
        TextInputFormat.addInputPath(job, new Path(args[0]));
        TextOutputFormat.setOutputPath(job, new Path(args[1]));
 
        return job.waitForCompletion(true) ? 0 : 1;
    }
}
