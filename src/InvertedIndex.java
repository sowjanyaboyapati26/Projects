import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;


public  class InvertedIndex  extends Configured implements Tool  { //  default tool runner 
	

	@Override
public int run(String[] args) throws Exception {
Configuration conf =new Configuration(); // job Configuration
Job job = new Job(conf, "InvertedIndex"); //job name


job.setJarByClass(InvertedIndex.class); // driver class 
//job.setMapperClass(mapInvertedIndex.class); //MapperClass
//job.setReducerClass(ReduceInvertedIndex.class); //ReducerClass

//job.setMapOutputKeyClass(Text.class); // mapper o/p key
//job.setMapOutputValueClass(Text.class);//mapper o/p value

//job.setOutputKeyClass(Text.class);// Reducer o/p key
//job.setOutputValueClass(Text.class); //Reducer o/p value


FileInputFormat.addInputPath(job, new Path(args[0])); // while running cmd i/p and o/p arguments paths
FileOutputFormat.setOutputPath(job, new Path(args[1]));

return job.waitForCompletion(true)? 0:1; //sumbit the job to the cluster and wait for finish, return 0 after finishing, if false, then it not wait for completion for job

}
	public static void main(String[] args) throws Exception {
	    int res = ToolRunner.run(new Configuration(), new InvertedIndex(), args);
	    System.exit(res);
	}	
}


