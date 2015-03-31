package movie;
import java.io.IOException;
import java.io.*;
import java.util.StringTokenizer;
import java.util.HashMap;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;


public class Movie {
   public static class TokenizerMapper extends Mapper<Object, Text, IntWritable, Text>{
		//private final static IntWritable one = new IntWritable(1);
			public static HashMap<Integer, Double> usermovies = new HashMap<Integer, Double>();

	public void setup(Context context) throws IOException {
		Configuration config = context.getConfiguration();
		FileSystem fs = FileSystem.get(config);
		//Path path = new Path(config.get("user"));
		Path path = new Path("/user/mcrabtre/rev/user_reviews.dat");
		BufferedReader in = new BufferedReader(new InputStreamReader(fs.open(path)));
		String line;
		while((line = in.readLine()) != null) {
			line = line.replaceAll("::", " ");
			StringTokenizer itr = new StringTokenizer(line);
			Integer movie = new Integer(Integer.parseInt(itr.nextToken()));
			Double rating = new Double(Double.parseDouble(itr.nextToken()));
			usermovies.put(movie, rating);
			System.out.println(movie + " " + rating);
		}
		in.close();
	}
	public void map(Object object, Text valuein, Context context) 
			throws IOException, InterruptedException {
		String line = new String(valuein.toString());
		line = line.replaceAll("::", " ");
		//System.out.println(line);
		StringTokenizer itr = new StringTokenizer(line);
		IntWritable keyout = new IntWritable();
		Text valout = new Text();
		keyout.set(Integer.parseInt(itr.nextToken()));
		Text movie = new Text(itr.nextToken());
		Text rating = new Text(itr.nextToken());
		valout.set(movie.toString() + "," + rating.toString());
		if( usermovies.get(Integer.parseInt(movie.toString()) ) != null) 
		context.write(keyout, valout);
		}
	}
	
	public static class Reduce extends Reducer<IntWritable, Text, IntWritable, Text> {
		public static HashMap<Integer, Double> usermovies = new HashMap<Integer, Double>();
		
		public void setup(Context context) throws IOException {
			Configuration config = context.getConfiguration();
			FileSystem fs = FileSystem.get(config);
			//Path path = new Path(config.get("user"));
			Path path = new Path("/user/mcrabtre/rev/user_reviews.dat");
			BufferedReader in = new BufferedReader(new InputStreamReader(fs.open(path)));
			String line;
			while((line = in.readLine()) != null) {
				line = line.replaceAll("::", " ");
				StringTokenizer itr = new StringTokenizer(line);
				Integer movie = new Integer(Integer.parseInt(itr.nextToken()));
				Double rating = new Double(Double.parseDouble(itr.nextToken()));
				usermovies.put(movie, rating);	
			}
			in.close();
		}
		public void reduce(IntWritable keyin, Iterable<Text> valin, Context context) 
				throws IOException, InterruptedException{
			double conf = 0;
			double total = 0;
			for(Text values : valin) {
				String line = new String(values.toString());
				line = line.replaceAll(","," ");
				StringTokenizer itr = new StringTokenizer(line);
				int movie = Integer.parseInt(itr.nextToken());
				double rating = Double.parseDouble(itr.nextToken());
				
				conf += 25 - (rating - usermovies.get(movie))*(rating - usermovies.get(movie));
				total++;
			}
			conf = conf/total;
			DoubleWritable valout = new DoubleWritable(conf);
			Text val1 = new Text(valout.toString());
			context.write(keyin, val1);
		}
	}
	
	public static class Map2 extends Mapper<Object, Text, IntWritable, Text>{
		public static HashMap<Integer, Double> usermovies = new HashMap<Integer, Double>();
		public static HashMap<Integer, Double> userconfidence = new HashMap<Integer, Double>();
		public void setup(Context context) throws IOException {
			Configuration config = context.getConfiguration();
			FileSystem fs = FileSystem.get(config);
			//Path path = new Path(config.get("output"));
			Path path = new Path("/user/mcrabtre/interm/output.dat");
			BufferedReader in = new BufferedReader(new InputStreamReader(fs.open(path)));
			String line;
			System.out.println("HEREEEEEE\n");
			while(!in.ready());
			while((line = in.readLine()) != null) {
				StringTokenizer itr = new StringTokenizer(line);
				Integer user = new Integer(Integer.parseInt(itr.nextToken()));
				Double conf = new Double(Double.parseDouble(itr.nextToken()));
				userconfidence.put(user, conf);
			}
			//in.close();
			Path path2 = new Path("/user/mcrabtre/rev/user_reviews.dat");
			BufferedReader in2 = new BufferedReader(new InputStreamReader(fs.open(path2)));
			while((line = in2.readLine()) != null) {
				line = line.replaceAll("::"," ");
				StringTokenizer itr = new StringTokenizer(line);
				Integer movie = new Integer(Integer.parseInt(itr.nextToken()));
				Double rating = new Double(Double.parseDouble(itr.nextToken()));
				usermovies.put(movie,rating);
			}
			//in2.close();
		}
		
		public void map(Object object, Text valuein, Context context) throws IOException, InterruptedException{
			String line = new String(valuein.toString());
			line = line.replaceAll("::", " ");
			//System.out.println(line);
			StringTokenizer itr = new StringTokenizer(line);
			IntWritable user = new IntWritable();
			user.set(Integer.parseInt(itr.nextToken()));
			Text movie = new Text(itr.nextToken());
			Text rating = new Text(itr.nextToken());
			if( usermovies.get(Integer.parseInt(movie.toString()) ) == null) {
				if(userconfidence.get(user.get()) != null) {
					Double urating = new Double(Double.parseDouble(rating.toString()));
					urating = urating * userconfidence.get(user.get());
					Text valout = new Text(urating.toString());
					IntWritable keyout = new IntWritable(Integer.parseInt(movie.toString()));
					context.write(keyout, valout);
				}
			}
		}
	}
	
	
	public static class Reduce2 extends Reducer<IntWritable, Text, IntWritable, Text> {
		public static HashMap<Integer, Double> usermovies = new HashMap<Integer, Double>();
		public static HashMap<Integer, Double> userconfidence = new HashMap<Integer, Double>();
		/*public void setup(Context context) throws IOException {
			Configuration config = context.getConfiguration();
			FileSystem fs = FileSystem.get(config);
			//Path path = new Path(config.get("output"));
			Path path = new Path("/user/mcrabtre/interm.dat");
			BufferedReader in = new BufferedReader(new InputStreamReader(fs.open(path)));
			String line;
			while((line = in.readLine()) != null) {
				StringTokenizer itr = new StringTokenizer(line);
				Integer user = new Integer(Integer.parseInt(itr.nextToken()));
				Double conf = new Double(Double.parseDouble(itr.nextToken()));
				userconfidence.put(user, conf);
			}
			in.close();
			Path path2 = new Path("/user/mcrabtre/rev/user_reviews.dat");
			BufferedReader in2 = new BufferedReader(new InputStreamReader(fs.open(path2)));
			while((line = in2.readLine()) != null) {
				StringTokenizer itr = new StringTokenizer(line,"::");
				Integer movie = new Integer(Integer.parseInt(itr.nextToken()));
				Double rating = new Double(Double.parseDouble(itr.nextToken()));
				usermovies.put(movie,rating);
			}
			in2.close();
		}
		*/
		public void reduce(IntWritable keyin, Iterable<Text> valin, Context context) throws IOException, InterruptedException{
			double totalrats = 0;
			double totalweight = 0;
			for(Text values: valin) {
				String valueString = values.toString();
				totalweight += Double.parseDouble(valueString);
				totalrats++;
			}
			double movierating = totalweight/totalrats;
			DoubleWritable valout1 = new DoubleWritable(movierating);
			Text valout = new Text(valout1.toString());
			context.write(keyin, valout);
		}
		
	}
	
	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();
		String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
		//conf.set("user", otherArgs[0]);
		conf.set("output", otherArgs[2]);
		Job job = new Job(conf, "Pass1");
		job.setJarByClass(Movie.class);
		job.setMapperClass(TokenizerMapper.class);
		//job.setCombinerClass(Reduce.class);
		job.setReducerClass(Reduce.class);
		job.setOutputKeyClass(IntWritable.class);
		job.setOutputValueClass(Text.class);
		FileInputFormat.addInputPath(job, new Path(otherArgs[1]));
		FileOutputFormat.setOutputPath(job, new Path(otherArgs[2]));
		if(!job.waitForCompletion(true) ) System.out.println("Error in first Map/Reduce");
		FileSystem fs = FileSystem.get(conf);
		FileUtil.copyMerge(fs, new Path("/user/mcrabtre/output1/"), fs, new Path("/user/mcrabtre/interm.dat"), true, conf, "");
		Configuration conf2 = new Configuration();
		Job job2 = new Job(conf2, "Pass2");
		job2.setJarByClass(Movie.class);
		job2.setMapperClass(Map2.class);
		job2.setReducerClass(Reduce2.class);
		job2.setOutputKeyClass(IntWritable.class);
		job2.setOutputValueClass(Text.class);
		FileInputFormat.addInputPath(job2, new Path(otherArgs[1]));
		FileOutputFormat.setOutputPath(job2, new Path(otherArgs[2]));
		if(!job2.waitForCompletion(true) ) System.out.println("Error in Second Map/Reduce");
		
	}
	
}
	
