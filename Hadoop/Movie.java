/*File: Movie.java
 * Package movie;
 * Author: Mackenzie Crabtree
 * RUNTIME ARGUMENTS
 * arg[0] = path to user_reviews.dat file
 * arg[1] = path to input directory (containing ratings.dat)
 * arg[2] = path to requested output directory 
 * BEFORE RUNNING, ENSURE "interm.dat" DOES NOT EXIST ON HDFS
 *
 * This is the Java-Hadoop version of the movie recommender project. It
 * contains both map/reduce passes. Map1 is in class TokenizerMapper. 
 * Reduce1 is in Reduce class
 * Map2 is in Map2 class
 * Reduce2 is in Reduce2 class
 * The first map takes in the ratings.dat file and outputs
 * movies not seen by the user provided in user_reviews.dat
 * The reduce then uses the recommender formula for each user 
 * and outputs user/similarity rating
 * The second map then outputs the movie followed by a weightedrating
 * depending on the user by building a map of user/user-similarity
 * The final reduce then adds up the ratings and divides by total number 
 * of ratings for that movie. 
 * All outputs are IntWritable/Text pairs
 */


package movie;
import java.io.IOException;
import java.io.*;
import java.util.StringTokenizer;
import java.util.HashMap;
import org.apache.hadoop.hdfs.*;
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


public class Movie {              //class that contains project
	
	
	
	//class TokenizerMapper contains map1. It's name and syntax is derived from
	//in class examples provided by Dr. Juedes
   public static class TokenizerMapper extends Mapper<Object, Text, IntWritable, Text>{
			public static HashMap<Integer, Double> usermovies = new HashMap<Integer, Double>();
			//usermovies hashmap contains pairs of movie, rating for original user
	
			
		/* void setup(Context) 
		 *  Ran before map1, takes in the user_reviews file from given path and builds hashmap
		 *  for what movies the user has seen and their rating for the first map reduce stage
		 * 
		 */
	  public void setup(Context context) throws IOException {
		Configuration config = context.getConfiguration();
		FileSystem fs = FileSystem.get(config);
		Path path = new Path(config.get("user"));  //user is command line arg reviews		
		BufferedReader in = new BufferedReader(new InputStreamReader(fs.open(path)));
		//open user_reviews file with bufferedreader in 
		String line;
		while((line = in.readLine()) != null) {
			line = line.replaceAll("::", " "); //take out :: so tokenizer will work
			StringTokenizer itr = new StringTokenizer(line); //make tokenizer
			Integer movie = new Integer(Integer.parseInt(itr.nextToken()));
			//get movie
			Double rating = new Double(Double.parseDouble(itr.nextToken()));
			//get rating
			usermovies.put(movie, rating);
			//add to hashmap
			//System.out.println(movie + " " + rating); //debugging
		}
		in.close();
	}	//end setup
	  
	  
	  /* public void map
	   * The input to this stage is the byte offset in the file (to my understanding)
	   * and the line of the file (as Text valuein). 
	   * This stage parses the lines of the ratings.dat file by grabbing
	   * user, movie, and rating. It then writes the user and movie/rating to the context in the form
	   * USER (intwritable) movie,rating (text). Only if the movie exists in the hashmap built
	   * by the setup function usermovies. 
	   */
	  public void map(Object object, Text valuein, Context context) 
			throws IOException, InterruptedException {
		String line = new String(valuein.toString()); //convert valuein to string
		line = line.replaceAll("::", " ");			  //remove :: for tokenizer
		//System.out.println(line);
		StringTokenizer itr = new StringTokenizer(line); //make tokenzier
		IntWritable keyout = new IntWritable();			 //key out is the user read in
		Text valout = new Text();
		keyout.set(Integer.parseInt(itr.nextToken()));	//set key out to read in user
		Text movie = new Text(itr.nextToken());			//movie is the next token
		Text rating = new Text(itr.nextToken());		//rating is the next token
		valout.set(movie.toString() + "," + rating.toString());	//out val = "movie,rating"
		if( usermovies.get(Integer.parseInt(movie.toString()) ) != null) 
		context.write(keyout, valout);					
		//only write-out if movie exists in hashmap
		}			//end map
	}				//end tokenizer class
	
   
   //class Reduce contains the setup and reduce1 stage of the recommender system
	public static class Reduce extends Reducer<IntWritable, Text, IntWritable, Text> {
		public static HashMap<Integer, Double> usermovies = new HashMap<Integer, Double>();
		//usermovies hashmap to be built
		
		//************************************
		//This setup function does the same as Map1's setup function
		//***************************************
		public void setup(Context context) throws IOException {
			Configuration config = context.getConfiguration();
			FileSystem fs = FileSystem.get(config);
			Path path = new Path(config.get("user"));
			BufferedReader in = new BufferedReader(new InputStreamReader(fs.open(path)));
			String line;
			while((line = in.readLine()) != null) {
				line = line.replaceAll("::", " ");    //remove semicolons for tokenizer
				StringTokenizer itr = new StringTokenizer(line);	//make tokenizer
				Integer movie = new Integer(Integer.parseInt(itr.nextToken()));	//get movie
				Double rating = new Double(Double.parseDouble(itr.nextToken()));	//get rating
				usermovies.put(movie, rating);		//build hashmap
			}
			in.close();
		}
		
		//*****************************************************
		//public void reduce
		// after setup has ran and set up hashmap for this reduce class, 
		// this function's "keyin" is the user from the map output above, and it's 
		// valin is the text "movie,rating". Thus, this function iterates through every
		// "movie,rating" text, parses out the movie and rating, and follows the formula
		// given in class by summing 25 - (diff)^2, and dividing by the total number of ratings
		// It then writes out the user (same as keyin), and the value of the equation, as Text
		//******************************************************************
		public void reduce(IntWritable keyin, Iterable<Text> valin, Context context) 
				throws IOException, InterruptedException{
			double conf = 0;
			double total = 0;
			for(Text values : valin) {
				String line = new String(values.toString());	//values is currently "movie,rating"
				line = line.replaceAll(","," ");		//make string "movie rating"
				StringTokenizer itr = new StringTokenizer(line);
				int movie = Integer.parseInt(itr.nextToken()); 		//movie is the first part of text
				double rating = Double.parseDouble(itr.nextToken());	//then the rating
				
				conf += 25 - (rating - usermovies.get(movie))*(rating - usermovies.get(movie));
				total++;
			}
			conf = conf/total;
			DoubleWritable valout = new DoubleWritable(conf);
			Text val1 = new Text(valout.toString());
			context.write(keyin, val1);	//write out user, similarity rating as intwritable, text
		}
	}	//end reduce class
	
	
	/* Class Map2 contains the setup and map function for the second pass of map/reduce 
	 * 
	 */
	public static class Map2 extends Mapper<Object, Text, IntWritable, Text>{
		public static HashMap<Integer, Double> usermovies = new HashMap<Integer, Double>();
		//hashmap of usermovies (user, rating)
		public static HashMap<Integer, Double> userconfidence = new HashMap<Integer, Double>();
		//hashmap of userconfidence ratings (user, conf)
		
		public void setup(Context context) throws IOException {
			Configuration config = context.getConfiguration();
			FileSystem fs = FileSystem.get(config);


			Path path = new Path("interm.dat");
			//interm.dat is the output of reduce1, and contains the user/confidence rating
			//this file must be cleared off of the HDFS before runtime to work correctly
			BufferedReader in = new BufferedReader(new InputStreamReader(fs.open(path)));
			String line;    //read interm path
			while((line = in.readLine()) != null) {
				StringTokenizer itr = new StringTokenizer(line);
				Integer user = new Integer(Integer.parseInt(itr.nextToken()));  //first is user
				Double conf = new Double(Double.parseDouble(itr.nextToken()));	//then similarity rating
				userconfidence.put(user, conf);		//add to hashmap
			}
			
			Path path2 = new Path(config.get("user"));
			BufferedReader in2 = new BufferedReader(new InputStreamReader(fs.open(path2)));
			while((line = in2.readLine()) != null) {
				line = line.replaceAll("::"," ");
				StringTokenizer itr = new StringTokenizer(line);
				Integer movie = new Integer(Integer.parseInt(itr.nextToken()));
				Double rating = new Double(Double.parseDouble(itr.nextToken()));
				usermovies.put(movie,rating);    //build usermovies hashmap as in all other setups
			}
		}
		
		
		/*public void map2 
		 * This map takes as input the same input file used for map1, in the form key (as byte offset), 
		 * and value being the line in the file. It then parses the valuein to obtain the user, movie, and rating
		 * then refers to the hashmaps built in the setup function. If the movie isn't in the usermove hashmap,
		 * and if a confidence rating for the read-in user exists, it then outputs the movie as keyout, and the 
		 * rating*userconfidence as a weightedrating for the movie, as an output value of text
		 * 
		 */
		
		
		public void map(Object object, Text valuein, Context context) throws IOException, InterruptedException{
			String line = new String(valuein.toString());	//valuein is the line in the file
			line = line.replaceAll("::", " ");				//replace the colons
			//System.out.println(line);
			StringTokenizer itr = new StringTokenizer(line); 
			IntWritable user = new IntWritable();
			user.set(Integer.parseInt(itr.nextToken()));    //user is the first value in the file
			Text movie = new Text(itr.nextToken());			//movie is the next token
			Text rating = new Text(itr.nextToken());		//rating is the next token
			if( usermovies.get(Integer.parseInt(movie.toString()) ) == null) {	//if movie doesn't exist
				if(userconfidence.get(user.get()) != null) {			//if user confidence exists
					Double urating = new Double(Double.parseDouble(rating.toString()));
					urating = urating * userconfidence.get(user.get());	//get new rating
					Text valout = new Text(urating.toString());		//make valueout the weightedrating
					IntWritable keyout = new IntWritable(Integer.parseInt(movie.toString()));
					//make keyout the same as the readin movie
					context.write(keyout, valout);	//write the movie (keyout) and the weighted rating (valout)
				}
			}
		}
	}
	
	
	//public class reduce2 needs no setup function, but contains the reduce process for the second map/reduce phase
	
	public static class Reduce2 extends Reducer<IntWritable, Text, IntWritable, Text> {
		//The keyin is the movie, and the valin is the iterable texts containing the weighted ratings
		//so all that needs to be done is sum up the weightedratings (as doubles), then divide by how many there were
		public void reduce(IntWritable keyin, Iterable<Text> valin, Context context) throws IOException, InterruptedException{
			double totalrats = 0;
			double totalweight = 0;
			for(Text values: valin) {
				String valueString = values.toString();		//change text to string
				totalweight += Double.parseDouble(valueString);	//add to the total the double value of the string
				totalrats++;								//increase divisor total
			}	
			double movierating = totalweight/totalrats;		//divide total by how many there were
			DoubleWritable valout1 = new DoubleWritable(movierating);	//make this a doublewritable
			Text valout = new Text(valout1.toString());		//convert to text for valout
			context.write(keyin, valout);				//write movie with its new weighted average
		}
		
	}
	
	//The main sets up both jobs, combines files in between 
	
	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();
		String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
		conf.set("user", otherArgs[0]);	//arg 0 is the user_reviews path argument
		Job job = new Job(conf, "Pass1"); //name job pass 1
		job.setJarByClass(Movie.class);	
		job.setMapperClass(TokenizerMapper.class); //map1 is in TokenizerMapper
		//job.setCombinerClass(Reduce.class);		//no combiner
		job.setReducerClass(Reduce.class);			//reduce1 is in Reduce
		job.setOutputKeyClass(IntWritable.class);	//output is IntWritable (user)
		job.setOutputValueClass(Text.class);		//output val is conf (as text)
		FileInputFormat.addInputPath(job, new Path(otherArgs[1])); //arg0 is input folder
		FileOutputFormat.setOutputPath(job, new Path(otherArgs[2])); //arg1 is output folde
		if(!job.waitForCompletion(true) ) System.out.println("Error in first Map/Reduce");
		FileSystem fs = FileSystem.get(conf);
		//This line takes all the output "parts" from the first map/reduce stage, and then combines them
		//to make 1 file, that way it can be read as normal for the second map's setup function
		FileUtil.copyMerge(fs, new Path(otherArgs[2]), fs, new Path("interm.dat"), true, conf, "");
		//otherArgs[2] is the output folder, so take all files and merge into interm.dat		
		
		//SETUP SECOND JOB
		Configuration conf2 = new Configuration();
		conf2.set("user", otherArgs[0]);
		Job job2 = new Job(conf2, "Pass2");
		job2.setJarByClass(Movie.class);
		job2.setMapperClass(Map2.class);	//map2 is in Map2
		job2.setReducerClass(Reduce2.class);	//reduce2 is in Reduce2
		job2.setOutputKeyClass(IntWritable.class);
		job2.setOutputValueClass(Text.class);
		FileInputFormat.addInputPath(job2, new Path(otherArgs[1]));    //use same input
		FileOutputFormat.setOutputPath(job2, new Path(otherArgs[2]));	//and output path
		if(!job2.waitForCompletion(true) ) System.out.println("Error in Second Map/Reduce");
		
	}
	
}
	
