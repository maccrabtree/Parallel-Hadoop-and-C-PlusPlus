Hadoop Program Explanation
Author: Mackenzie Crabtree

The commands I use to run this Hadoop project:
in any directory:
mkdir movie;
javac -classpath /usr/local/hadoop/hadoop-core-1.1.2.jar:/usr/local/hadoop/lib/commons-cli-1.2.jar -d movie Movie.java
cd movie;
jar -cvf movie.jar movie;
cd ..;
hadoop movie/movie.jar movie.Movie user_reviews.dat input output;
(Where "user_reviews.dat" is the path of the user_reviews file and
Where the input folder contains ratings.dat big movie rating database file)
PLEASE NOTE:
This program uses a copyMerge function inbetween jobs to put all output of the first map/reduce
into 1 file readable by the second map setup function. So, for multiple runs, you have to delete
"interm.dat" from the hadoop file system.
I've included a run-time script of the program using the example test case and the commands I issue before
hand to ensure it works. 

