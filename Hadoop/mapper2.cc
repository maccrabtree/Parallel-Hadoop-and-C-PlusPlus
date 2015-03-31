//*****************************************
// Author: Mackenzie Crabtree
// File: map2.cc 
// 
// Takes in command line arguments of user_scores
// from reduce1 output, and user_reviews of the
// user rated movies, as well as ratings.dat from
// standard in. First, builds two hash_maps,
// one for movies and ratings of the user,
// and one for the users and their "confidence" rating.
// This then reads in the rating file,
// sees if the user has rated the movie
// and if a confidence rating exists for the user,
// and then outputs movie \t rating*userconfidence
// for each movie not seen by the user. 
//
//*****************************************
#include <iostream>
#include <vector>
#include <fstream>
#include <sstream>
#include <map>
#include <cstdlib>
using namespace std;

ifstream fin;
map<int,double> user_ratings;   //movies rated by user
map<int,double> recom_ratings;  //confidence rating of users



//**************************************
// void setup()
// Assuming fin has opened user_reviews file
// Builds map user_ratings for movie, rating
// of the user from the file
//
//*************************************
void setup() {
     string line;
     while(getline(fin, line)) {
	int movie; double rating;
	istringstream sin(line);
	sin >> movie;	//get movie
	char junk;
	sin >> junk >> junk;	//throw away colons
	sin >> rating;	//get rating
	user_ratings.insert(pair<int, double>(movie, rating));
	//insert into hash_map
     }

}


//******************************************
// void setup2()
// assuming "fin" has opened user_scores
// Reads through file, parsing out user, and their
// confidence rating, then builds the recom_ratings
// hashmap with the user,weight
// for use by the map2 function
//
//**********************************************
void setup2() {
     string line;
     while(getline(fin, line)) {
        int user; double weight;
        istringstream sin(line);
        sin >> user;	//get user
        sin >> weight;  //get confidence rating
        recom_ratings.insert(pair<int, double>(user, weight));
	//insert into hashmap
     }

}


//**********************************************8
// void map2()
// Assuming functions setup1() and setup2() have been called 
// and maps of both user_ratings and recom_ratings have been built
// This function reads in all lines of ratings.dat from standard in
// in the form user::movie::rating::junk , parses out the user, movie
// and rating, and then checks the maps to see if the user has seen the movie
// and if a confidence rating exists for the read in user. 
// This then outputs the movie, and rating*confidence of that user for movies
// not seen by the original user. 
// Output: movie \t weightedrating
//****************************************************
void map2() {
     string line;
     while(getline(cin, line)) {
	int userid, movie;
	double rating;
	char junk;
	istringstream sin(line);
	sin >> userid;		//get user
	sin >> junk >> junk; 	//get colons
	sin >> movie;		//get movie
	sin >> junk >> junk;	//get colons
	sin >> rating;		//get rating
	if(user_ratings.count(movie) == 0) {	//if user hasn't seen movie
	    if(recom_ratings.count(userid) != 0)	//if conf.rating of user exists
		cout << movie << "\t" << rating*recom_ratings[userid] << endl;	//output
	}
	//cout << "Work\n";
    }

}





int main(int argc, char* argv[]) {
    if(argc < 3) {
	cerr << "Not enough input given!\n"; exit(1);
    }
    fin.open(argv[2]);	//open user_reviews.dat 
    if(fin.fail()) {	
	cerr << "Error opening input file!\n"; exit(1);
    }
    setup();		//build user hashmap
    fin.close();	//close
    fin.open(argv[1]);	//open user_scores.dat
    if(fin.fail()) {	
	cerr << "Error opening input file!\n"; exit(1);
    }
    setup2();		//build user/rating hashmap
    fin.close();	//close
    map2();		//call map function
 
    return 0;
    
}
