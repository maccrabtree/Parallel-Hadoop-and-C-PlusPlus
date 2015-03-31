//***************************************
// File: map1.cc
// Author: Mackenzie Crabtree
// First map phase of movie recommender 
// Takes in ratings.dat from standard in and user_reviews 
// file as first command line arg, and outputs only movies seen
// by the user
//
//
//*****************************************

#include <iostream>
#include <vector>
#include <fstream>
#include <sstream>
#include <map>
#include <cstdlib>
using namespace std;

ifstream fin;		//file input stream
map<int,double> user_ratings; //map of user_ratings


//*********************************************
//void setup()
//
// Given that fin has opened the user reviews file
// in the form "movie::rating", this function parses
// out the movie and rating, and puts this within
// the user_ratings hashmap
//
//
//*******************************************

void setup() {
     string line;
     while(getline(fin, line)) {
	int movie; double rating;
	istringstream sin(line);
	sin >> movie;		//get movie
	char junk;
	sin >> junk >> junk;    //take the colons out
	sin >> rating;          //get rating
	user_ratings.insert(pair<int, double>(movie, rating));
     }

}

//**********************************************
// void map1()
//
// Given setup() has already been called and hashmap has been built
// this function reads the ratings.dat file from standard in in the 
// form "user::movie::rating::time", parses out the user,movie, and rating
// and outputs user\tmovie\trating for movies seen by the user. Uses
// hashmap to check if a movie has been rated. 
//
//**************************************************

void map1() {
     string line;
     while(getline(cin, line)) {
	int userid, movie;
	double rating;
	char junk;
	istringstream sin(line);
	sin >> userid;      //get user
	sin >> junk >> junk; //get colons
	sin >> movie;		//get movie
	sin >> junk >> junk;    //get colons
	sin >> rating;		//get rating
	if(user_ratings.count(movie) != 0) cout << userid << "\t" << movie << "\t" << rating << endl;
	//if user has seen the movie, output user,movie,rating
    }

}





int main(int argc, char* argv[]) {
    if(argc < 2) {
	cerr << "Not enough input given!\n"; exit(1);
    }
    fin.open(argv[1]);      //open user reviews
    if(fin.fail()) {
	cerr <"Error opening input file!\n"; exit(1);
    }
    setup();      //build map
    map1();	  //map process
 
    return 0;
    
}
