//********************************
// Author: Mackenzie Crabtree
// File: reduce1.cc
// 
// Called after map1. Takes command line argument
// of user_reviews and standard in of map1's 
// output. Then builds map of user_ratings from
// user_reviews, then uses this map to build
// a "confidence" rating of each user
// based on the equation 
// Summation of all movies j (25 -(ratingu(j)-(ratingi(j))^2
// divided by movies shared by user u and i 
// Then outputs user \t confidence rating for each user
//************************************

#include <iostream>
#include <vector>
#include <fstream>
#include <sstream>
#include <map>
#include <cstdlib>
using namespace std;

ifstream fin;
map<int,double> user_ratings;	//map of movie, ratings




//**************************************
// void setup()
// Assumes fin has opened user_reviews.dat 
// and builds the map of user_ratings by reading in
// the movie, and rating, and inserts this into the map
//
//************************************************

void setup() {
     string line;
     while(getline(fin, line)) {
	int movie; double rating;
	istringstream sin(line);
	sin >> movie;	//get movie
	char junk;	
	sin >> junk >> junk;	//get colons
	sin >> rating;		//get rating
	user_ratings.insert(pair<int, double>(movie, rating));
	//insert movie, rating pair into map
     }

}


//********************************************
// void reduce1()
//
// Assumes setup() has been called. And input
// is already sorted. For each user, this determines
// a confidence rating by looping through all the movies
// in common from user u and i, and summing
// 25 - (ratingu(j) - ratingi(j))^2, then divides by the 
// total number of shared movies between u and i. Then outputs
// user \t confidence rating
//**************************************************


void reduce1() {
     string line;
     int last_user = 0;		//last read in user
     double total = 0;		//total movies shared
     double numer = 0;		//numerator of equation
     int userid, movie;		//for current read in
     double rating;		//read in rating
     while(getline(cin, line)) {
	istringstream sin(line);
	sin >> userid;		//read in userid
	sin >> movie;		//read in movie
	sin >> rating;		//read in rating
	if(last_user != userid) {	//if there's a change in user
	    if(total != 0) {		//and at least 1 movie
	        cout << last_user << "\t" << numer/total << endl; //output result of equation
		last_user = userid;	//change the user you're looking at
		numer = 0;		//reset numerator
		total = 0;		//reset total movies
	    }		
        }
        total++;			//add 1 to movie total
	numer += 25 - (user_ratings[movie] - rating)*(user_ratings[movie] - rating); 
	//sum according to equation
	last_user = userid;		//change/keep last read-in user
    }
	cout << last_user << "\t" << numer/total << endl; //output last user, since while won't execute
}





int main(int argc, char* argv[]) {
    if(argc < 2) {
	cerr << "Not enough input given!\n"; exit(1);
    }
    fin.open(argv[1]);	//open user_reviews.dat
    if(fin.fail()) {
	cerr <"Error opening input file!\n"; exit(1);
    }
    setup();		//call setup
    reduce1();		//call reduce
 
    return 0;
    
}
