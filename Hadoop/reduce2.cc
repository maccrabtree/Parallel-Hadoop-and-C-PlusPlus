//*************************************
// File: reduce2.cc
// Author: Mackenzie Crabtree
//
// Final reduce of recommender system
// Assumes map1, reduce1, map2 have been called
// Takes from standard in output of map2 in the form
// movie \t weightedrating
// Then combines all similar movies and divides by total
// ratings for that movie. Then outputs movie \t weighted average
// Needs no further input/arguments, but accepts them
//
//
//*************************************************


#include <iostream>
#include <vector>
#include <fstream>
#include <sstream>
#include <map>
#include <cstdlib>
using namespace std;

//***************************************
// void reduce2()
// Takes from standard in output of map2 
// in the form movie/weightedrating, then simply
// reads in all same movies and adds their weightedrating
// then divides by total number of ratings for that movie
// to produce final output in the form movie/weightedaverage
//
// Assumes sorted input
//***************************************************


void reduce2() {
     string line;
     int last_movie = 0;	//last movie read-in
     double total = 0;		//total movies similar
     double numer = 0;		//numerator of division
     int curr_movie;		//current movie read in
     double rating;		//read-in rating
     while(getline(cin, line)) {
	istringstream sin(line);
	sin >> curr_movie;	//read in current movie
	sin >> rating;		//read in rating of movie
	if(last_movie != curr_movie) {	//if change in movie
	    if(total != 0) {		//at least 1 readin

	        cout << last_movie << "\t" << numer/total << endl;
		//cout movie and sum of ratings/number of ratings

		last_movie = curr_movie;	//change readin
		numer = 0;			//reset numerator
		total = 0;			//rest total
	    }
        }
        total++;	//increment total movies
	numer += rating;	//add rating
	last_movie = curr_movie;	//change/keep movie readin
    }
	cout << last_movie << "\t" << numer/total << endl;
	//output last since while won't execute
}





int main(int argc, char* argv[]) {
    reduce2(); //call reduce2
 
    return 0;
    
}
