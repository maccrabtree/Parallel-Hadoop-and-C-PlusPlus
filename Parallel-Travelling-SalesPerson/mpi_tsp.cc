//***************************************************************
// File: Dynamic TSP.cc
// Authors: Joseph Scott and Mackenzie Crabtree
// MPI implementation of the held-karp algorithm to solve 
// Travelling salesman problem. Heavily built on code by 
// Dr. Juedes, with MPI overtop. Executes each set analysis of 
// size i to n parallel, then sends packets of changed information
// via broadcasts by each processor so each table maintains
// own tables of values. Proc 0 then displays answer
//***************************************************************
#include <iostream>
#include <float.h>
#include <string>
#include <sstream>
#include <cassert>
#include <vector>
#include <omp.h>
#include <mpi.h>
using namespace std;

struct Cpacket	//Changes in C table
{
    int i, j;
    double c;
};

struct Fpacket 	//Changes in F table
{
    int i, j;
    int f;
};

vector<int> Minimum_TSP_tour3(vector<vector<double> > &weights, vector<vector<int> > adj_mat,double &total_dist, bool &found_it, int pid, int np)
{
    const int s = 3;				//Modified Code found online, can't remember link
						//somewhere from stack overlow for creating MPI_Structures
    int blocklengths[s] = {1, 1, 1};
    MPI_Datatype types[s] = {MPI_INT, MPI_INT, MPI_DOUBLE};
    MPI_Datatype MPI_CPacket;
    MPI_Aint offset[s];
    offset[0] = offsetof(Cpacket, i);
    offset[1] = offsetof(Cpacket, j);
    offset[2] = offsetof(Cpacket, c);

    MPI_Type_create_struct(s, blocklengths, offset, types, &MPI_CPacket);
    MPI_Type_commit(&MPI_CPacket);

    int blocklengths2[s] = {1, 1, 1};
    MPI_Datatype types2[s] = {MPI_INT, MPI_INT, MPI_INT};
    MPI_Datatype MPI_FPacket;
    MPI_Aint offset2[s];
    offset2[0] = offsetof(Fpacket, i);
    offset2[1] = offsetof(Fpacket, j);
    offset2[2] = offsetof(Fpacket, f);

    MPI_Type_create_struct(s, blocklengths2, offset2, types2, &MPI_FPacket);
    MPI_Type_commit(&MPI_FPacket);        //MPI Struct now ready

    int bits[] = {0, 1, 2, 4, 8, 16, 32, 64, 128, 256, 512, 1024, 2048, 4096, 8192, 16384, 32768, 65536, 131072, 262144, 524288, 1048576, 2097152, 4194304, 8388608, 16777216, 33554432, 67108864, 134217728, 268435456, 536870912, 1073741824};

    long long n = adj_mat.size();
    vector<vector<int> > buckets;
    buckets.resize(n); //
    long long max_sets = 1 << (n - 1);
    buckets[1].push_back(1);
    int last_count = 1;
    for (int i = 2; i < max_sets; i++)	//Each core calculates their own buckets
    {
        // Sort by size of set
        int count = 0;
        for (int j = 1; j < n; j++)
        {
            if ((bits[j]&(i - 1)) == 0)
            {
                // First zero bit
                last_count = last_count + 2 - j;
                buckets[last_count].push_back(i);
                break;
            }
        }
    }
    vector<vector<double> > C(max_sets + 1, vector<double>(n, -1));
    vector<Fpacket> fpackets;
    vector<vector<int> > F(max_sets + 1, vector<int>(n, -1));
    for (int l1 = 1; l1 < n; l1++)								//ALgorithm very similar to code given by Dr. Juedes
    {
        vector<Cpacket> cpackets;		//Changes in C table

        int work = buckets[l1].size() / (np);
        int start = pid*work;
        int stop = (pid + 1) * work;
        if (pid == np - 1)			//Calculate work for each core, and append extra to the last process
        {
            stop = buckets[l1].size();
        }
	if(work == 0) {  //if work left is less than num of processes
 		start = pid;
		stop = pid;
		if(pid < buckets[l1].size()) stop++;
		//only do work if your process fits bound
        }
        for (start; start < stop; start++)
        {
            int i = buckets[l1][start];
            for (int l = 1; l < n; l++)
            {
                //   --- calculate C(S,l)
                if ((i & bits[l]) > 0)
                { // --- I.e., is bit l set in the integer i
                    if (i == bits[l])
                    { // --- (Set S == {l} )
                        if (adj_mat[0][l])
                        {
                            C[i][l] = weights[0][l];
                            F[i][l] = 0;
                            Cpacket cp;
                            cp.i = i;
                            cp.j = l;
                            cp.c = weights[0][l];
                            cpackets.push_back(cp);		//We have a change in the C table, so we send its change encoded in the packet

                            Fpacket fp;				//Similarly for the F table
                            fp.i = i;
                            fp.j = l;
                            fp.f = 0;
                            fpackets.push_back(fp);
                        }
                    }
                    else
                    {
                        double mindist = 10000000.0;
                        bool found = false;
                        int from = -1;
                        for (int j = 1; j < n; j++)
                        {
                            if (((i - bits[l]) & bits[j]) > 0)
                            { // j is an element of S-{l}
                                if (adj_mat[j][l])
                                {
                                    if (C[i - bits[l]][j] >= 0)
                                    { // Exists
                                        double t = C[i - bits[l]][j] + weights[j][l];
                                        found = true;
                                        if (t < mindist)
                                        {
                                            mindist = t;
                                            from = j;
                                        }
                                    }
                                }
                            }
                        }
                        if (found)
                        {
                            C[i][l] = mindist;			//Once again, send C and F changes
                            F[i][l] = from;
                            Cpacket cp;
                            cp.i = i;
                            cp.j = l;
                            cp.c = mindist;
                            cpackets.push_back(cp);

                            Fpacket fp;
                            fp.i = i;
                            fp.j = l;
                            fp.f = from;
                            fpackets.push_back(fp);
                        }
                    }
                }
            }
        }
        vector<vector< Cpacket> > pack_table(np);	//Store changes by other processors in a table 
        int size, mysize = cpackets.size();
        pack_table[pid] = cpackets;                     //save own changes in the table
        for (int i = 0; i < np; i++)			//After each iteration of se size, we need to update the C values
        {
            size = mysize;	//save size
            vector<Cpacket> tmp = pack_table[pid];    //make temp vector in case overwritten
            MPI_Bcast(&size, 1, MPI_INT, i, MPI_COMM_WORLD);		//Have each processor broadcast its calculated value to the other cores
            tmp.resize(size);			
            MPI_Bcast(&tmp[0], size, MPI_CPacket, i, MPI_COMM_WORLD);
            pack_table[i] = tmp;   //store the received vector in the pack table
        }
        for (int i = 0; i < pack_table.size(); i++) //go through received packets and update C table
        {
            for (int j = 0; j < pack_table[i].size(); j++)
            {
                C[pack_table[i][j].i][pack_table[i][j].j] = pack_table[i][j].c;		//Iterate through the C change table, and update the C  value after the last set size iteration
            }
        }
    }
    if (pid == 0)					//At this point the Optimal Tour is calculated
    {
        for (int i = 0; i < fpackets.size(); i++)	//F table independent from calculation, and hence we do not broadcast in the loop, just at end (here)
        {
            F[fpackets[i].i][fpackets[i].j] = fpackets[i].f;
        }
        vector<Fpacket> fpacks;
        for (int ip = 1; ip < np; ip++)
        {
            int size;
            MPI_Recv(&size, 1, MPI_INT, ip, 0, MPI_COMM_WORLD, MPI_STATUS_IGNORE);		//Recive f changes from all other processors.
            fpacks.resize(size);
            vector<Fpacket> fpacks(size);
            MPI_Recv(&fpacks[0], size, MPI_FPacket, ip, 0, MPI_COMM_WORLD, MPI_STATUS_IGNORE);

            for (int i = 0; i < fpacks.size(); i++)
            {
                F[fpacks[i].i][fpacks[i].j] = fpacks[i].f;		//Unpack the values found.
            }
        }
    }
    else
    {
        int mysize = fpackets.size();
        MPI_Send(&mysize, 1, MPI_INT, 0, 0, MPI_COMM_WORLD);					//Send F table changes to pid= 0
        MPI_Send(&fpackets[0], fpackets.size(), MPI_FPacket, 0, 0, MPI_COMM_WORLD);
    }
    if (pid == 0)
    {
        vector<int> opt;					//Trace back and calculate optimal tour.
        int BIGS = max_sets - 1;
        double mindist = 100000000.0; // Big Value;
        int from = -1;
        bool found = false;
        for (int l = 1; l < n; l++)
        {
            if (adj_mat[l][0])
            {
                if (C[BIGS][l] >= 0.0)
                {
                    double t = C[BIGS][l] + weights[l][0];
                    found = true;
                    if (t < mindist)
                    {
                        mindist = t;
                        from = l;
                    }
                }
            }
        }
        opt.push_back(from);
        if (found)
        {
            while (from != 0)
            {
                int f;
                f = F[BIGS][from];
                BIGS = BIGS - bits[from];
                from = f;
                opt.push_back(from);
            }
            total_dist = mindist;
        }
        found_it = found;
        return opt;
    }
    return vector<int>();
}

int main(int argc, char *argv[])
{
    int np;
    int pid;
    MPI_Init(&argc, &argv);
    MPI_Comm_size(MPI_COMM_WORLD, &np);
    MPI_Comm_rank(MPI_COMM_WORLD, &pid);
    int n;
    vector<vector<double> > weights;
    vector<vector<int> > adj_mat;
    if (pid == 0)
    {
        cin >> n;

        string line;
        getline(cin, line); // Get the newline
        adj_mat.resize(n);
	weights.resize(n);
        for (int i = 0; i < n; i++)
        {
            adj_mat[i].resize(n, false);
	    weights[i].resize(n,-1);
        }
        while (!cin.eof())
        {
            getline(cin, line);
            if (!cin.fail())
            {
                istringstream in(line);

                int j;
                in >> j;
                char c;
                in >> c;
                while (!in.eof())
                {
                    int i;
                    double w;
                    in >> i;
                    in >> w;
                    adj_mat[j][i] = true;
		    weights[j][i] = w;
                }
            }
        }
    }
    MPI_Bcast(&n, 1, MPI_INT, 0, MPI_COMM_WORLD);
    if (pid != 0)
    {
        adj_mat.resize(n);
        weights.resize(n);
	for (int i = 0; i < n; i++)
        {
	  adj_mat[i].resize(n, false);
	  weights[i].resize(n,-1);
        }
    }
    for (int i = 0; i < n; i++)
    {
        MPI_Bcast(&adj_mat[i][0], n, MPI_INT,  0, MPI_COMM_WORLD);
	MPI_Bcast(&weights[i][0], n, MPI_DOUBLE,0, MPI_COMM_WORLD);
    }
    vector<int> opt;
    bool found;
    double opt_dist;
    opt = Minimum_TSP_tour3(weights,adj_mat, opt_dist, found, pid, np);
    if (found && pid == 0)
    {
        cout << "Optimum TSP Tour length = " << opt_dist << endl;
        cout << "Tour = ";
        for (int i = 0; i < opt.size(); i++)
        {
            cout << opt[i] << " ";
        }
        cout << endl;
    }
    else if (pid == 0)
    {
        cout << "No Hamiltonian Tour." << endl;
    }


    MPI_Finalize();
}
