#include <iostream>
#include <fstream>
#include <string>
#include <sstream>
#include <queue>
#include <vector>
#include <algorithm>
#include <math.h>
#include <unistd.h>
#include "mpi.h"
#include "pthread.h"

// Enumeratie cu constante care sunt folosite in THREAD-uri
enum Threads {ROOT, HORROR, COMEDY, FANTASY, SF, ROOT_THREADS = 4, CONTINUE = 100, END = 200, DONE = 300, FINAL = 400};

// Structura unui argument, folosita in toate cele 3 functii de THREAD
struct argument {
    int genre;                              // tipul paragrafului (id-ul thread-ului in unele cazuri)
    int id;                                 // id-ul thread-ului
    int* para20_count;                      // numarul de miniparagrafe de maxim 20 de linii
    int* done;                              // numarul de miniparagrafe procesate
    int* NTHREADS;                          // numarul de thread-uri maxime
    std::string fname;                      // numele fisierului de intrare
    std::vector<std::string>* para_20;      // vectorul de miniparagrafe
    pthread_mutex_t* mutex;                 // mutex
    pthread_barrier_t* barrier;             // bariera
};