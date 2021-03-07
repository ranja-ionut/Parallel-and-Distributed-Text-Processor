#include "tema.h"

using namespace std;

/**
 * Functie care detecteaza daca programul a fost rulat corespunzator.
 */
void init(int argc, char** argv) {
    if (argc < 2) {
        cout << "[Error] Usage: " << argv[0] << " FILENAME" << endl;
        exit(-1);
    }
}

/**
 * Functia pentru cele 4 thread-uri din MASTER care se vor ocupa de citirea
 * in paralel.
 * 
 * Fiecare thread va trimite paragraf cu paragraf spre WORKER-ul corespunzator
 * si va stoca paragrafele modificate intr-o coada.
 * 
 * La final se va face scrierea in fisierul de iesire, corespunzator cu ordinea
 * originala a paragrafelor.
 */
void* master_reader(void* arg) {
    argument args = *(argument*)arg;
    int genre = args.genre;
    string keyword = "none";
    bool isKeyword = true;
    bool inParagraph = false;
    queue<int> order;           // Coada ce stocheaza ordinea paragrafelor
    queue<string> paragraphs;   // Coada ce stocheaza paragrafele modificate

    string horror = "horror", comedy = "comedy", fantasy = "fantasy",
           sf = "science-fiction";

    /**
     * In functie de genul thread-ului din master se va alege keyword-ul ce
     * reprezinta tipul paragrafului de care se ocupa.
     */
    if (genre == HORROR) {
        keyword = horror;
    } else if (genre == COMEDY) {
        keyword = comedy;
    } else if (genre == FANTASY) {
        keyword = fantasy;
    } else if (genre == SF) {
        keyword = sf;
    }

    fstream input, output;
    string in_line, paragraph;
    int lines = 0, recv_status = CONTINUE, paragraph_size = 0;
    char *p_str;
    MPI_Status status;

    /** 
     * Se obtine numele fisierului de iesire din fisierul de intare.
     */
    string in_file, out_file;
    in_file = args.fname;
    size_t split = in_file.find("txt");
    out_file = in_file.substr(0, split).append("out");

    input.open(in_file.c_str(), ios::in);
    output.open(out_file.c_str(), ios::out | ios::app);

    /**
     * Cat timp se mai poate citi din fisierul de intrare, efectuam
     * urmatoarele operatii:
     */
    while(!input.eof()) {
        /**
         * Se citeste linie cu linie din fisierul de intrare.
         */
        getline(input, in_line);
        
        /**
         * Fiecare thread va verifica ce tip de paragraf a intalnit si ii va adauga
         * tipul in coada `order`.
         */
        if (isKeyword) {
            if (in_line.compare(0, horror.length(), horror) == 0) {
                order.push(HORROR);
            }

            if (in_line.compare(0, comedy.length(), comedy) == 0) {
                order.push(COMEDY);
            }

            if (in_line.compare(0, fantasy.length(), fantasy) == 0) {
                order.push(FANTASY);
            }

            if (in_line.compare(0, sf.length(), sf) == 0) {
                order.push(SF);
            }
        }

        /**
         *  Daca am intalnit keyword-ul, atunci marcam faptul ca ce urmeaza nu
         * va mai putea fi un keyword si ne aflam in interiorul paragrafului.
         */
        if (isKeyword && in_line.compare(0, keyword.length(), keyword) == 0) {
            isKeyword = false;
            inParagraph = true;
        } else if (inParagraph && in_line.compare("") == 0) {
            /**
             * Daca ne aflam in paragraf si am ajuns la o linie goala, atunci
             * inseamna ca am terminat cu paragraful curent si il putem trimite
             * spre a fi procesat.
             */
            
            // Marcam ca nu mai suntem in paragraf si ce urmeaza poate fi keyword.
            inParagraph = false;
            isKeyword = true;

            /**
             * Trimitem catre worker un status care ii spune ca trebuie sa continue
             * sa prelucreze alt paragraf, numarul de linii al paragrafului si 
             * paragraful in sine.
             */
            MPI_Send(&recv_status, 1, MPI_INT, genre, genre, MPI_COMM_WORLD);
            MPI_Send(&lines, 1, MPI_INT, genre, genre, MPI_COMM_WORLD);
            MPI_Send(paragraph.c_str(), paragraph.size(), MPI_CHAR, genre, genre, MPI_COMM_WORLD);

            // Resetam paragraful si numarul de linii.
            paragraph.clear();
            lines = 0;

            /**
             * Ne asteptam ca dupa ce un paragraf a fost terminat sa primim un mesaj
             * cu tag-ul FINAL.
             * 
             * Realizam un MPI_Probe ca sa obtinem informatii despre acest mesaj si
             * un MPI_Get_Count pentru a obtine dimensiunea paragrafului.
             */
            MPI_Probe(genre, FINAL, MPI_COMM_WORLD, &status);
            MPI_Get_count(&status, MPI_CHAR, &paragraph_size);

            // Alocam un buffer pentru paragraful pe care il vom primi inapoi.
            p_str = (char*)calloc(paragraph_size, sizeof(char));

            // Receptionam paragraful.
            MPI_Recv(p_str, paragraph_size, MPI_CHAR, genre, FINAL, MPI_COMM_WORLD, MPI_STATUS_IGNORE);

            // Il convertim intr-un string.
            paragraph = string(p_str, paragraph_size);

            // Il punem in coada de mesaje a thread-ului.
            paragraphs.push(paragraph);
            
            // Il resetam.
            paragraph.clear();

            // Eliberam memoria.
            free(p_str);
        } else if (inParagraph) {
            /**
             * In cazul in care ne aflam in paragraf si nu am ajuns inca la finalul acestuia,
             * vom continua sa il construim linie cu linie (astfel incrementand si numarul
             * de linii).
             */
            paragraph += in_line + "\n";
            lines++;
        }

        /**
         * Pentru cazul in care am ajuns la final si inca suntem in interiorul paragrafului,
         * atunci inseamna ca acesta este ultimul paragraf si il putem trimite.
         */
        if (input.eof()) {
            if (inParagraph) {
                /**
                 * Trimitem CONTINUE, numarul de linii si paragraful.
                 */
                MPI_Send(&recv_status, 1, MPI_INT, genre, genre, MPI_COMM_WORLD);
                MPI_Send(&lines, 1, MPI_INT, genre, genre, MPI_COMM_WORLD);
                MPI_Send(paragraph.c_str(), paragraph.size(), MPI_CHAR, genre, genre, MPI_COMM_WORLD);

                // Resetam paragraful
                paragraph.clear();

                // Obtinem dimensiunea paragrafului modificat.
                MPI_Probe(genre, FINAL, MPI_COMM_WORLD, &status);
                MPI_Get_count(&status, MPI_CHAR, &paragraph_size);

                // Alocam un buffer pentru acesta.
                p_str = (char*)calloc(paragraph_size, sizeof(char));

                // Obtinem paragraful modificat.
                MPI_Recv(p_str, paragraph_size, MPI_CHAR, genre, FINAL, MPI_COMM_WORLD, MPI_STATUS_IGNORE);

                // Il convertim la string.
                paragraph = string(p_str, paragraph_size);

                // Il adaugam in coada.
                paragraphs.push(paragraph);

                // Resetam paragraful.
                paragraph.clear();

                // Eliberam memoria buffer-ului.
                free(p_str);
            }

            /**
             *  Trimite mesaj catre WORKER ca nu mai urmeaza niciun paragraf pentru el si
             * isi poate incheia executia.
             */
            recv_status = END;
            MPI_Send(&recv_status, 1, MPI_INT, genre, genre, MPI_COMM_WORLD);
        }
    }


    // Se asteapta la bariera ca toate paragrafele sa fi fost procesate
    pthread_barrier_wait(args.barrier);

    /**
     * Fiecare thread va verifica ce thread va trebui sa scrie in fisierul de iesire,
     * iar daca el este cel ce trebuie sa o faca, atunci va scrie tipul paragrafului,
     * urmat de continut si newline; in caz contrar va trece la urmatoarea valoare
     * din coada si va astepta la bariera ca celelalte thread-uri sa isi fi terminat
     * verificarea.
     */
    while(!order.empty()) {
        if (order.front() == genre) {
            output << keyword + "\n" << paragraphs.front() << endl;
            paragraphs.pop();
        }

        order.pop();

        pthread_barrier_wait(args.barrier);
    }

    input.close();
    output.close();

    pthread_exit(NULL);
}

/**
 * Functia pentru thread-ul din WORKER ce se ocupa de primirea paragrafelor de la MASTER
 * si comunica cu celelalte thread-uri din WORKER.
 * 
 * Tot acest thread va trimite paragraful procesat inapoi la MASTER.
 */
void* worker_recv_send(void* arg) {
    argument args = *(argument*)arg;
    int genre = args.genre, NTHREADS = *(args.NTHREADS);
    int paragraph_size = 0, lines = 0, recv_status;
    char* p_str;
    string paragraph, paragraph_line;
    MPI_Status status;

    /**
     * Cat timp nu primim un END ca status de la MASTER, atunci va continua sa efectueze
     * urmatoarele operatii:
     */
    while (true) {
        // Receptioneaza status-ul de la MASTER
        MPI_Recv(&recv_status, 1, MPI_INT, ROOT, genre, MPI_COMM_WORLD, MPI_STATUS_IGNORE);

        // Daca am primit semnal de oprire
        if (recv_status == END) {
            /**
             * Pentru fiecare THREAD diferit de cel master din WORKER, acesta va trimite
             * semnale de oprire a executiei catre aceste THREAD-uri, apoi se va opri
             * si el.
             */
            for (int i = 1; i < NTHREADS; i++) {
                MPI_Send(&recv_status, 1, MPI_INT, genre, ROOT_THREADS + i, MPI_COMM_WORLD);
            }

            break;
        }

        /**
         * Primim numarul de linii de la MASTER si apoi vom determina dimensiunea
         * paragrafului, precum si paragraful in sine.
         */
        MPI_Recv(&lines, 1, MPI_INT, ROOT, genre, MPI_COMM_WORLD, MPI_STATUS_IGNORE);
        MPI_Probe(ROOT, genre, MPI_COMM_WORLD, &status);
        MPI_Get_count(&status, MPI_CHAR, &paragraph_size);
        
        // Alocam buffer pentru paragraf
        p_str = (char*)calloc(paragraph_size, sizeof(char));

        // Primim paragraful
        MPI_Recv(p_str, paragraph_size, MPI_CHAR, ROOT, genre, MPI_COMM_WORLD, MPI_STATUS_IGNORE);

        // Il convertim la string
        paragraph = string(p_str, paragraph_size);

        // Eliberam buffer-ul
        free(p_str);

        /**
         * Mai jos se realizeaza impartirea paragrafului primit intr-un vector
         * de string-uri de maxim 20 de linii fiecare.
         */
        istringstream paragraph_to_process(paragraph);
        /**
         * Obtinem numarul de miniparagrafe ce vor trebui procesate de catre
         * thread-urile de procesare (maxim NTHREADS - 1).
         */
        int para20_count = (int)ceil(lines / 20.f), count_20 = 0, count = 0;
        vector<string>* para_20 = args.para_20;

        // Resetam paragraful
        paragraph.clear();

        // Actualizam valorile comune folosite de toate NTHREADS thread-uri.
        *(args.para20_count) = para20_count;
        *(args.done) = 0;

        // Resetam vectorul de string-uri.
        para_20->clear();

        // Pentru fiecare miniparagraf de maxim 20 de linii, introducem un string gol in vector
        for (int i = 0; i < para20_count; i++) {
            para_20->push_back("");
        }

        /**
         * Din paragraful primit de la MASTER vom prelua maxim 20 de linii si le vom
         * pune in pozitia corespunzatoare din vectorul de string-uri.
         */
        while(getline(paragraph_to_process, paragraph_line)) {
            // Crestem numarul de linii si adaugam o linie in miniparagraful curent
            count++;
            (*para_20)[count_20] += paragraph_line + "\n";

            // Cand extragem 20 de linii:
            if (count == 20) {
                // Semnalam celorlalte thread-uri ca exista un alt miniparagraf ce trebuie procesat
                recv_status = CONTINUE;

                /**
                 * Trimitem catre thread-ul corespunzator semnalul si pozitia la care va
                 * trebui sa insereze paragraful procesat
                 */
                MPI_Send(&recv_status, 1, MPI_INT, genre, ROOT_THREADS + count_20 % (NTHREADS - 1) + 1, MPI_COMM_WORLD);
                MPI_Send(&count_20, 1, MPI_INT, genre, ROOT_THREADS + count_20 % (NTHREADS - 1) + 1, MPI_COMM_WORLD);
                
                // Trecem la urmatorul set de 20 si resetam numarul de linii
                count_20++;
                count = 0;
            }
        }

        // Daca la final a mai ramas un miniparagraf care nu contine maxim 20 de linii, il vom trimite spre procesare
        if (count != 0) {
            recv_status = CONTINUE;

            MPI_Send(&recv_status, 1, MPI_INT, genre, ROOT_THREADS + count_20 % (NTHREADS - 1) + 1, MPI_COMM_WORLD);
            MPI_Send(&count_20, 1, MPI_INT, genre, ROOT_THREADS + count_20 % (NTHREADS - 1) + 1, MPI_COMM_WORLD);

            count_20++;
        }

        /**
         * Asteptam ca ultimul thread care mai are de procesat miniparagrafe sa isi termine executia
         * si sa semnaleze pe tag-ul DONE faptul ca intreg paragraful a fost procesat.
         */
        MPI_Recv(&recv_status, 1, MPI_INT, genre, DONE, MPI_COMM_WORLD, MPI_STATUS_IGNORE);

        if (recv_status == DONE) {
            // Resetam paragraful
            paragraph.clear();

            /**
             * Reconstruim paragraful procesat in ordine folosind vectorul de miniparagrafe.
             */
            for (string p : *para_20) {
                paragraph += p;
            }
        }

        // Trimitem inapoi la MASTER paragraful procesat pe tag-ul FINAL
        MPI_Send(paragraph.c_str(), paragraph.size(), MPI_CHAR, ROOT, FINAL, MPI_COMM_WORLD);

        // Resetam paragraful
        paragraph.clear();
    }

    pthread_exit(NULL);
}

/**
 * Functia pentru THREAD-urile din WORKER care se ocupa de procesare miniparagrafelor
 * din paragraful mare.
 * 
 * Atat THREAD-ul master din WORKER cat si aceste thread-uri se afla pe aceeasi statie
 * in universul MPI, deci vor avea acces la acelasi spatiu de memorie, astfel se pot
 * folosi variabile comune intre cele 2 tipuri de THREAD-uri din WORKER.
 */
void* worker_process(void* arg) {
    argument args = *(argument*)arg;
    int genre = args.genre, id = args.id, recv_status, paragraph_size, index;
    MPI_Status status;
    string para_20;

    /**
     * Cat timp thread-ul master din WORKER nu a primit inca semnalul de oprire,
     * thread-urile ce se ocupa cu procesarea vor continua sa ruleze urmatoarele
     * operatii:
     */
    while(true) {
        // Receptioneaza semnalul de la master-ul din WORKER
        MPI_Recv(&recv_status, 1, MPI_INT, genre, id, MPI_COMM_WORLD, MPI_STATUS_IGNORE);

        // Daca nu mai exista paragrafe de procesat, atunci incheie executia
        if (recv_status == END) {
            break;
        }

        // Receptioneaza index-ul in care va trebui sa fie inserat paragraful procesat
        MPI_Recv(&index, 1, MPI_INT, genre, id, MPI_COMM_WORLD, MPI_STATUS_IGNORE);

        /**
         * Obtinem miniparagraful din vectorul comun si ii resetam valoarea, urmand
         * sa inseram in acelasi index noul miniparagraf.
         */
        para_20 = (*(args.para_20))[index];
        (*(args.para_20))[index].clear();

        istringstream para20_to_process(para_20);
        string processed_para20 = "", line;
        
        /**
         * Cat timp mai exista linii de procesat din miniparagraf, atunci
         * se vor executa urmatoarele operatii:
         */
        while(getline(para20_to_process, line)) {
            istringstream line_stream (line);
            string word, processed_line;
            vector<string> words;

            // Obtinem un vector de cuvinte, format din cuvintele liniei curente
            while(getline(line_stream, word, ' ')) {
                words.push_back(word);
            }
            
            /**
             * Pentru fiecare cuvant din vectorul de cuvinte se va realiza procesarea
             * acestuia si reconstruirea liniei.
             */
            if (genre == HORROR) {
                string consonants = "bcdfghjklmnpqrstvwxyzBCDFGHJKLMNPQRSTVWXYZ";

                for (string w : words) {
                    for (int i = 0; i < w.size(); i++) {
                        if(consonants.find(w[i]) != -1) {
                            string copy = w;
                            copy[i] = tolower(copy[i]);
                            w.insert(i + 1, copy, i, 1);
                            i++;
                        }
                    }

                    processed_line += w + " ";
                }
            } else if (genre == COMEDY) {
                for (string w : words) {
                    for (int i = 1; i < w.size(); i += 2) {
                        w[i] = toupper(w[i]);
                    }
                    processed_line += w + " ";
                }
            } else if (genre == FANTASY) {
                for (string w : words) {
                    w[0] = toupper(w[0]);
                    processed_line += w + " ";
                }
            } else if (genre == SF) {
                int count = 0;
                for (string w : words) {
                    count++;

                    if (count == 7) {
                        count = 0;
                        reverse(w.begin(), w.end());
                    }

                    processed_line += w + " ";
                }
            }
            
            // Miniparagraful nou va primi linia procesata
            processed_para20 += processed_line + "\n";
        }

        // Inseram in acelasi index miniparagraful procesat
        (*(args.para_20))[index] = processed_para20;

        // Resetam paragraful procesat
        processed_para20.clear();

        // ZONA CRITICA
        pthread_mutex_lock(args.mutex);

        /**
         * Fiecare thread va incrementa numarul de miniparagrafe procesate pana
         * in acest moment, iar daca cumva thread-ul curent este ultimul, acesta
         * va semnala pe tag-ul DONE catre master-ul din WORKER ca intreg
         * paragraful a fost procesat.
         */

        (*(args.done))++;

        if (*(args.done) == *(args.para20_count)) {
            recv_status = DONE;
            MPI_Send(&recv_status, 1, MPI_INT, genre, DONE, MPI_COMM_WORLD);
        }

        // END ZONA CRITICA
        pthread_mutex_unlock(args.mutex);
    }

    pthread_exit(NULL);
}

int main(int argc, char** argv) {
    int numtasks, rank, provided;

    MPI_Init_thread(&argc, &argv, MPI_THREAD_MULTIPLE, &provided);
    if (provided != MPI_THREAD_MULTIPLE) {
        cout << "[Error] MPI implementation does not support multiple threads" << endl;
        MPI_Abort(MPI_COMM_WORLD, 1);
    }

    init(argc, argv);

    MPI_Comm_size(MPI_COMM_WORLD, &numtasks);
    MPI_Comm_rank(MPI_COMM_WORLD,&rank);

    if (rank == ROOT) {
        void* status;
        pthread_t threads[ROOT_THREADS];
	    argument arguments[ROOT_THREADS];
        pthread_barrier_t barrier;
        int r;

        // Initializare bariera + thread-uri

        pthread_barrier_init(&barrier, NULL, ROOT_THREADS);

        for (int i = 0; i < ROOT_THREADS; i++) {
            arguments[i].genre = i + 1;
            arguments[i].fname = argv[1];
            arguments[i].barrier = &barrier;

            r = pthread_create(&threads[i], NULL, master_reader, &arguments[i]);

            if (r) {
                cout << "[Error] Could not create thread " << i + 1 << endl;
                exit(-1);
            }
        }

        for (int i = 0; i < ROOT_THREADS; i++) {
            r = pthread_join(threads[i], &status);

            if (r) {
                cout << "[Error] Could not join thread " << i + 1 << endl;
                exit(-1);
            }
        }

        pthread_barrier_destroy(&barrier);
    } else {
        void* status;
        int NTHREADS = max((long int)2, sysconf(_SC_NPROCESSORS_CONF));
        pthread_t threads[NTHREADS];
        pthread_mutex_t mutex;
	    argument arguments[NTHREADS];
        vector<string> para_20;
        int para20_count, done = 0;
        int r;

        // Initializare mutex + thread-uri

        pthread_mutex_init(&mutex, NULL);
        
        arguments[0].id = rank;
        arguments[0].genre = rank;
        arguments[0].para20_count = &para20_count;
        arguments[0].done = &done;
        arguments[0].NTHREADS = &NTHREADS;
        arguments[0].para_20 = &para_20;
        arguments[0].mutex = &mutex;

        r = pthread_create(&threads[0], NULL, worker_recv_send, &arguments[0]);

        if (r) {
            cout << "[Error] Could not create thread " << rank << endl;
            exit(-1);
        }


        for (int i = 1; i < NTHREADS; i++) {
            arguments[i].id = i + ROOT_THREADS;
            arguments[i].genre = rank;
            arguments[i].para20_count = &para20_count;
            arguments[i].done = &done;
            arguments[i].NTHREADS = &NTHREADS;
            arguments[i].para_20 = &para_20;
            arguments[i].mutex = &mutex;

            r = pthread_create(&threads[i], NULL, worker_process, &arguments[i]);

            if (r) {
                cout << "[Error] Could not create thread " << i + ROOT_THREADS << endl;
                exit(-1);
            }
        }
        
        for (int i = 0; i < NTHREADS; i++) {
            r = pthread_join(threads[i], &status);

            if (r) {
                cout << "[Error] Could not join thread " << rank << endl;
                exit(-1);
            }
        }

        pthread_mutex_destroy(&mutex);
    }

    MPI_Finalize();
}