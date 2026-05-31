#define _GNU_SOURCE
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <time.h>
#include <sys/ipc.h>
#include <sys/types.h>
#include <sys/sem.h>
#include <sys/msg.h>
#include <sys/shm.h>
#include <signal.h>
#include <sys/signal.h>
#include <errno.h>
#include <unistd.h>
#include "myheader.h"

/*Dichiarazioni delle funzioni - Processo porto*/

void generaMerce(struct merci_porto** merci, int merce_q, struct shmem_parametri *init_shmem, int* dati_merci, int timer, int* dump_merci, int semid_merci, int* dump_porti, int fork_i);
void merceScaduta(struct merci_porto** merci, int days, int num_merci, int* dump_merci, int semid_merci, int* dump_porti, int fork_i);
void caricaNave(int porto_msg_ID, int pid, int ton, struct merci_porto** magazzino, struct shmem_parametri *shmem, int semid, int* dump_merci, int* dump_porti, int fork_i);
static void userSignalHandler(int sig);
static void termSignalHandler(int sig);
static void swellSignalHandler(int sig);

static int usr1_occurred = 0;
static long int swell;
static int term_occurred = 0;

static struct shmem_parametri* glob_init_shmem;
static int* glob_dump_merci;
static int *glob_dump_porti;
static double* glob_dati_porti;
static int* glob_dati_merci;
static int* glob_dati_magazzino;
static int* glob_porti_merci;
static int glob_dati_magazzino_ID;
static int glob_risorsa_sem_ID;
static int glob_porto_msg_ID;
static int glob_init_sem_ID;
static int glob_fork_i;



/*Funzione main*/

int main( int argc, char* argv[]){
	int banchine = 0;
	int banchine_libere;
	double posx = 0;
	double posy = 0;
	int init_shmem_ID;
	int init_sem_ID;
	struct sembuf initsops;
	struct shmem_parametri *init_shmem = NULL; 
	int fork_i;
	
	int days = 0;
	int porto_msg_ID;
	struct msg_pn msg_invio;
	struct msg_np msg_ricevo;
	int i_ciclomessaggio;
	int q_caricamento;
	
	struct merci_porto **merci = NULL;
	int risorsa_sem_ID;
	struct sembuf rissops;
	union semun arg_rissops;
	union semun temp_rissops;
	int tmp;
	int rissem_i;
	int merci_i;
	int merce_q;
	int genMerci_i;
	int gen_i;
	int one_mer_fork;
	
	int* dump_porti; /*pid presente spedita ricevuta tot occup*/
	int* dump_merci; /*0 in porto 1 in nave 2 consegnata 3 scadporto 4 scadnave*/
	int merci_shmem_ID;
	int merci_sem_ID;
	
	double* dati_porti; /*0 pid, 1 x, 2 y, 3 msgid, 4 shmid, 5 semid 6 swell, 7 swell_ongoing*/
	int* dati_merci; /*0 qta, 1 scad*/
	int dati_magazzino_ID;
	int* dati_magazzino;
	
	int* porti_merci; /*porti x merci, quantità prodotta*/
	int porti_merci_ID;
	
	struct msg_porto msg;
	int msg_loop_i;
	
	struct sigaction userHandlerPointer;
	struct sigaction swellHandlerPointer;
	struct sigaction termHandlerPointer;
	sigset_t blockmask;
	sigset_t oldmask;

	
	/*Assegnamento handler*/
	userHandlerPointer.sa_handler = userSignalHandler;
	termHandlerPointer.sa_handler = termSignalHandler;
	swellHandlerPointer.sa_handler = swellSignalHandler;

	
	
	
	/*Inizializzo srand per generazione numeri casuali*/
	srand(getpid());
	
	/*Attacco memoria condivisa di inizializzazione, inizializzo variabili*/

	init_shmem_ID = atoi(argv[0]);
	if ((init_shmem = (struct shmem_parametri*) shmat(init_shmem_ID, NULL, 0)) < (struct shmem_parametri*) 0){
		printf("Errore nell'attacco a memoria condivisa init - Processo porto\n");
		exit(1);
	}
	glob_init_shmem = init_shmem;

	banchine = (rand() % (init_shmem -> SO_BANCHINE)) + 1;
	swell = (((long int)init_shmem->SO_SWELL_DURATION)*1000000000)/24;
	
	fork_i = atoi(argv[1]);
	glob_fork_i = fork_i;
	switch (fork_i){
		case 0:
			posx = 0;
			posy = 0;
			break;
		case 1:
			posx = 0;
			posy = (init_shmem->SO_LATO);
			break;
		case 2:
			posx = (init_shmem->SO_LATO);
			posy = 0;
			break;
		case 3:
			posx = (init_shmem->SO_LATO);
			posy = (init_shmem->SO_LATO);
			break;
		
		default:
			posx = (double)(rand()%(int)(init_shmem->SO_LATO + 1));
			posy = (double)(rand()%(int)(init_shmem->SO_LATO + 1));
			break;
	
	}
	
	/*Attacco memoria condivisa per dati porti*/
	if((dati_porti = (double*)shmat(atoi(argv[6]), NULL, 0)) < (double*) 0){
		printf("Errore attacco memoria condivisa dati porti - Processo porto\n");
		exit(1);
	}
	dati_porti[fork_i*8 + 0] = (double)getpid();
	dati_porti[fork_i*8 + 1] = posx;
	dati_porti[fork_i*8 + 2] = posy;
	glob_dati_porti = dati_porti;
	
	/*Attacco memoria condivisa per dump porti*/
	if((dump_porti = (int*)shmat(atoi(argv[3]), NULL, 0)) < (int*) 0){
		printf("Errore attacco memoria condivisa dati porti - Processo porto\n");
		exit(1);
	}
	dump_porti[fork_i*6 + 0] = getpid();
	dump_porti[fork_i*6 + 4] = banchine;
	glob_dump_porti = dump_porti;
	
	/*Attacco memoria condivisa per dati merci*/
	if((dati_merci = (int*)shmat(atoi(argv[7]), NULL, 0)) < (int*) 0){
		printf("Errore attacco memoria condivisa dati merci - Processo porto\n");
		exit(1);
	}
	glob_dati_merci = dati_merci;
	
	/*Attacco memoria condivisa per il dump delle merci*/
	merci_shmem_ID = atoi(argv[4]);
	merci_sem_ID = atoi(argv[5]);
	if ((dump_merci = (int*) shmat(merci_shmem_ID, NULL, 0)) < (int*) 0){
		printf("Errore nell'attacco a memoria condivisa merci - Processo porto\n");
		exit(1);
	}
	glob_dump_merci = dump_merci;
	
	/*Attacco memoria condivisa per dump porti x merci*/
	porti_merci_ID = atoi(argv[8]);
	if((porti_merci = (int*)shmat(porti_merci_ID, NULL, 0)) == (int*)-1){
		printf("Errore attacco a memoria di dump merci x porti - Processo porto");
		exit(1);
	}
	glob_porti_merci = porti_merci;
	
	/*Alloco array "magazzino" per le merci*/
	/*Creo un semaforo inizializzato al numero di banchine disponibili*/
	arg_rissops.val = banchine;
	
	if((risorsa_sem_ID = semget(IPC_PRIVATE, 1, 0666|IPC_CREAT)) == -1){
		printf("Errore allocazione semaforo porto\n");
		exit(1);
	}
	if((semctl(risorsa_sem_ID, 0, SETVAL, arg_rissops)) == -1){
		printf("Errore inizializzazione semaforo porto\n");
		exit(1);
	}
	glob_risorsa_sem_ID = risorsa_sem_ID;
	dati_porti[fork_i*8 + 5] = risorsa_sem_ID;
	
	
	/*Alloco array magazzino e array di controllo*/
	merci = (struct merci_porto**)calloc(init_shmem->SO_MERCI, sizeof(struct merci_porto*));
	dati_magazzino_ID = shmget(IPC_PRIVATE, sizeof(int)*init_shmem->SO_MERCI, 0666|IPC_CREAT);
	dati_magazzino = (int*)shmat(dati_magazzino_ID, NULL, 0);
	dati_porti[fork_i*8 + 4] = dati_magazzino_ID;
	glob_dati_magazzino = dati_magazzino;
	glob_dati_magazzino_ID = dati_magazzino_ID;
	
	/*Creo coda di messaggi per comunicazione con le navi*/
	if((porto_msg_ID = msgget(IPC_PRIVATE, 0666)) == -1){
		printf("Errore creazione coda di messaggi - Processo porto\n");
		exit(1);
	}
	glob_porto_msg_ID = porto_msg_ID;
	dati_porti[fork_i*8 + 3] = (double)porto_msg_ID;
	
	gen_i = fork_i%2;
	/*Alloco ogni entry dell'array*/
	for(merci_i = 0; merci_i < init_shmem->SO_MERCI; merci_i++){
		merci[merci_i] = (struct merci_porto*)malloc(sizeof(struct merci_porto));
		merci[merci_i]->lotti = NULL;
		merci[merci_i]->coda = NULL;
		merci[merci_i]->richiesta = 0;
		merci[merci_i]->ricevuta = 0;
		merci[merci_i]->scaduta = 0;
		merci[merci_i]->disponibile = 0;
		merci[merci_i]->spedita = 0;
		/*Uso un indice gen_i dipendente da fork_i per decidere se generare le merci in posizione pari oppure in posizione dispari*/
		if(merci_i % 2 == gen_i) {
			merci[merci_i]->dom_off = 0;
			merci[merci_i]->richiesta = (((init_shmem->SO_FILL)/(init_shmem->SO_PORTI)) / (init_shmem->SO_MERCI/(1+(init_shmem->SO_MERCI > 1))));
			dati_magazzino[merci_i] = merci[merci_i]->richiesta;
		} else {
			merci[merci_i]->dom_off = 1;
			dati_magazzino[merci_i] = 0;
		}
		
	}
	
	/*Installazione nuovi handler di segnali*/
	userHandlerPointer.sa_flags = SA_RESTART;
	if(sigaction(SIGUSR1, &userHandlerPointer, NULL) == -1){
		printf("Errore installazione handler di segnali user - Processo porto\n");
		exit(1);
	}
	if(sigaction(SIGUSR2, &swellHandlerPointer, NULL) == -1){
		printf("Errore installazione handler di segnali swell - Processo porto\n");
		exit(1);
	}
	if(sigaction(SIGTERM, &termHandlerPointer, NULL) == -1){
		printf("Errore installazione handler di segnali term - Processo porto\n");
		exit(1);
	}

	/*Attendo segnale di via libera per iniziare la simulazione*/
	/*Opero sul semaforo passato dal master*/
	/*Decremento il valore del semaforo*/
	init_sem_ID = atoi(argv[2]);
	glob_init_sem_ID = init_sem_ID;
	initsops.sem_num = 0;
	initsops.sem_op = -1;
	initsops.sem_flg = 0;
	if(semop(init_sem_ID, &initsops, 1) == -1){
		printf("Errore nel decrementare il semaforo - Porto\n");
		exit(1);
	}
	
	/*Attendo che il valore del semaforo sia zero. In tal caso, tutti i processi sono pronti e la simulazione può avere inizio*/
	initsops.sem_op = 0;
	if(semop(init_sem_ID, &initsops, 1) == -1){
		printf("Errore nell'attesa del semaforo - Porto\n");
		exit(1);
	}
	
	/*Inizio generazione merci*/
	/*Per garantire una buona disponibilità di merce, inizialmente lancio la funzione più volte*/
	/*Inizialmente, tutti i porti generano un po' di merce*/
	genMerci_i = init_shmem->SO_MERCI;
	merce_q = (((init_shmem->SO_FILL)/(init_shmem->SO_DAYS)) / (init_shmem->SO_PORTI/(1+(init_shmem->SO_MERCI == 1)))) / (genMerci_i);
	
	while(genMerci_i > 0){
		generaMerce(merci, merce_q, init_shmem, dati_merci, days, dump_merci, merci_sem_ID, dump_porti, fork_i);
		genMerci_i--;
	}
	
	/*Inizio simulazione - mi metto in attesa del segnale giornaliero. Attesa eterna ciclica fino a terminazione simulazione*/
	/*Se riceve il segnale, svolge operazioni:*/
	/*Incrementa timer days di 1, dealloca merce scaduta, invia messaggio report giornaliero al Master,*/
	/*Decide se generare nuova merce, torna a dormire*/
	for(;;){
		/*Gestione merci scadute e report giornaliero*/
		if (usr1_occurred == 1){
			sigfillset(&blockmask);
			sigdelset(&blockmask, SIGTERM);
			sigprocmask(SIG_BLOCK, &blockmask, &oldmask);
			
			days = init_shmem->days;
			if(!term_occurred){
				merceScaduta(merci, days, init_shmem->SO_MERCI, dump_merci, merci_sem_ID, dump_porti, fork_i);
			}

			if((banchine_libere = semctl(risorsa_sem_ID, 0, GETVAL, 0)) == -1){
				printf("Errore semctl - Processo porto\n");
				exit(1);
			}
			dump_porti[fork_i*6 + 5] = banchine - banchine_libere;
			usr1_occurred = 0;
			
			sigprocmask(SIG_SETMASK, &oldmask, NULL);
			
			/*Estraggo se generare merci o no*/
			if(!term_occurred){
				gen_i = days%2;
				if(init_shmem->SO_MERCI == 1){
					one_mer_fork = (fork_i-1)%4;
					one_mer_fork = 1 - (one_mer_fork == 0);
					if(one_mer_fork == gen_i){
						/*if(fork_i%4 == 0 && init_shmem->SO_PORTI%2 != 0){
							merce_q = ((init_shmem->SO_FILL)/(init_shmem->SO_DAYS)) / ((init_shmem->SO_PORTI/4));
						} else {
							merce_q = ((init_shmem->SO_FILL)/(init_shmem->SO_DAYS)) / (init_shmem->SO_PORTI/4);
						}*/
						merce_q = ((init_shmem->SO_FILL)/(init_shmem->SO_DAYS)) / ((init_shmem->SO_PORTI/4));
						generaMerce(merci, merce_q, init_shmem, dati_merci, days, dump_merci, merci_sem_ID, dump_porti, fork_i);
					}
				}else{
					if((fork_i%2) == gen_i){
						if(fork_i%2 == 0 && init_shmem->SO_PORTI%2 != 0){
							merce_q = ((init_shmem->SO_FILL)/(init_shmem->SO_DAYS)) / ((init_shmem->SO_PORTI/2)+1);
						} else {
							merce_q = ((init_shmem->SO_FILL)/(init_shmem->SO_DAYS)) / (init_shmem->SO_PORTI/2);
						}
						generaMerce(merci, merce_q, init_shmem, dati_merci, days, dump_merci, merci_sem_ID, dump_porti, fork_i);
					}
				}
			}
		} else {
			/*Quando non giungono segnali, il porto gestisce le richieste delle navi*/
			if((msgrcv(porto_msg_ID, &msg_ricevo, sizeof(struct msg_np)-sizeof(long), 1, 0)) == -1){
				if(errno == EINTR){
				}else{
					printf("Errore msgrcv - Processo porto\n");
				}
				
			} else {
				if(msg_ricevo.caricoScarico == 0){
					caricaNave(porto_msg_ID, msg_ricevo.pid_n, msg_ricevo.ton, merci, init_shmem, merci_sem_ID, dump_merci, dump_porti, fork_i); 
					
				} else if (msg_ricevo.caricoScarico == 1) {
					merci[msg_ricevo.tipo]->ricevuta += msg_ricevo.quantita;
					merci[msg_ricevo.tipo]->richiesta -= msg_ricevo.quantita;
					dump_porti[fork_i*6 + 3] += msg_ricevo.quantita;
					
				}
			}
		}
	}
	
}

/*Funzione per generare nuovi lotti di merce*/
void generaMerce(struct merci_porto** merci, int merce_q, struct shmem_parametri *init_shmem, int* dati_merci, int timer, int* dump_merci, int semid_merci, int* dump_porti, int fork_i){
	int merce_i;
	int lotti = 0;
	struct merce *lotto = NULL;
	
	sigset_t blockmask;
	sigset_t oldmask;
	
	sigfillset(&blockmask);
	sigdelset(&blockmask, SIGTERM);
	sigprocmask(SIG_BLOCK, &blockmask, &oldmask);
	
	/*Scelgo un indice di merce casuale*/
	/*Se la merce specifica non è in generazione, scelgo la prima merce successiva in generazione*/
	/*Questo algoritmo funziona perchè abbiamo disposto le merci generate/richieste in posizioni alterne*/
	/*Nel caso peggiore, le due estremità dell'array di dimensione dispari saranno entrambe in domanda e compierò due iterazioni*/
	merce_i = (rand() % (init_shmem -> SO_MERCI));
	
	if(init_shmem->SO_MERCI == 1 && merci[0]->dom_off == 0){
	}else{
	while(merci[merce_i]->dom_off != 1){
		merce_i = (merce_i + 1)%(init_shmem -> SO_MERCI);
	}
	/*Valuto quanti lotti generare*/
	while((lotti+1)*(dati_merci[merce_i*2 + 0]) <= merce_q){
		lotti++;
	}
	
	/*Genero i lotti di merce, inserisco in coda*/
	/*Operazione controllata da un semaforo che protegge la matrice contenente i dati necessari al dump delle merci*/
	if(reserveSem(semid_merci, merce_i) == -1){
		printf("Errore nella reserve semaforo dump - generazione merce\n");
		exit(1);
	}
	while(lotti>0){
		lotto = (struct merce*)malloc(sizeof(struct merce));
		lotto->tipo = merce_i;
		lotto->quantita = dati_merci[merce_i*2 + 0];
		lotto->scadenza = dati_merci[merce_i*2 + 1] + timer;
		lotto->stato_flag = 0;
		lotto->next = NULL;
		if(merci[merce_i]->lotti == NULL && merci[merce_i]->coda == NULL){
			merci[merce_i]->lotti = lotto;
			merci[merce_i]->coda = lotto;
		} else {
			merci[merce_i]->coda->next = lotto;
			merci[merce_i]->coda = lotto;
		}
		merci[merce_i]->disponibile += lotto->quantita;
		dump_merci[merce_i*5 + 0] += lotto->quantita;
		dump_porti[fork_i*6 + 1] += lotto->quantita;
		glob_porti_merci[fork_i*init_shmem->SO_MERCI + merce_i] += lotto->quantita; 
		lotti--;
	}
	if(releaseSem(semid_merci, merce_i) == -1){
		printf("Errore nella release semaforo dump - generazione merce\n");
		exit(1);
	}
	}
	
	sigprocmask(SIG_SETMASK, &oldmask, NULL);
	
	return;	
}

/*Funzione che dealloca la merce scaduta*/
void merceScaduta(struct merci_porto** merci, int days, int num_merci, int* dump_merci, int semid_merci, int* dump_porti, int fork_i){
	int i;
	struct merce* p = NULL, *q = NULL;
	
	sigset_t blockmask;
	sigset_t oldmask;
	
	sigfillset(&blockmask);
	sigdelset(&blockmask, SIGTERM);
	sigprocmask(SIG_BLOCK, &blockmask, &oldmask);
	
	for(i = 0; i<num_merci; i++){
		if(merci[i]->dom_off == 1){
			/*Siccome le merci nuove sono inserite in coda, le merci vecchie saranno sempre in testa. Rimuovo solo in testa*/
				if(reserveSem(semid_merci, i) == -1){
					printf("Errore nella reserve semaforo dump dealloc - scadenza merce\n");
					exit(1);
				} else {
				p = merci[i]->lotti;
				while(p!=NULL && p->scadenza <= days){
					if(p == merci[i]->coda){
						merci[i]->disponibile -= p->quantita;
						dump_porti[fork_i*6 + 1] -= p->quantita;
						merci[i]->scaduta += p->quantita;
						dump_merci[i*5 + 0] -= p->quantita;
						dump_merci[i*5 + 3] += p->quantita;
						merci[i]->coda = NULL;
						merci[i]->lotti = NULL;
						free(p);
					} else {
						merci[i]->disponibile -= p->quantita;
						dump_porti[fork_i*6 + 1] -= p->quantita;
						merci[i]->scaduta += p->quantita;
						dump_merci[i*5 + 0] -= p->quantita;
						dump_merci[i*5 + 3] += p->quantita;
						merci[i]->lotti = p->next;
						free(p);
					}
					p = merci[i]->lotti;
				}
				if(releaseSem(semid_merci, i) == -1){
					printf("Errore nella release semaforo dump dealloc - scadenza merce\n");
					exit(1);
				}
			}
				
		}
	}
	
	sigprocmask(SIG_SETMASK, &oldmask, NULL);
	
	return;
}

void caricaNave(int porto_msg_ID, int pid, int ton, struct merci_porto** magazzino, struct shmem_parametri *shmem, int semid, int* dump_merci, int* dump_porti, int fork_i){
	int i;
	int count_fallimento = 0;
	struct merce* p;
	int n_i = 0;
	int found = 0;
	struct msg_pn msg;
	
	sigset_t blockmask;
	sigset_t oldmask;
	
	sigfillset(&blockmask);
	sigdelset(&blockmask, SIGTERM);
	
	
	/*Prendo un lotto per ogni tipo di merce offerta dal porto, fino a esaurimento capacità a bordo od offerta in porto*/
	while((ton) > 0 && count_fallimento < shmem->SO_MERCI){
			count_fallimento = 0;
			for(i = 0; i < shmem->SO_MERCI; i++){
				if(magazzino[i]->dom_off == 1 && magazzino[i]->disponibile > 0 && magazzino[i]->lotti->quantita <= (ton)){
					
					sigprocmask(SIG_BLOCK, &blockmask, &oldmask);
					if(reserveSem(semid, i) == -1){
						printf("Errore nel decrementare il semaforo dumpmerce - Processo porto\n");
						exit(1);
					}
					sigprocmask(SIG_SETMASK, &oldmask, NULL);
					
					/*Compio op carico*/
					p = magazzino[i]->lotti;
					magazzino[i]->lotti = p->next;
					/*caso lotto unico*/
					if(p->next == NULL){
						magazzino[i]->coda = NULL;
					}
					magazzino[i]->disponibile -= p->quantita;
					magazzino[i]->spedita += p->quantita;
					
					msg.mtype = pid;
					msg.tipo = p->tipo;
					msg.quantita = p->quantita;
					msg.scadenza = p->scadenza;
					
					sigprocmask(SIG_BLOCK, &blockmask, &oldmask);
					if(msgsnd(porto_msg_ID, &msg, sizeof(struct msg_pn)-sizeof(long), 0) == -1){
						printf("Errore msgsnd caricanave - Processo porto\n");
					}
					sigprocmask(SIG_SETMASK, &oldmask, NULL);
	
					dump_merci[i*5 + 1] += p->quantita;
					dump_merci[i*5 + 0] -= p->quantita;
					dump_porti[fork_i*6 + 1] -= p->quantita;
					dump_porti[fork_i*6 + 2] += p->quantita;
					ton -= p->quantita;
					
					free(p);
					
					sigprocmask(SIG_BLOCK, &blockmask, &oldmask);
					if(releaseSem(semid, i) == -1){
						printf("Errore nel rilasciare il semaforo dumpmerce - Processo porto\n");
						exit(1);
					}
					sigprocmask(SIG_SETMASK, &oldmask, NULL);
				}
				else{
					/*Variabile che conteggia i fallimenti. Se nessuna merce può più essere caricata, termino op caricamento*/
					count_fallimento++;
				}
			}
		}
		/*Invio messaggio di terminazione*/
		msg.mtype = pid;
		msg.tipo = -1;
		msg.quantita = 0;
		msg.scadenza = 0;
		
		sigprocmask(SIG_BLOCK, &blockmask, &oldmask);
		msgsnd(porto_msg_ID, &msg, sizeof(struct msg_pn)-sizeof(long), 0);
		sigprocmask(SIG_SETMASK, &oldmask, NULL);
		
		return;
}

/*Handler di segnali*/
void userSignalHandler(int sig){
	if (sig == SIGUSR1){
		usr1_occurred = 1;
	}
	return;
}

void swellSignalHandler(int sig){
	struct timespec tim1, tim2;
	sigset_t blockmask;
	sigset_t oldmask;
	long int sleeptime = swell;
	
	tim1.tv_sec = 0;
	while(sleeptime >= 1000000000){
		sleeptime -= 1000000000;
		tim1.tv_sec++;
	}
	tim1.tv_nsec = sleeptime;
	
	/*tim1.tv_sec = 0;
	tim1.tv_nsec = swell;*/
	sigfillset(&blockmask);
	sigdelset(&blockmask, SIGTERM);
	sigprocmask(SIG_BLOCK, &blockmask, &oldmask);
	if (sig == SIGUSR2){
		nanosleep(&tim1, &tim2);
	}
	glob_dati_porti[glob_fork_i*8 + 7] = 0;
	sigprocmask(SIG_SETMASK, &oldmask, NULL);
	return;
}

void termSignalHandler(int sig){
	struct sembuf initsops;
	if (sig == SIGTERM){
		term_occurred = 1;
		/*Termina simulazione con successo*/
			if(shmdt(glob_init_shmem) == -1){
				printf("Errore shmdt - Processo porto\n");
			}
			if(shmdt(glob_dump_merci) == -1){
				printf("Errore shmdt - Processo porto\n");
			}
			if(shmdt(glob_dump_porti) == -1){
				printf("Errore shmdt - Processo porto\n");
			}
			if(shmdt(glob_dati_porti) == -1){
				printf("Errore shmdt - Processo porto\n");
			}
			if(shmdt(glob_dati_merci) == -1){
				printf("Errore shmdt - Processo porto\n");
			}
			if(shmdt(glob_dati_magazzino) == -1){
				printf("Errore shmdt - Processo porto\n");
			}
			if(shmdt(glob_porti_merci) == -1){
				printf("Errore shmdt - Processo porto\n");
			}
			if(shmctl(glob_dati_magazzino_ID, IPC_RMID, NULL) == -1){
				printf("Errore shmctl - Processo porto\n");
			}
			if(semctl(glob_risorsa_sem_ID, 0, IPC_RMID) == -1){
				printf("Errore semctl term - Processo porto\n");
			}
			if(msgctl(glob_porto_msg_ID, IPC_RMID, NULL) == -1){
				printf("Errore remove msg - Processo porto\n");
			}
			initsops.sem_num = 0;
			initsops.sem_op = -1;
			initsops.sem_flg = 0;
			if(semop(glob_init_sem_ID, &initsops, 1) == -1){
				printf("Errore nel decrementare il semaforo - Porto\n");
				exit(1);
			}
			exit(0);
	}
	return;
}


















