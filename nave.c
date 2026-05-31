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
#include <math.h>
#include "myheader.h"

int caricoScarico(int load, int capacity, double threshold);
long int findPorto(double* posx, double* posy, struct shmem_parametri* shmem, int* porto_shmem_ID, int* porto_msg_ID, int* porto_sem_ID, double* dati_porti, int* dati_magazzino_ID);
void loadUnload_new(int porto_shmem_ID, int porto_msg_ID, int porto_sem_ID, int dati_magazzino_ID, int *flag_stato, int* merce_onboard, struct merce** merci, int carico_scarico, struct shmem_parametri* init_shmem, int *dump_merci, int merci_sem_ID, int* pid_navi);
void merceScaduta(struct merce** merci, int days, int num_merci, int* scaduta, int* merce_onboard, int* dump_merci, int semid_merci);
void pausaTrasferimento(long int merce_trasferita, long int loadspeed);

static void userSignalHandler(int sig);
static int usr1_occurred = 0;
static void termSignalHandler(int sig);
static int term_occurred = 0;
static void stormSignalHandler(int sig);
static long int storm;

static struct shmem_parametri *glob_init_shmem;
static struct merce ** glob_merci;
static int* glob_dump_merci;
static int* glob_pid_navi;
static double *glob_dati_porti;
static int glob_init_sem_ID;
static int* glob_dati_magazzino = 0;
static int glob_porto_sem_ID = 0;


int main (int argc, char** argv){
	int init_shmem_ID;
	int init_sem_ID;
	struct sembuf initsops;
	struct shmem_parametri *init_shmem = NULL; 
	struct msg_nave msg;
	
	double posx;
	double posy;
	pid_t nave_id;
	int flag_stato = 0; /*0 = in mare, 1 = in porto*/
	int merce_onboard = 0;
	struct merce** merci = NULL;
	int merce_scaduta = 0;
	int carico_scarico; /*0 = carico; 1 = scarico*/
	long int sleeptime;
	struct timespec tim1, tim2;
	int days = 0;
	
	int merci_i;
	
	int term_i;
	struct merce* p;
	int qt;
	
	int porto_shmem_ID = 0;
	int porto_msg_ID;
	int porto_sem_ID;
	
	int* dump_merci; /*0 in porto 1 in nave 2 consegnata 3 scadporto 4 scadnave*/
	int merci_shmem_ID; 
	int merci_sem_ID;
	
	int* pid_navi; /*0 pid 1 mare/porto 2 vuota/piena*/
	double* dati_porti; /*0 pid, 1 x, 2 y, 3 msgid, 4 shmid, 5 semid 6 swell, 7 swell_ongoing*/
	int dati_magazzino_ID;
	
	struct sigaction userHandlerPointer;
	struct sigaction stormHandlerPointer;
	struct sigaction termHandlerPointer;
	sigset_t blockmask;
	sigset_t oldmask;
			

	userHandlerPointer.sa_handler = userSignalHandler;
	termHandlerPointer.sa_handler = termSignalHandler;
	stormHandlerPointer.sa_handler = stormSignalHandler;
	
	
	/*Inizializzo srand per generazione numeri casuali*/
	srand(getpid());
	
	
	/*Attacco a memoria condivisa*/
	init_shmem_ID = atoi(argv[0]);
	if ((init_shmem = (struct shmem_parametri*) shmat(init_shmem_ID, NULL, SHM_RDONLY)) < (struct shmem_parametri*) 0){
		printf("Errore nell'attacco a memoria condivisa - Processo nave\n");
		exit(1);
	}
	glob_init_shmem = init_shmem;
	storm = (((long int)init_shmem->SO_STORM_DURATION)*1000000000)/24;
	
	/*Attacco memoria condivisa per il dump delle merci ecc ecc*/
	merci_shmem_ID = atoi(argv[3]);
	merci_sem_ID = atoi(argv[4]);
	if ((dump_merci = (int*) shmat(merci_shmem_ID, NULL, 0)) < (int*) 0){
		printf("Errore nell'attacco a memoria condivisa merci - Processo nave\n");
		exit(1);
	}
	glob_dump_merci = dump_merci;
	if ((pid_navi = (int*) shmat(atoi(argv[6]), NULL, 0)) < (int*) 0){
		printf("Errore nell'attacco a memoria condivisa navi - Processo nave\n");
		exit(1);
	}
	glob_pid_navi = pid_navi;
	if ((dati_porti = (double*) shmat(atoi(argv[5]), NULL, 0)) < (double*) 0){
		printf("Errore nell'attacco a memoria condivisa porti - Processo nave\n");
		exit(1);
	}
	glob_dati_porti = dati_porti;
	
	/*Inizializzo posizione a caso*/
	posx = (double)(rand()%(int)(init_shmem->SO_LATO + 1));
	posy = (double)(rand()%(int)(init_shmem->SO_LATO + 1));
	
	nave_id = getpid();
	
	/*Alloco array per stoccaggio merci a bordo*/
	merci = (struct merce**)calloc(init_shmem->SO_MERCI, sizeof(struct merce*));
	for(merci_i = 0; merci_i < init_shmem->SO_MERCI; merci_i++){
		merci[merci_i] = NULL;
	}
	glob_merci = merci;
	/*Installazione nuovi handler di segnali*/	

	if(sigaction(SIGUSR1, &userHandlerPointer, NULL) == -1){
		printf("Errore installazione handler di segnali user - Processo nave\n");
		exit(1);
	}
	if(sigaction(SIGUSR2, &stormHandlerPointer, NULL) == -1){
		printf("Errore installazione handler di segnali storm - Processo nave\n");
		exit(1);
	}
	if(sigaction(SIGTERM, &termHandlerPointer, NULL) == -1){
		printf("Errore installazione handler di segnali term - Processo nave\n");
		exit(1);
	}
	
	/*Attendo segnale di via libera per iniziare la simulazione*/
	/*Opero sul semaforo passato dal master*/
	/*Decremento il valore del semaforo*/
	init_sem_ID = atoi(argv[1]);
	glob_init_sem_ID = init_sem_ID;
	
	initsops.sem_num = 0;
	initsops.sem_op = -1;
	initsops.sem_flg = 0;
	if(semop(init_sem_ID, &initsops, 1) == -1){
		printf("Errore nel decrementare il semaforo - Nave\n");
		exit(1);
	}
	
	/*Attendo che il valore del semaforo sia zero. In tal caso, tutti i processi sono pronti e la simulazione può avere inizio*/
	initsops.sem_op = 0;
	if(semop(init_sem_ID, &initsops, 1) == -1){
		printf("Errore nell'attesa del semaforo - Nave\n");
		exit(1);
	}
	
	
	for(;;){
		/*Blocco per movimento e scambio merci*/
		if(!term_occurred){
			carico_scarico = caricoScarico(merce_onboard, init_shmem->SO_CAPACITY, init_shmem->SO_THRESHOLD);
		}
		if(!term_occurred){
			sleeptime = findPorto(&posx, &posy, init_shmem, &porto_shmem_ID, &porto_msg_ID, &porto_sem_ID, dati_porti, &dati_magazzino_ID);
		}
		if(!term_occurred){
			tim1.tv_sec = 0;
			while(sleeptime >= 1000000000){
				sleeptime -= 1000000000;
				tim1.tv_sec++;
			}
			tim1.tv_nsec = sleeptime;
		}
		if(!term_occurred){
			sigfillset(&blockmask);
			sigprocmask(SIG_BLOCK, &blockmask, &oldmask);
			if (nanosleep(&tim1, &tim2) == -1){
				printf("Errore nanosleep findporto - nave\n");
			}
			sigprocmask(SIG_SETMASK, &oldmask, NULL);
		}	
		if(!term_occurred){
			
			loadUnload_new(porto_shmem_ID, porto_msg_ID, porto_sem_ID, dati_magazzino_ID, &flag_stato, &merce_onboard, merci, carico_scarico, init_shmem, dump_merci, merci_sem_ID, pid_navi);
			
		}
		
		/*Deallocazione merci giornaliera*/
		if(usr1_occurred == 1){
			sigfillset(&blockmask);
			sigdelset(&blockmask, SIGTERM);
			sigprocmask(SIG_BLOCK, &blockmask, &oldmask);
			
			days = init_shmem->days;
			merceScaduta(merci, days, init_shmem->SO_MERCI, &merce_scaduta, &merce_onboard, dump_merci, merci_sem_ID); 
			usr1_occurred = 0;
			
			sigprocmask(SIG_SETMASK, &oldmask, NULL);
		}
	}
	
}

/*Carica a bordo se la nave è piena per meno di SO_THRESHOLD, scarica in porto se è carica per oltre il 50%*/
int caricoScarico(int load, int capacity, double threshold){
	int ret;
	if(((double)load/(double)capacity) < threshold){
		ret = 0;
	} else {
		ret = 1;
	}
	return ret;
}

/*Funzione che trova un porto random, ne trova coordinate, id memoria condivisa e semaforo, e restituisce
* il tempo necessario per raggiungerlo*/
long int findPorto(double* posx, double* posy, struct shmem_parametri* shmem, int* porto_shmem_ID, int* porto_msg_ID, int* porto_sem_ID, double* dati_porti, int* dati_magazzino_ID){
	double tempx;
	double tempy;
	int i;
	double dist;
	long int time_ret;
	
	/*Scelgo randomicamente il porto */
	do{
		i = (rand()%(shmem->SO_PORTI));
	}while((int)dati_porti[i*8 + 4] == *porto_shmem_ID && dati_porti[i*8 + 7] == 1);
	
	tempx = dati_porti[i*8 + 1];
	tempy = dati_porti[i*8 + 2];
	dist = sqrt(pow(tempx - *posx, 2)+pow(tempy - *posy, 2));
	*posx = tempx;
	*posy = tempy;
	*porto_shmem_ID = (int)dati_porti[i*8 + 4];
	*porto_msg_ID = (int)dati_porti[i*8 + 3];
	*porto_sem_ID = (int)dati_porti[i*8 + 5];
	*dati_magazzino_ID = dati_porti[i*8 + 4];
	/*Calcola time*/
	/*t = ds/dv*/
	dist = dist*1000000000;
	time_ret = (long int)dist/(shmem->SO_SPEED);
	
	return time_ret;
	
}

/*Funzione che gestisce il carico e lo scarico delle merci in un porto.*/
void loadUnload_new(int porto_shmem_ID, int porto_msg_ID, int porto_sem_ID, int dati_magazzino_ID, int *flag_stato, int* merce_onboard, struct merce** merci, int carico_scarico, struct shmem_parametri* init_shmem, int *dump_merci, int merci_sem_ID, int* pid_navi){
	struct sembuf semops;
	struct msg_np msg_invio;
	struct msg_pn msg_ricevo;
	int i_sgrognax = 1;
	struct merce* new_lotto;
	struct merce* p;
	int n_i = 0;
	int found = 0;
	int merce_trasferita;
	int i;
	int* dati_magazzino;
	
	sigset_t blockmask;
	sigset_t oldmask;
	
	sigfillset(&blockmask);
	sigdelset(&blockmask, SIGTERM);
	sigprocmask(SIG_BLOCK, &blockmask, &oldmask);
	
	/*Check sulle banchine del porto mediante semaforo*/
	semops.sem_num = 0;
	semops.sem_op = -1;
	semops.sem_flg = 0;
	if(semop(porto_sem_ID, &semops, 1) == -1){
		printf("Errore nel decrementare il semaforo banchine - Processo nave\n");
		exit(1);
	}
	glob_porto_sem_ID = porto_sem_ID;
	/*Attacco memoria condivisa per i dati delle merci in porto*/
	glob_dati_magazzino = dati_magazzino = (int*)shmat(dati_magazzino_ID, NULL, 0);
	
	/*Cerco la entry con il PID della nave nell'array pid_navi e setto il campo 1 come "in porto"*/
	while(n_i < init_shmem->SO_NAVI && found == 0){
		if(pid_navi[n_i*3 + 0] == getpid()){
			pid_navi[n_i*3 + 1] = 1;
			*flag_stato = 1;
			found = 1;
		} else {
			n_i++;
		}
	}
	
	if(carico_scarico){ /*Scarico*/
		/*Scarico ogni merce di cui il porto ha richiesta, fino ad esaurimento richiesta o disponibilità a bordo*/
		for(i = 0; i < init_shmem->SO_MERCI; i++){
			if(merci[i] != NULL){
				if(dati_magazzino[i] >= merci[i]->quantita){
					merce_trasferita = 0;
					/*Check sul semaforo*/
					if(reserveSem(merci_sem_ID, i) == -1){
						printf("Errore nel decrementare il semaforo dumpmerce - Processo nave\n");
						exit(1);
					}
					/*Compio op di scarico*/
					p = merci[i];
					while(p != NULL && p->quantita <= dati_magazzino[i]){
						dati_magazzino[i] -= p->quantita;
						dump_merci[i*5 + 2] += p->quantita;
						dump_merci[i*5 + 1] -= p->quantita;
						merce_trasferita += p->quantita;
						*merce_onboard -= p->quantita;
						merci[i] = p->next;
						
						msg_invio.mtype = 1;
						msg_invio.caricoScarico = carico_scarico;
						msg_invio.pid_n = getpid();
						msg_invio.ton = 0;
						msg_invio.tipo = p->tipo;
						msg_invio.scadenza = p->scadenza; 
						msg_invio.quantita = p->quantita;
				
						if(msgsnd(porto_msg_ID, &msg_invio, sizeof(struct msg_np)-sizeof(long), 1) == -1){
							printf("Errore msgsnd - processo nave\n");
							exit(1);
						}
						
						free(p);
						p = merci[i];
					}
			
					pausaTrasferimento(merce_trasferita, init_shmem->SO_LOADSPEED);
					
					if(releaseSem(merci_sem_ID, i) == -1){
						printf("Errore nel rilasciare il semaforo dumpmerce - Processo nave\n");
						exit(1);
					}
				}
			}
		}
		
	
	} else { /*Carico*/
		msg_invio.mtype = 1; /*1*/
		msg_invio.caricoScarico = carico_scarico; /* 0/1 */
		msg_invio.pid_n = getpid();
		msg_invio.ton = (init_shmem->SO_CAPACITY) - *merce_onboard;
		msg_invio.tipo = 0;
		msg_invio.quantita = 0;
		msg_invio.scadenza = 0;
		
		if(msgsnd(porto_msg_ID, &msg_invio, sizeof(struct msg_np)-sizeof(long), 1) == -1){
			printf("Errore msgsnd - processo nave\n");
			exit(1);
		}
		
		while(i_sgrognax ==  1 && !term_occurred){
			if(msgrcv(porto_msg_ID, &msg_ricevo, sizeof(struct msg_pn)-sizeof(long), getpid(), 0) == -1){
				printf("Errore msgrcv - processo nave\n");
				i_sgrognax = 0;
				
			}
			
			if(msg_ricevo.tipo == -1 && !term_occurred){ /*Ho finito*/
				i_sgrognax = 0;
			} else {
				new_lotto = (struct merce*)malloc(sizeof(struct merce));
				new_lotto->quantita = msg_ricevo.quantita;
				new_lotto->scadenza = msg_ricevo.scadenza;
				new_lotto->tipo = msg_ricevo.tipo;
				new_lotto->next = merci[msg_ricevo.tipo];
				merci[msg_ricevo.tipo] = new_lotto;
				*merce_onboard += msg_ricevo.quantita;	
				
				pausaTrasferimento(new_lotto->quantita, init_shmem->SO_LOADSPEED);
			}
			
		}
	}
	
	shmdt(dati_magazzino);
	glob_dati_magazzino = 0;
	
	/*Incremento il semaforo delle banchine disponibili*/
	semops.sem_num = 0;
	semops.sem_op = 1;
	semops.sem_flg = 0;
	if(semop(porto_sem_ID, &semops, 1) == -1){
		printf("Errore nel'incrementare il semaforo banchine - Processo nave\n");		
		exit(1);
	}
	glob_porto_sem_ID = 0;
	
	/*Nell'array pid_navi setto il campo 1 come "in mare" e assegno opportunamente il campo 2 (nave vuota o carica)*/
	pid_navi[n_i*3 + 1] = 0;
	pid_navi[n_i*3 + 2] = (*merce_onboard > 0);
	*flag_stato = 0;
	
	sigprocmask(SIG_SETMASK, &oldmask, NULL);
	
	return;

}

void pausaTrasferimento(long int merce_trasferita, long int loadspeed){
	struct timespec tim1, tim2;
	long int sleeptime;
	sigset_t blockmask;
	sigset_t oldmask;
	
	sigfillset(&blockmask);
	sigdelset(&blockmask, SIGTERM);
	sigprocmask(SIG_BLOCK, &blockmask, &oldmask);
	/*time = q/v*/
	/*Tempo in nsec per maggiore precisione*/
	sleeptime = ((merce_trasferita)*1000000000)/loadspeed;
	tim1.tv_sec = 0;
	while(sleeptime >= 1000000000){
		sleeptime -= 1000000000;
		tim1.tv_sec++;
	}
	tim1.tv_nsec = sleeptime;
	if(nanosleep(&tim1, &tim2) == -1){
		if(errno == EINTR){
			return;
		}
		printf("Errore nanosleep pausa trasferimento - Processo nave\n");
	}
	sigprocmask(SIG_SETMASK, &oldmask, NULL);
	return;
}

void merceScaduta(struct merce** merci, int days, int num_merci, int* scaduta, int* merce_onboard, int* dump_merci, int semid_merci){
	int i;
	struct merce *p, *q;
	sigset_t blockmask;
	sigset_t oldmask;
	
	sigfillset(&blockmask);
	sigdelset(&blockmask, SIGTERM);
	sigprocmask(SIG_BLOCK, &blockmask, &oldmask);
	
	for(i=0;i<num_merci && !term_occurred;i++){
		if(reserveSem(semid_merci, i) == -1){
			if(errno == EINTR){
				return;
			}
			printf("Errore nella reserve semaforo dump dealloc - merce scad nave\n");
		} else {
			p = merci[i];
			while(p!=NULL && !term_occurred){
				if(p == merci[i] && p->scadenza <= days && !term_occurred){
					merci[i] = p->next;
					*scaduta += p->quantita;
					*merce_onboard -= p->quantita;
					dump_merci[i*5 + 4] += p->quantita;
					dump_merci[i*5 + 1] -= p->quantita;
					free(p);
					p = merci[i];
				} else if (p->scadenza > days && p->next != NULL && p->next->scadenza <= days && !term_occurred){
					q = p->next;
					p->next = q->next;
					*scaduta += q->quantita;
					*merce_onboard -= q->quantita;
					dump_merci[i*5 + 4] += p->quantita;
					dump_merci[i*5 + 1] -= p->quantita;
					free(q);
				} else {
					p = p->next;
				}
			}
			if(releaseSem(semid_merci, i) == -1){
				printf("Errore nella release semaforo dump dealloc - merce scad nave\n");
			}
		}
	}
	
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

void stormSignalHandler(int sig){
	struct timespec tim1, tim2;
	sigset_t blockmask;
	sigset_t oldmask;
	long int sleeptime = storm;
	
	sigfillset(&blockmask);
	sigdelset(&blockmask, SIGTERM);
	
	tim1.tv_sec = 0;
	
	while(sleeptime >= 1000000000){
		sleeptime -= 1000000000;
		tim1.tv_sec++;
	}
	tim1.tv_nsec = sleeptime;
	
	sigprocmask(SIG_BLOCK, &blockmask, &oldmask);
	if (sig == SIGUSR2){
		nanosleep(&tim1, &tim2);
	}
	sigprocmask(SIG_SETMASK, &oldmask, NULL);
	return;
}

void termSignalHandler(int sig){
	struct sembuf initsops;
	int term_i;
	struct merce* p;
	int qt;
	sigset_t blockmask, oldmask;
	if (sig == SIGTERM){
		term_occurred = 1;
		sigfillset(&blockmask);
		sigprocmask(SIG_BLOCK, &blockmask, &oldmask);
		for(term_i = 0; term_i < glob_init_shmem->SO_MERCI; term_i++){
			qt = 0;
			p = glob_merci[term_i];
			while(p != NULL){
				qt += p->quantita;
				p = p->next;
			}
			glob_dump_merci[term_i*5 + 1] -= qt;
		}
		if(shmdt(glob_init_shmem) == -1){
			printf("Errore shmdt - Processo nave\n");
		}
		if(shmdt(glob_dump_merci) == -1){
			printf("Errore shmdt - Processo nave\n");
		}
		if(shmdt(glob_pid_navi) == -1){
			printf("Errore shmdt - Processo nave\n");
		}
		if(shmdt(glob_dati_porti) == -1){
			printf("Errore shmdt - Processo nave\n");
		}
		if(glob_dati_magazzino != 0){
			if(shmdt(glob_dati_magazzino) == -1){
				printf("Errore shmdt - Processo nave\n");
			}
		}
		initsops.sem_num = 0;
		initsops.sem_op = 1;
		initsops.sem_flg = 0;
		if(glob_porto_sem_ID != 0){
			if(semop(glob_porto_sem_ID, &initsops, 1) == -1){
				printf("Errore nel decrementare il semaforo - Nave\n");
			}	
		}
		initsops.sem_num = 0;
		initsops.sem_op = -1;
		initsops.sem_flg = 0;
		if(semop(glob_init_sem_ID, &initsops, 1) == -1){
			printf("Errore nel decrementare il semaforo - Nave\n");
			exit(1);
		}
		exit(0);	
	}
	return;
}






























