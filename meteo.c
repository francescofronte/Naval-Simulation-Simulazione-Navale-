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

static int usr1_occurred = 0;
static int term_occurred = 0;
static void userSignalHandler(int sig);
static void termSignalHandler(int sig);

static struct shmem_parametri* glob_init_shmem;
static int* glob_pid_navi;
static double* glob_dati_porti;
static int glob_sem_ID;

int main (int argc, char* argv[]){
	int init_sem_ID;
	struct sembuf initsops;
	int init_shmem_ID;
	struct shmem_parametri *init_shmem = NULL; 
	struct sigaction userHandlerPointer;
	struct sigaction termHandlerPointer;
	int i_shuffle;
	int i_random;
	int tmp;
	int i = 0;
	pid_t nave = 0;
	pid_t navekill;
	pid_t porto = 0;
	int porto_i;
	int nave_i;
	
	int* pid_navi; /*0 pid 1 mare/porto 2 vuota/piena*/
	double* dati_porti; /*0 pid, 1 x, 2 y, 3 msgid, 4 shmid, 5 semid 6 swell, 7 swell_ongoing*/
	
	clock_t before;
	clock_t elapsed;
	long int hours;
	
	
	
	/*Inizializzo srand*/
	srand(getpid());
	/*Assegnamento handler*/
	userHandlerPointer.sa_handler = userSignalHandler;
	termHandlerPointer.sa_handler = termSignalHandler;
	/*Installazione nuovi handler di segnali*/
	if(sigaction(SIGUSR1, &userHandlerPointer, NULL) == -1){
		printf("Errore installazione handler di segnali user - Processo meteo\n");
		exit(1);
	}
	
	if(sigaction(SIGTERM, &termHandlerPointer, NULL) == -1){
		printf("Errore installazione handler di segnali term - Processo meteo\n");
		exit(1);
	}
	
	/*Attacco memorie condivise*/
	init_shmem_ID = atoi(argv[0]);
	if ((init_shmem = (struct shmem_parametri*) shmat(init_shmem_ID, NULL, 0)) < (struct shmem_parametri*) 0){
		printf("Errore nell'attacco a memoria condivisa - Processo meteo\n");
		exit(1);
	}
	glob_init_shmem = init_shmem;
	init_shmem->rallentate = 0;
	
	if ((pid_navi = (int*) shmat(atoi(argv[4]), NULL, 0)) < (int*) 0){
		printf("Errore nell'attacco a memoria condivisa navi - Processo meteo\n");
		exit(1);
	}
	glob_pid_navi = pid_navi;
	
	if ((dati_porti = (double*) shmat(atoi(argv[3]), NULL, 0)) < (double*) 0){
		printf("Errore nell'attacco a memoria condivisa porti - Processo meteo\n");
		exit(1);
	}
	glob_dati_porti = dati_porti;
	
	/*Shuffle array pid navi per estrazione randomica*/
	for(i_shuffle = 0; i_shuffle < init_shmem->SO_NAVI; i_shuffle++){
		tmp = pid_navi[3*i];
		i_random = (rand()%init_shmem->SO_NAVI)*3;
		pid_navi[i*3] = pid_navi[i_random];
		pid_navi[i_random] = tmp;
	}
	
	/*Attendo segnale di via libera per iniziare la simulazione*/
	/*Opero sul semaforo passato dal master*/
	/*Decremento il valore del semaforo*/
	init_sem_ID = atoi(argv[1]);
	glob_sem_ID = init_sem_ID;
	initsops.sem_num = 0;
	initsops.sem_op = -1;
	initsops.sem_flg = 0;
	if(semop(init_sem_ID, &initsops, 1) == -1){
		printf("Errore nel decrementare il semaforo - Meteo\n");
		exit(1);
	}
	/*Attendo che il valore del semaforo sia zero. In tal caso, tutti i processi sono pronti e la simulazione può avere inizio*/
	initsops.sem_op = 0;
	if(semop(init_sem_ID, &initsops, 1) == -1){
		printf("Errore nell'attesa del semaforo - Meteo\n");
		exit(1);
	}
	
	before = clock();
	
	for(;;){
		/*Mareggiata e tempesta - una volta al giorno*/
		if(usr1_occurred == 1){
			/*Estrazione casuale pid nave e porto*/
			/*Mi assicuro che la nave non sia affondata e non sia in porto*/
			do{
				nave_i = rand()%(init_shmem->SO_NAVI);
				nave = pid_navi[nave_i*3 + 0];
				
			}while(pid_navi[nave_i*3 + 1] == 1 || pid_navi[nave_i*3] == -1);
			
			porto_i = rand()%(init_shmem->SO_PORTI);
			porto = (int)dati_porti[porto_i*8 + 0];
			
			dati_porti[porto_i*8 + 6] += 1; 
			dati_porti[porto_i*8 + 7] = 1;
			init_shmem->rallentate++;
			
			/*Invio segnale a nave e porto scelti*/
			if(kill(nave, SIGUSR2) == -1){
				printf("Errore nell'invio segnale storm a nave\n");
			}
			if(kill(porto, SIGUSR2) == -1){
				printf("Errore nell'invio segnale swell a porto\n");
			}
			usr1_occurred = 0;
			
		}
		/*Affondo una nave ogni volta che sono passate almeno SO_MAELSTROM ore*/
		elapsed = clock()-before;
		hours = (long int)(((double)elapsed/CLOCKS_PER_SEC)*24);
		if(hours >= init_shmem->SO_MAELSTROM){
			navekill = pid_navi[i*3 + 0];
			pid_navi[i*3 + 0] = -1;
			i++;
			if(kill(navekill, SIGTERM) == -1){
				printf("Errore nell'invio segnale term a nave\n");
			}
			if(i == init_shmem->SO_NAVI){
				printf("[METEO] Le navi sono state tutte abbattute\n");
				raise(SIGTERM);
			}
			before = clock();
		}
	}

}


/*Handler di segnale SIGUSR1*/
void userSignalHandler(int sig){
	if (sig == SIGUSR1){
		usr1_occurred = 1;
	}
	return;
}

void termSignalHandler(int sig){
	struct sembuf initsops;
	if (sig == SIGTERM){
		term_occurred = 1;
		/*Termina simulazione con successo*/
			if(shmdt(glob_init_shmem) == -1){
				printf("Errore shmdt - Processo meteo\n");
			}
			if(shmdt(glob_pid_navi) == -1){
				printf("Errore shmdt - Processo meteo\n");
			}
			if(shmdt(glob_dati_porti) == -1){
				printf("Errore shmdt - Processo meteo\n");
			}
			
			initsops.sem_num = 0;
			initsops.sem_op = -1;
			initsops.sem_flg = 0;
			if(semop(glob_sem_ID, &initsops, 1) == -1){
				printf("Errore nel decrementare il semaforo - Meteo\n");
			}
			exit(0);
	}
	return;
}
























