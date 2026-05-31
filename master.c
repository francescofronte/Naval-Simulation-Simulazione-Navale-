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

int main(void){
	FILE* fp = NULL;
	struct shmem_parametri* init_shmem = NULL;
	int init_shmem_ID;
	int SO_MINVITA;
	int SO_MAXVITA;
	int SO_SIZE;
	char trash_buffer[10];
	int i_init;
	int* dump_merci;
	int dump_merci_ID;
	int* dump_porti; /*pid presente spedita ricevuta tot occup*/
	int dump_porti_ID;
	double* dati_porti; /*0 pid, 1 x, 2 y, 3 msgid, 4 shmid, 5 semid 6 swell, 7 swell_ongoing*/
	int dati_porti_ID;
	int* dati_merci; /*0 qta, 1 scad*/
	int dati_merci_ID;
	int* pid_navi; /*0 pid 1 mare/porto 2 vuota/piena*/
	int pid_navi_ID;
	int* porti_merci; /*porti x merci, quantità prodotta*/
	int porti_merci_ID;
	int init_sem_ID;
	union semun init_sem_arg;
	struct sembuf initsops;
	int merci_sem_ID;
	union semun merci_sem_arg;
	int msg_ID = 0;
	
	int max = 0;
	int winner;
	
	int fork_i;
	pid_t p;
	pid_t pid_meteo;
	char** argVec_p =  NULL;
	char** argVec_n =  NULL;
	char** argVec_m =  NULL;
	char init_buffer[10];
	char fork_buffer[10];
	char insem_buffer[10];
	char msg_buffer[10];
	char merci_buffer[10];
	char mersem_buffer[10];
	char porti_buffer[10];
	char porti_dump_buffer[10];
	char datimerci_buffer[10];
	char navi_buffer[10];
	
	int days = 0;
	int prosegui = 1;
	int i_porti;
	int le_navi;
	int le_merci;
	pid_t nave_iter;
	
	int conta_cariche;
	int conta_scariche;
	int conta_porto;
	int conta_affondate;
	
	int contafill = 0;
	
	pid_t p_term;
	
	
	printf("---PREDISPOSIZIONE INIZIALIZZAZIONE---\n");
	/*Inizializzo srand*/
	srand(time(NULL));
	
	/*Creo memoria condivisa per inizializzazione parametri*/
	
	if((init_shmem_ID = shmget(IPC_PRIVATE, sizeof(struct shmem_parametri), 0666|IPC_CREAT)) == -1){
		printf("Errore allocazione memoria di inizializzazione - Master");
		exit(1);
	}
	if((init_shmem = (struct shmem_parametri*)shmat(init_shmem_ID, NULL, 0)) < (struct shmem_parametri*) 0){
		printf("Errore attacco memoria condivisa - Master");
		exit(1);
	}
	
	
	/*Leggo parametri di configurazione da file*/
	/*NB! E' un file di interi, uno per riga, che va NECESSARIAMENTE ordinato in questo modo:
	SO_BANCHINE
	SO_LATO
	SO_MERCI
	SO_FILL
	SO_DAYS
	SO_SPEED
	SO_CAPACITY
	SO_LOADSPEED
	SO_PORTI
	SO_NAVI
	SO_SWELL_DURATION
	SO_STORM_DURATION
	SO_MAELSTROM
	SO_MINVITA
	SO_MAXVITA
	SO_SIZE
	*/
	printf("---LETTURA PARAMETRI DI CONFIGURAZIONE---\n");
	
	if((fp = fopen("configurazione.txt", "r")) == NULL){
		printf("Errore fopen");
		exit(1);
	}
	
	if(fscanf(fp, "%s%d%s%lf%s%d%s%d%s%d%s%d%s%d%s%d%s%d%s%d%s%d%s%d%s%d%s%d%s%d%s%d%s%lf", trash_buffer,
											     &init_shmem->SO_BANCHINE, 
											     trash_buffer,
											     &init_shmem->SO_LATO,
											     trash_buffer,
											     &init_shmem->SO_MERCI,
											     trash_buffer,
											     &init_shmem->SO_FILL,
											     trash_buffer,
											     &init_shmem->SO_DAYS,
											     trash_buffer,
											     &init_shmem->SO_SPEED,
											     trash_buffer,
											     &init_shmem->SO_CAPACITY,
											     trash_buffer,
									 		     &init_shmem->SO_LOADSPEED,
									 		     trash_buffer,
									 		     &init_shmem->SO_PORTI,
									 		     trash_buffer,
											     &init_shmem->SO_NAVI,
											     trash_buffer,
											     &init_shmem->SO_SWELL_DURATION,
											     trash_buffer,
											     &init_shmem->SO_STORM_DURATION,
											     trash_buffer,
											     &init_shmem->SO_MAELSTROM,
											     trash_buffer,
											     &SO_MINVITA,
											     trash_buffer,
											     &SO_MAXVITA,
											     trash_buffer,
											     &SO_SIZE,
											     trash_buffer,
											     &init_shmem->SO_THRESHOLD) != 34){
		printf("Errore nella fscanf");
		exit(1);
	}
	
	init_shmem->days = days;
	
	fclose(fp);
	
	/*Se i parametri di input hanno valori non accettabili, vengono resettati a valori di default*/
	if(init_shmem->SO_BANCHINE < 1){
		init_shmem->SO_BANCHINE = 10;
	}
	if(init_shmem->SO_LATO < 1){
		init_shmem->SO_LATO = 1000;
	}
	if(init_shmem->SO_MERCI < 1){
		init_shmem->SO_MERCI = 5;
	}
	if(init_shmem->SO_FILL < 1){
		init_shmem->SO_FILL = 500000;
	}
	if(init_shmem->SO_DAYS < 1){
		init_shmem->SO_DAYS = 10;
	}
	if(init_shmem->SO_SPEED < 1){
		init_shmem->SO_SPEED = 500;
	}
	if(init_shmem->SO_CAPACITY < 1){
		init_shmem->SO_CAPACITY = 10;
	}
	if(init_shmem->SO_LOADSPEED < 1){
		init_shmem->SO_LOADSPEED = 200;
	}
	if(init_shmem->SO_PORTI < 4){
		init_shmem->SO_PORTI = 10;
	}
	if(init_shmem->SO_NAVI < 1){
		init_shmem->SO_NAVI = 100;
	}
	if(init_shmem->SO_SWELL_DURATION < 1){
		init_shmem->SO_SWELL_DURATION = 24;
	}
	if(init_shmem->SO_STORM_DURATION < 1){
		init_shmem->SO_STORM_DURATION = 6;
	}
	if(init_shmem->SO_MAELSTROM < 1){
		init_shmem->SO_MAELSTROM = 24;
	}
	if(SO_MINVITA < 1){
		SO_MINVITA = 5;
	}
	if(SO_MAXVITA < 1){
		SO_MAXVITA = 10;
	}
	if(SO_SIZE < 1){
		SO_SIZE = 10;
	}
	if(init_shmem->SO_THRESHOLD > 0.9 || init_shmem->SO_THRESHOLD < 0.1){
		init_shmem->SO_THRESHOLD = 0.5;
	}
	
	printf("---INIZIALIZZAZIONE STRUTTURE DI SUPPORTO CONDIVISE---\n");
	
	/*Creo memoria condivisa per dump merci*/
	/*Matrice mappata su array unidimensionale con giochi di offset*/
	if((dump_merci_ID = shmget(IPC_PRIVATE, sizeof(int)*(init_shmem->SO_MERCI)*(5), 0666|IPC_CREAT)) == -1){
		printf("Errore allocazione memoria di dump merce - Master");
		exit(1);
	}
	if((dump_merci = (int*)shmat(dump_merci_ID, NULL, 0)) == (int*)-1){
		printf("Errore attacco a memoria di dump merce - Master");
		exit(1);
	}
	for(i_init = 0; i_init<init_shmem->SO_MERCI; i_init++){
		dump_merci[i_init*5 + 0] = 0;
		dump_merci[i_init*5 + 1] = 0;
		dump_merci[i_init*5 + 2] = 0;
		dump_merci[i_init*5 + 3] = 0;
		dump_merci[i_init*5 + 4] = 0;
	}
	/*Creo e inizializzo semafori binari a protezione della risorsa dump_merci*/
	merci_sem_arg.array = (short unsigned int*)calloc(init_shmem->SO_MERCI, sizeof(short unsigned int));
	for(i_init = 0; i_init<init_shmem->SO_MERCI; i_init++){
		merci_sem_arg.array[i_init] = 1;
	}
	
	if((merci_sem_ID = semget(IPC_PRIVATE, init_shmem->SO_MERCI, 0666|IPC_CREAT)) == -1){
		printf("Errore allocazione semaforo dump merci");
		exit(1);
	}
	
	if(semctl(merci_sem_ID, 0, SETALL, merci_sem_arg) == -1){
		printf("Errore inizializzazione semaforo dump merci");
		exit(1);
	}
	
	/*Allocazione matrice dati merci*/
	/*Inizializzo randomicamente quantità e scadenza per ogni merce*/
	if((dati_merci_ID = shmget(IPC_PRIVATE, sizeof(int)*(init_shmem->SO_MERCI)*(2), 0666|IPC_CREAT)) == -1){
		printf("Errore allocazione memoria di dati merci - Master");
		exit(1);
	}
	if((dati_merci = (int*)shmat(dati_merci_ID, NULL, 0)) == (int*)-1){
		printf("Errore attacco a memoria di dati merci - Master");
		exit(1);
	}
	for(i_init = 0; i_init<init_shmem->SO_MERCI; i_init++){
		dati_merci[i_init*2 + 0] = (rand()%SO_SIZE)+1;
		dati_merci[i_init*2 + 1] = (rand()%(SO_MAXVITA - SO_MINVITA + 1)) + SO_MINVITA;
	}
	
	
	/*Allocazione matrice dati navi*/ /*0 pid 1 mare/porto 2 vuota/piena*/
	if((pid_navi_ID = shmget(IPC_PRIVATE, sizeof(int)*(init_shmem->SO_NAVI)*(3), 0666|IPC_CREAT)) == -1){
		printf("Errore allocazione memoria di pid navi - Master");
		exit(1);
	}
	if((pid_navi = (int*)shmat(pid_navi_ID, NULL, 0)) == (int*)-1){
		printf("Errore attacco a memoria di pid navi - Master");
		exit(1);
	}
	for(i_init = 0; i_init<init_shmem->SO_NAVI; i_init++){
		pid_navi[i_init*3 + 0] = 0;
		pid_navi[i_init*3 + 1] = 0;
		pid_navi[i_init*3 + 2] = 0;
	}
	/*Allocazione matrice dati porti*/
	if((dati_porti_ID = shmget(IPC_PRIVATE, sizeof(double)*(init_shmem->SO_PORTI)*(8), 0666|IPC_CREAT)) == -1){
		printf("Errore allocazione memoria di dati porti - Master");
		exit(1);
	}
	if((dati_porti = (double*)shmat(dati_porti_ID, NULL, 0)) == (double*)-1){
		printf("Errore attacco a memoria di dati porti - Master");
		exit(1);
	}
	for(i_init = 0; i_init<init_shmem->SO_PORTI; i_init++){
		dati_porti[i_init*8 + 0] = 0;
		dati_porti[i_init*8 + 1] = 0;
		dati_porti[i_init*8 + 2] = 0;
		dati_porti[i_init*8 + 3] = 0;
		dati_porti[i_init*8 + 4] = 0;
		dati_porti[i_init*8 + 5] = 0;
		dati_porti[i_init*8 + 6] = 0;
		dati_porti[i_init*8 + 7] = 0;
	}
	
	/*Creo memoria condivisa per dump porti*/ /*pid presente spedita ricevuta tot occup*/
	if((dump_porti_ID = shmget(IPC_PRIVATE, sizeof(int)*(init_shmem->SO_PORTI)*(6), 0666|IPC_CREAT)) == -1){
		printf("Errore allocazione memoria di dump porti - Master");
		exit(1);
	}
	if((dump_porti = (int*)shmat(dump_porti_ID, NULL, 0)) == (int*)-1){
		printf("Errore attacco a memoria di dump porti - Master");
		exit(1);
	}
	for(i_init = 0; i_init<init_shmem->SO_PORTI; i_init++){
		dump_porti[i_init*6 + 0] = 0;
		dump_porti[i_init*6 + 1] = 0;
		dump_porti[i_init*6 + 2] = 0;
		dump_porti[i_init*6 + 3] = 0;
		dump_porti[i_init*6 + 4] = 0;
		dump_porti[i_init*6 + 5] = 0;
	}
	
	/*Creo memoria condivisa per merce prodotta dai porti - porti "vincitori"*/
	if((porti_merci_ID = shmget(IPC_PRIVATE, sizeof(int)*(init_shmem->SO_PORTI)*(init_shmem->SO_MERCI), 0666|IPC_CREAT)) == -1){
		printf("Errore allocazione memoria di dump merci x porti - Master");
		exit(1);
	}
	if((porti_merci = (int*)shmat(porti_merci_ID, NULL, 0)) == (int*)-1){
		printf("Errore attacco a memoria di dump merci x porti - Master");
		exit(1);
	}
	for(i_init = 0; i_init<init_shmem->SO_PORTI; i_init++){
		for(le_merci = 0; le_merci<init_shmem->SO_MERCI; le_merci++){
			porti_merci[i_init*init_shmem->SO_MERCI + le_merci] = 0;
		}
	}
	
	
	/*Allocazione semaforo di inizializzazione, inizializzato a numero di porti + numero di navi + 2*/
	/*Attende che tutti i processi siano pronti prima di dare il via alla simulazione*/
	init_sem_arg.val = init_shmem -> SO_NAVI + init_shmem -> SO_PORTI + 2;
	
	if((init_sem_ID = semget(IPC_PRIVATE, 1, 0666|IPC_CREAT)) == -1){
		printf("Errore allocazione semaforo inizializzazione");
		exit(1);
	}
	
	if(semctl(init_sem_ID, 0, SETVAL, init_sem_arg) == -1){
		printf("Errore inizializzazione semaforo inizializzazione");
		exit(1);
	}
	
	/*Creazione coda di messaggi per report giornalieri
	if((msg_ID = msgget(IPC_PRIVATE, 0666)) == -1){
		printf("Errore creazione coda di messaggi");
		exit(1);
	}*/
	
	printf("---INIZIALIZZAZIONE COMPLETATA---\n");
	
	printf("---FORK PORTI---\n");
	
	/*Fork dei processi porto*/
	for(fork_i = 0; fork_i < init_shmem->SO_PORTI; fork_i++){
		p = fork();
		
		if (p == 0) {
			/*Creo array argv*/
			argVec_p = (char**)calloc(9, sizeof(char*));

			sprintf(init_buffer, "%d", init_shmem_ID);
			argVec_p[0] = init_buffer;
			
			sprintf(fork_buffer, "%d", fork_i);
			argVec_p[1] = fork_buffer;
			
			sprintf(insem_buffer, "%d", init_sem_ID);
			argVec_p[2] = insem_buffer;
			
			sprintf(porti_dump_buffer, "%d", dump_porti_ID);
			argVec_p[3] = porti_dump_buffer;
			
			sprintf(merci_buffer, "%d", dump_merci_ID);
			argVec_p[4] = merci_buffer;
			
			sprintf(mersem_buffer, "%d", merci_sem_ID);
			argVec_p[5] = mersem_buffer;
			
			sprintf(porti_buffer, "%d", dati_porti_ID);
			argVec_p[6] = porti_buffer;
			
			sprintf(datimerci_buffer, "%d", dati_merci_ID);
			argVec_p[7] = datimerci_buffer;
			
			sprintf(msg_buffer, "%d", porti_merci_ID);
			argVec_p[8] = msg_buffer;
			
			argVec_p[9] = NULL;
			
			/*Mando in esecuzione il processo porto*/
			execv("porto", argVec_p);
			printf("Sono utile! :) \n");
			exit(1);
		} else if(p == -1){
			printf("Errore fork porto %d\n", fork_i);
			exit(1);
		} else {
			/*printf("Ho forkato! Id figlio: %d\n", p);
			sleep(1);*/
		}
		
	}
	printf("Caricamento... \n");
	sleep(1);
	printf("Porti pronti!\n");
	sleep(1);
	
	printf("---FORK NAVI---\n");
	/*Fork dei processi nave*/
	for(fork_i = 0; fork_i < init_shmem->SO_NAVI; fork_i++){
		p = fork();
		
		if (p == 0) {
			/*Creo array argv*/
			argVec_n = (char**)calloc(9, sizeof(char*));
			
			sprintf(init_buffer, "%d", init_shmem_ID);
			argVec_n[0] = init_buffer;
			
			sprintf(insem_buffer, "%d", init_sem_ID);
			argVec_n[1] = insem_buffer;
			
			sprintf(msg_buffer, "%d", msg_ID);
			argVec_n[2] = msg_buffer;
			
			sprintf(merci_buffer, "%d", dump_merci_ID);
			argVec_n[3] = merci_buffer;
			
			sprintf(mersem_buffer, "%d", merci_sem_ID);
			argVec_n[4] = mersem_buffer;
			
			sprintf(porti_buffer, "%d", dati_porti_ID);
			argVec_n[5] = porti_buffer;
			
			sprintf(navi_buffer, "%d", pid_navi_ID);
			argVec_n[6] = navi_buffer;
			
			sprintf(datimerci_buffer, "%d", dati_merci_ID);
			argVec_n[7] = datimerci_buffer;
			
			argVec_n[8] = NULL;
			
			/*Mando in esecuzione il processo porto*/
			execv("nave", argVec_n);
			printf("Se ti senti inutile, pensa a me... :( \n");
			exit(1);
		} else if(p == -1){
			printf("Errore fork nave %d\n", fork_i);
			exit(1);
		} else {
			pid_navi[fork_i*3] = p;
			/*printf("Ho forkato! Id figlio: %d\n", p);
			sleep(1);*/
		}
		
	}
	
	printf("Caricamento... \n");
	sleep(1);
	printf("Navi pronte!\n");
	sleep(1);
	
	printf("---FORK METEO---\n");
	
	/*Fork processo meteo*/
	
	switch(p = fork()){
		case 0:
			/*Creo array argv*/
			argVec_m = (char**)calloc(5, sizeof(char*));
			
			sprintf(init_buffer, "%d", init_shmem_ID);
			argVec_m[0] = init_buffer;
			
			sprintf(insem_buffer, "%d", init_sem_ID);
			argVec_m[1] = insem_buffer;
			
			sprintf(msg_buffer, "%d", msg_ID);
			argVec_m[2] = msg_buffer;

			sprintf(porti_buffer, "%d", dati_porti_ID);
			argVec_m[3] = porti_buffer;
			
			sprintf(navi_buffer, "%d", pid_navi_ID);
			argVec_m[4] = navi_buffer;
			
			argVec_m[5] = NULL;

			/*Mando in esecuzione il processo porto*/
			execv("meteo", argVec_m);
			printf("Se ti senti inutile, pensa a me... :( \n");
			exit(1);
		case -1:
			printf("Errore fork meteo\n");
			exit(1);
		default:
			/*printf("Ho forkato! Id figlio: %d\n", p);*/
			pid_meteo = p;
	} 
	
	
	printf("Caricamento... \n");
	sleep(1);
	printf("Meteo pronto!\n");
	
	printf("---ATTENDO QUALCHE SECONDO PER CONSENTIRE DI COMPLETARE L'INIZIALIZZAZIONE---\n");
	sleep(2);
	
	initsops.sem_num = 0;
	initsops.sem_op = -1;
	initsops.sem_flg = 0;
	if(semop(init_sem_ID, &initsops, 1) == -1){
		printf("Errore nel decrementare il semaforo - Master\n");
		exit(1);
				
	} 
	printf("---FORK TERMINATE - INIZIO SIMULAZIONE---\n");
	initsops.sem_op = 0;
	if(semop(init_sem_ID, &initsops, 1) == -1){
		printf("Errore nell'attesa del semaforo - Master\n");
		exit(1);
	}
	
	/*Setto semaforo di inizializzazione a numero navi, utile per terminazione*/
	init_sem_arg.val = init_shmem -> SO_NAVI + 1;
	if(semctl(init_sem_ID, 0, SETVAL, init_sem_arg) == -1){
		printf("Errore inizializzazione semaforo inizializzazione");
		exit(1);
	}
	
	
	sleep(1);
	days++;
	init_shmem->days = days;
	while(days < init_shmem->SO_DAYS && prosegui == 1){ 
		/*Gestione report*/
		printf("------------------------------------------------------------------------------------------\n");
		printf("------------------------------------------GIORNO %d---------------------------------------\n", days);
		
		/*Invio sigusr1 a tutti*/
		for(i_porti = 0; i_porti < init_shmem->SO_PORTI; i_porti++){
			if(kill((int)dati_porti[i_porti*8], SIGUSR1) == -1){
				printf("Errore invio segnale SIGUSR1 al porto.\n");
			}
		}
		for(le_navi = 0; le_navi < init_shmem->SO_NAVI; le_navi++){
			nave_iter = pid_navi[le_navi*3];
			if(nave_iter != -1){
				if(kill(nave_iter, SIGUSR1) == -1){
					printf("Errore invio segnale SIGUSR1 a nave.\n");
				}
			}
		}
		if(kill(pid_meteo, SIGUSR1) == -1){
			printf("Errore invio segnale SIGUSR1 al meteo.\n");
		}
		/*Stampo report merci*/
		/*0 in porto 1 in nave 2 consegnata 3 scadporto 4 scadnave*/
		printf("--------------------------------------------MERCI-----------------------------------------\n");
		for(le_merci = 0; le_merci < init_shmem->SO_MERCI; le_merci++){
			printf("Merce tipo %d:\n", le_merci);
			printf("   In porto: %d\n", dump_merci[le_merci*5 + 0]);
			printf("   In nave: %d\n", dump_merci[le_merci*5 + 1]);
			printf("   Consegnata: %d\n", dump_merci[le_merci*5 + 2]);
			printf("   Scad. in porto: %d\n", dump_merci[le_merci*5 + 3]);
			printf("   Scad. in nave: %d\n", dump_merci[le_merci*5 + 4]);	
		}
		
		/*Stampo report navi*/
		printf("--------------------------------------------NAVI------------------------------------------\n");
		conta_cariche = 0;
		conta_scariche = 0;
		conta_porto = 0;
		conta_affondate = 0;
		for(le_navi = 0; le_navi < init_shmem->SO_NAVI; le_navi++){
			if(pid_navi[le_navi*3 + 0] == -1){
				conta_affondate++;
			} else if(pid_navi[le_navi*3 + 1] == 0 && pid_navi[le_navi*3 + 2] == 0){
				conta_scariche++;
			} else if (pid_navi[le_navi*3 + 1] == 0 && pid_navi[le_navi*3 + 2] == 1){
				conta_cariche++;
			} else if(pid_navi[le_navi*3 + 1] == 1){
				conta_porto++;
			}
		}
		printf("   In mare, cariche: %d\n", conta_cariche);
		printf("   In mare, scariche: %d\n", conta_scariche);
		printf("   In porto: %d\n", conta_porto);
		printf("   Affondate: %d\n", conta_affondate);
		printf("   Rallentate da tempesta: %d\n", init_shmem->rallentate); /*Una nave al giorno!*/
		
		if(conta_affondate == init_shmem->SO_NAVI){
			prosegui = 0;
			printf("---TUTTE LE NAVI SONO AFFONDATE---\n");
		}
		
		/*Stampo report porti*/
		printf("--------------------------------------------PORTI-----------------------------------------\n");
		for(i_porti = 0; i_porti<init_shmem->SO_PORTI; i_porti++){
			
			printf("Porto %d\n", dump_porti[i_porti*6 + 0]);
			printf("   Merce presente: %d\n", dump_porti[i_porti*6 + 1]);
			printf("   Merce spedita: %d\n", dump_porti[i_porti*6 + 2]);
			printf("   Merce ricevuta: %d\n", dump_porti[i_porti*6 + 3]);
			printf("   Banchine occupate/totali: %d/%d\n", dump_porti[i_porti*6 + 5], dump_porti[i_porti*6 + 4]);
		}
		
		/*Stampo report meteo*/
		printf("Porti rallentati dalla mareggiata: \n");
		for(i_porti = 0; i_porti<init_shmem->SO_PORTI; i_porti++){
			if(dati_porti[i_porti*8 + 6] > 0){
				printf("   - %d, %d\n", (int)dati_porti[i_porti*8 + 0], (int)dati_porti[i_porti*8 + 6]);
			}
		}
		sleep(1);
		days++;
		init_shmem->days = days;
	}
	
	/*Report finale*/
	/*Stampo report merci*/
		printf("------------------------------------------------------------------------------------------\n");
		printf("-------------------------------------DUMP TERMINAZIONE------------------------------------\n");
		printf("------------------------------------------GIORNO %d---------------------------------------\n", days);
		/*0 in porto 1 in nave 2 consegnata 3 scadporto 4 scadnave*/
		printf("--------------------------------------------MERCI-----------------------------------------\n");
		for(le_merci = 0; le_merci < init_shmem->SO_MERCI; le_merci++){
			printf("Merce tipo %d:\n", le_merci);
			printf("   In porto: %d\n", dump_merci[le_merci*5 + 0]);
			printf("   In nave: %d\n", dump_merci[le_merci*5 + 1]);
			printf("   Consegnata: %d\n", dump_merci[le_merci*5 + 2]);
			printf("   Scad. in porto: %d\n", dump_merci[le_merci*5 + 3]);
			printf("   Scad. in nave: %d\n", dump_merci[le_merci*5 + 4]);	
			
			contafill += dump_merci[le_merci*5 + 0];
			contafill += dump_merci[le_merci*5 + 1];
			contafill += dump_merci[le_merci*5 + 2];
			contafill += dump_merci[le_merci*5 + 3];
			contafill += dump_merci[le_merci*5 + 4];
			
			for(i_porti = 0; i_porti<init_shmem->SO_PORTI; i_porti++){
				if(porti_merci[i_porti*init_shmem->SO_MERCI + le_merci] > max){
					max = porti_merci[i_porti*init_shmem->SO_MERCI + le_merci];
					winner = dati_porti[i_porti*8];
				}
			}
			
			printf("   Maggior produzione presso il porto: %d\n", winner);
			max = 0;
		}
		
		printf("Fill totale: %d\n", contafill);
	/*Stampo report navi*/
		printf("--------------------------------------------NAVI------------------------------------------\n");
		conta_cariche = 0;
		conta_scariche = 0;
		conta_porto = 0;
		conta_affondate = 0;
		for(le_navi = 0; le_navi < init_shmem->SO_NAVI; le_navi++){
			if(pid_navi[le_navi*3 + 0] == -1){
				conta_affondate++;
			} else if(pid_navi[le_navi*3 + 1] == 0 && pid_navi[le_navi*3 + 2] == 0){
				conta_scariche++;
			} else if (pid_navi[le_navi*3 + 1] == 0 && pid_navi[le_navi*3 + 2] == 1){
				conta_cariche++;
			} else if(pid_navi[le_navi*3 + 1] == 1){
				conta_porto++;
			}
		}
		printf("   In mare, cariche: %d\n", conta_cariche);
		printf("   In mare, scariche: %d\n", conta_scariche);
		printf("   In porto: %d\n", conta_porto);
		printf("   Affondate: %d\n", conta_affondate);
		printf("   Rallentate da tempesta: %d\n", init_shmem->rallentate); /*Una nave al giorno!*/
	/*Stampo report porti*/
		printf("--------------------------------------------PORTI-----------------------------------------\n");
		for(i_porti = 0; i_porti<init_shmem->SO_PORTI; i_porti++){
			
			printf("Porto %d\n", dump_porti[i_porti*6 + 0]);
			printf("   Merce presente: %d\n", dump_porti[i_porti*6 + 1]);
			printf("   Merce spedita: %d\n", dump_porti[i_porti*6 + 2]);
			printf("   Merce ricevuta: %d\n", dump_porti[i_porti*6 + 3]);
			printf("   Banchine occupate/totali: %d/%d\n", dump_porti[i_porti*6 + 5], dump_porti[i_porti*6 + 4]);
		}
	/*Stampo report meteo*/
		printf("Porti rallentati dalla mareggiata: \n");
		for(i_porti = 0; i_porti<init_shmem->SO_PORTI; i_porti++){
			if(dati_porti[i_porti*8 + 6] > 0){
				printf("   - %d, %d\n", (int)dati_porti[i_porti*8 + 0], (int)dati_porti[i_porti*8 + 6]);
			}
		}
	/*Terminazione*/
	printf("---PREDISPOSIZIONE TERMINAZIONE---\n");
	/*Termino tutti i processi nave e del processo meteo*/
	for(le_navi = 0; le_navi < init_shmem->SO_NAVI; le_navi++){
		if(pid_navi[le_navi*3] != -1){
			p_term = pid_navi[le_navi*3];
			if(kill(p_term, SIGTERM) == -1){
				printf("Errore invio segnale SIGTERM a nave. Provo SIGKILL.\n");
				if(kill(p_term, SIGKILL) == -1){
					printf("Errore invio segnale SIGKILL a nave. Ti attacchi adesso.\n");
				
				}
			}
		}
	}
	printf("   Navi terminate con successo.\n");
	
	if(kill(pid_meteo, SIGTERM) == -1){
		printf("Errore invio segnale SIGTERM al meteo.\n");
	}
	printf("   Meteo terminato con successo.\n");
	
	/*Attendo terminazione di tutti i processi nave e meteo per terminare i porti*/
	initsops.sem_num = 0;
	initsops.sem_flg = 0;
	initsops.sem_op = 0;
	if(semop(init_sem_ID, &initsops, 1) == -1){
		printf("Errore nell'attesa del semaforo - Master\n");
		exit(1);
	}
	
	init_sem_arg.val = init_shmem -> SO_PORTI;
	if(semctl(init_sem_ID, 0, SETVAL, init_sem_arg) == -1){
		printf("Errore inizializzazione semaforo inizializzazione");
		exit(1);
	}
	
	/*Termino tutti i processi porto*/
	for(i_porti = 0; i_porti < init_shmem->SO_PORTI; i_porti++){
		p_term = dati_porti[i_porti*8];
		if(kill(p_term, SIGTERM) == -1){
			printf("Errore invio segnale SIGTERM a porto. Provo SIGKILL.\n");
			if(kill(p_term, SIGKILL) == -1){
				printf("Errore invio segnale SIGKILL a porto. Ti attacchi adesso.\n");
				
			}
		}
	}
	printf("   Porti terminati con successo.\n");
	
	/*Attendo terminazione di tutti i processi porto per rimuovere le strutture IPC*/
	initsops.sem_num = 0;
	initsops.sem_flg = 0;
	initsops.sem_op = 0;
	if(semop(init_sem_ID, &initsops, 1) == -1){
		printf("Errore nell'attesa del semaforo - Master\n");
		exit(1);
	}
	
	/*Rimuovo le strutture IPC*/
	printf("---RIMOZIONE STRUTTURE IPC---\n");
	if(shmdt(init_shmem) == -1){printf("Errore detach init_shmem - Master\n");}
	if(shmctl(init_shmem_ID, IPC_RMID, NULL) == -1){printf("Errore remove init_shmem - Master\n");}
	
	if(shmdt(dump_merci) == -1){printf("Errore detach dump_merci - Master\n");}
	if(shmctl(dump_merci_ID, IPC_RMID, NULL) == -1){printf("Errore remove dump_merci - Master\n");}
	
	if(shmdt(dati_merci) == -1){printf("Errore detach dati_merci - Master\n");}
	if(shmctl(dati_merci_ID, IPC_RMID, NULL) == -1){printf("Errore remove dati_merci - Master\n");}
	
	if(shmdt(dump_porti) == -1){printf("Errore detach dump_porti - Master\n");}
	if(shmctl(dump_porti_ID, IPC_RMID, NULL) == -1){printf("Errore remove dump_porti - Master\n");}
	
	if(shmdt(dati_porti) == -1){printf("Errore detach dati_porti - Master\n");}
	if(shmctl(dati_porti_ID, IPC_RMID, NULL) == -1){printf("Errore remove dati_porti - Master\n");}
	
	if(shmdt(pid_navi) == -1){printf("Errore detach pid_navi - Master\n");}
	if(shmctl(pid_navi_ID, IPC_RMID, NULL) == -1){printf("Errore remove pid_navi - Master\n");}
	
	if(shmdt(porti_merci) == -1){printf("Errore detach porti_merci - Master\n");}
	if(shmctl(porti_merci_ID, IPC_RMID, NULL) == -1){printf("Errore remove porti_merci_ID - Master\n");}
	
	if(semctl(init_sem_ID, 0, IPC_RMID, NULL) == -1){printf("Errore remove init_sem - Master\n");}
	
	if(semctl(merci_sem_ID, 0, IPC_RMID, NULL) == -1){printf("Errore remove merci_sem - Master\n");}
	
	/*if(msgctl(msg_ID, IPC_RMID, NULL) == -1){printf("Errore remove msg - Master\n");}*/
	
	
	printf("---TERMINAZIONE AVVENUTA CON SUCCESSO---\n");
	
	return;
}













































