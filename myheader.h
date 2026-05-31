/* Header */
#define _GNU_SOURCE
#include <sys/types.h>

#ifndef INCLUSO
#define INCLUSO

/*Struttura - Lotto di merce*/
struct merce{
	int tipo;
	int quantita;
	int scadenza;
	struct merce *next;
	int stato_flag; /*0 = in porto, 1 = in nave*/

};

/*Struttura - Memoria condivisa di inizializzazione*/
struct shmem_parametri {
	int SO_BANCHINE;
	double SO_LATO;
	int SO_MERCI;
	int SO_FILL;
	int SO_DAYS;
	int SO_SPEED;
	int SO_CAPACITY;
	int SO_LOADSPEED;
	int SO_PORTI;
	int SO_NAVI;
	int SO_SWELL_DURATION;
	int SO_STORM_DURATION;
	int SO_MAELSTROM;
	int days;
	int rallentate;
	double SO_THRESHOLD;
};

/*Struttura - Catalogo merce in porto*/
struct merci_porto {
	int dom_off;
	int richiesta;
	int ricevuta; 
	int scaduta;
	int disponibile;
	int spedita;
	struct merce* lotti;
	struct merce* coda;
};

/*Struttura - Messaggio porto*/
struct msg_porto {
	long mtype; /*0 = porto, 1 = nave, 2 = meteo*/
	int pid_p;
	int disponibile;
	int spedita;
	int ricevuta;
	int banchine;
	int b_occupate;
};

/*Struttura - Messaggio nave*/

struct msg_nave{
	long mtype;
	int pid_n;
	int flag_stato; /*0 = in mare, 1 = in porto*/
	int flag_merce; /*0 = scarica, 1 = carica*/
};

/*Struttura - messaggio meteo*/
struct msg_meteo{
	long mtype;
	int affondate;
};

/*Msg nave-porto*/
struct msg_np {
	long mtype; /*1*/
	int caricoScarico; /* 0/1 */
	pid_t pid_n;
	int ton;
	int tipo;
	int quantita;
	int scadenza;
};

/*Msg porto-nave*/
struct msg_pn{
	long mtype; /*pid nave ricevente*/
	int tipo; /*Se -1: terminazione trasferimento*/
	int quantita;
	int scadenza;	
};

/*Semun*/
union semun {
	int val; /* Value per SETVAL */
	struct semid_ds *buf; /* Buffer per IPC_STAT, IPC_SET */
	unsigned short *array; /* Array per GETALL, SETALL */
	struct seminfo *__buf; /* Buffer per IPC_INFO */
};

/*Timespec
struct timespec {
    time_t   tv_sec;        /* seconds 
    long     tv_nsec;       /* nanoseconds 
};*/

/*struct sigaction{
	void (*sa_handler) (int signum);
	sigset_t sa_mask;
	int sa_flags;
};

/*Implementazione semafori binari prelevata dalle slides del corso*/

extern int initSemAvailable(int semId, int semNum);

extern int initSemInUse(int semId, int semNum);

extern int reserveSem(int semId, int semNum);

extern int releaseSem(int semId, int semNum);

extern int reserveSemNB(int semId, int semNum);
#endif














