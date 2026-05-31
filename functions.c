#include <stdio.h>
#include "myheader.h"
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

extern int initSemAvailable(int semId, int semNum) {
	union semun arg;
	arg.val = 1;
	return semctl(semId, semNum, SETVAL, arg);
}

extern int initSemInUse(int semId, int semNum) {
	union semun arg;
	arg.val = 0;
	return semctl(semId, semNum, SETVAL, arg);
}

extern int reserveSem(int semId, int semNum) {
	struct sembuf sops;
	sops.sem_num = semNum;
	sops.sem_op = -1;
	sops.sem_flg = 0;
	return semop(semId, &sops, 1);
}

extern int releaseSem(int semId, int semNum) {
	struct sembuf sops;
	sops.sem_num = semNum;
	sops.sem_op = 1;
	sops.sem_flg = 0;
	return semop(semId, &sops, 1);
}

extern int reserveSemNB(int semId, int semNum) {
	struct sembuf sops;
	sops.sem_num = semNum;
	sops.sem_op = -1;
	sops.sem_flg = IPC_NOWAIT;
	return semop(semId, &sops, 1);
}
