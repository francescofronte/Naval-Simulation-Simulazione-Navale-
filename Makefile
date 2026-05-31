CC	= gcc
CFLAGS	= -std=c89 -Wpedantic
RM	= rm -f


all: meteo porto nave simulazioneNavale

meteo: meteo.c myheader.h
	$(CC) $(CFLAGS) meteo.c functions.c -lm -o meteo

porto: porto.c myheader.h
	$(CC) $(CFLAGS) porto.c functions.c -lm -o porto

nave: nave.c myheader.h
	$(CC) $(CFLAGS) nave.c functions.c -lm -o nave

simulazioneNavale: master.c myheader.h
	$(CC) $(CFLAGS) master.c functions.c -lm -o simulazioneNavale
	

clean:
	$(RM) meteo porto nave simulazioneNavale
