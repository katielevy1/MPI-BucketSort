bucketsort: bucketsort.o 
	mpicc bucketsort.o -o bucketsort

bucketsort.o: bucketsort.c
	mpicc -c bucketsort.c

clean:
	rm -f bucketsort.o bucketsort
