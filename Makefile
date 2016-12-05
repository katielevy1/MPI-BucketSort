bucketsort: bucketsort.o 
	gcc  -Wall bucketsort.o  -o bucketsort

bucketsort.o: bucketsort.c
	gcc -c -Wall bucketsort.c 

clean:
	rm -f bucketsort.o bucketsort
