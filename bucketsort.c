/*
 * File: bucketsort.c
 * Author: Katie Levy and Michael Poyatt
 * Date: 12/4/16
 * Description: 
 */

#include<stdlib.h>
#include<stdio.h>
#include<string.h>
#include<time.h>
#include<sys/time.h>
#include<math.h>
#include<mpi.h>

 /* Function declarations */
int serialsort(int size, int unsorted[], int tempA[]);
int mergeSort(int start, int stop, int unsorted[], int tempA[]);
int merge(int start, int middle, int stop, int unsorted[], int tempA[]);
int validateSerial();
int validateParallel();
void printArray(int arr[], int size);
int bucketSort();


/* Global variables */
int processCount;
long n; //array size
int *vecSerial;
int *vecParallel;
int *temp;

/*--------------------------------------------------------------------*/
int main(int argc, char* argv[]){
   // Check for 1 command line args
   if(argc != 2){
       printf("You need to enter a command line argument to run this program");
       exit(0);
   }
   int my_id, root_process, ierr;
    MPI_Status status;
    ierr = MPI_Init(&argc, &argv);
    ierr = MPI_Comm_rank(MPI_COMM_WORLD, &my_id);
    ierr = MPI_Comm_size(MPI_COMM_WORLD, &processCount);
    
    if( my_id == 0 ) {

        // Parse command line args
        n = strtol(argv[1], NULL, 10);

        // For timing
        struct timeval  tv1, tv2;

        // Allocate memory for global arrays
        vecSerial = (int *) malloc(sizeof(int) * n);
        vecParallel = (int *) malloc(sizeof(int) * n);   
        temp = (int *) malloc(sizeof(int) * n); 
        int i; 

        // Fill the arrays with the same random numbers
        srand(time(NULL));
        for(i = 0; i < n; i++){
            int random = rand() % 100;
            vecSerial[i] = random;
        }

        // Copy first array to second array
        memcpy(vecParallel, vecSerial, sizeof(int)*n);
        memcpy(temp, vecSerial, sizeof(int)*n);

        // Perform the serial mergesort
        gettimeofday(&tv1, NULL); // start timing
        serialsort(n, vecSerial, temp);
        gettimeofday(&tv2, NULL); // stop timing
        double serialTime = (double) (tv2.tv_usec - tv1.tv_usec) / 1000000 +
            (double) (tv2.tv_sec - tv1.tv_sec);
        // Print results.
        printf("Serial time = %e\n", serialTime);
        validateSerial();
        printArray(vecSerial, n);
        

        // Perform the parallel bucketsort
        gettimeofday(&tv1, NULL); // start timing

        bucketSort();
    
        gettimeofday(&tv2, NULL); // stop timing
        double parallelTime = (double) (tv2.tv_usec - tv1.tv_usec) / 1000000 +
            (double) (tv2.tv_sec - tv1.tv_sec);
        printf("Parallel time = %e\n", parallelTime);
        //printArray(vecParallel, n);
        //TODO: Validate parallel 

        // Print results
        double speedup = serialTime / parallelTime;
        double efficiency = speedup / processCount;
        /*printf("Number of processes: %d\n", processCount);
        printf("Array Size: %ld\n", n);
        printf("Time to sort with serial merge sort: %e\n", serialTime);
        printf("Time to sort with parallel bucket sort: %e\n", parallelTime);
        printf("Speedup: %e\n", speedup);
        printf("Efficincy: %e\n", efficiency);*/

        free(vecSerial);
        free(vecParallel);
        free(temp);
        return 0;
    } else {

    }
    ierr = MPI_Finalize();
}

// Returns 0 on success and 1 on failure
int serialsort(int size, int unsorted[], int tempA[]){
    if(!(mergeSort(0, size -1, unsorted,  tempA))){
        return 0;
    }else{
        return 1;
    }
}

// Serial mergesort
int mergeSort(int start, int stop, int unsorted[], int tempA[]){
    if(start >= stop){
        return 0;
    }
    int middle = ((stop + start) / 2);
    mergeSort(start, middle, unsorted,  tempA);
    mergeSort(middle+1, stop, unsorted,  tempA);
    merge(start, middle, stop, unsorted,  tempA);
    return 0;
}

// Merge portion of mergesort
int merge(int start, int middle, int stop, int unsorted[], int tempA[]){
    int first = start;
    int second = middle+1;
    int tempIndex = start;
    while(first <= middle && second <= stop){
        if(unsorted[first] < unsorted[second]){
            tempA[tempIndex] = unsorted[first];
            first++;
            tempIndex++;
        } else {
            tempA[tempIndex] = unsorted[second];
            second++;
            tempIndex++;
        }
    }
    while(first <= middle){
        tempA[tempIndex] = unsorted[first];
            first++;
            tempIndex++;
    }
    while(second <= stop){
        tempA[tempIndex] = unsorted[second];
            second++;
            tempIndex++;
    }
    int i;
    for(i = start; i <= stop; i++){
        unsorted[i] = tempA[i];
    }
    return 0;
}

// Verify that the serial mergesort is correct
int validateSerial(){
    int i;
    for(i = 0; i < n-1; i++){
        if(vecSerial[i] > vecSerial[i+1]){
            printf("Serial sort unsuccesful.\n");
            return 1;
        }
    }
    return 0;
}

// Verify the parallel bucketsort is correct
// by comparing the arrays
int validateParallel(){
    int i;
    for(i = 0; i < n-1; i++){
        if(vecSerial[i] != vecParallel[i]){
            printf("Parallel sort unsuccesful.\n");
            return 1;
        }
    }
    return 0;
}

// Print array passed in as argument
void printArray(int arr[], int size){  
    int i;
    for(i = 0; i < size; i++){
        printf("%d\t", arr[i]);
    }
    printf("\n");
    return;
}

int bucketSort(){
    // Process 0 computes pivots
    int *pivots;
    pivots = (int *) malloc(sizeof(int) * processCount-1);
    int s = (int) 10 * processCount * log2(n);
    int *samples;
    samples = (int *) malloc(sizeof(int) * s);
    int *samples_temp;
    samples_temp = (int *) malloc(sizeof(int) * s);
    int i;
    for(i = 0; i < s; i++){
        int random = rand() % 100;
        samples[i] = random;
    }
    serialsort(s, samples, samples_temp);

    for(i = 0; i < processCount - 1; i++){
        pivots[i] = samples[((i+1) * s) / processCount];
    }
    // Process 0 sends the pivots to all the processes
    // MPI_Bcast()
    return 0;
}