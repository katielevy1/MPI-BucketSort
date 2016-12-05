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

 /* Function declarations */
int serialsort(int size);
int mergeSort(int start, int stop);
int merge(int start, int middle, int stop);
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
   // Check for 2 command line args
   if(argc != 3){
       printf("You need to enter 2 command line arguments to run this program");
       exit(0);
   }
   
    // Parse command line args
    processCount = (int) strtol(argv[1], NULL, 10);
    n = strtol(argv[2], NULL, 10);

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
    serialsort(n);
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
}

// Returns 0 on success and 1 on failure
int serialsort(int size){
    if(!(mergeSort(0, size -1))){
        return 0;
    }else{
        return 1;
    }
}

// Serial mergesort
int mergeSort(int start, int stop){
    if(start >= stop){
        return 0;
    }
    int middle = ((stop + start) / 2);
    mergeSort(start, middle);
    mergeSort(middle+1, stop);
    merge(start, middle, stop);
    return 0;
}

// Merge portion of mergesort
int merge(int start, int middle, int stop){
    int first = start;
    int second = middle+1;
    int tempIndex = start;
    while(first <= middle && second <= stop){
        if(vecSerial[first] < vecSerial[second]){
            temp[tempIndex] = vecSerial[first];
            first++;
            tempIndex++;
        } else {
            temp[tempIndex] = vecSerial[second];
            second++;
            tempIndex++;
        }
    }
    while(first <= middle){
        temp[tempIndex] = vecSerial[first];
            first++;
            tempIndex++;
    }
    while(second <= stop){
        temp[tempIndex] = vecSerial[second];
            second++;
            tempIndex++;
    }
    int i;
    for(i = start; i <= stop; i++){
        vecSerial[i] = temp[i];
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
    int *samples_sorted;
    samples_sorted = (int *) malloc(sizeof(int) * s);
    int i;
    for(i = 0; i < s; i++){
        int random = rand() % 100;
        samples[i] = random;
    }
    //TODO: samples_sorted = serialsort(samples, s);

    // Process 0 sends the pivots to all the processes
    return 0;
}