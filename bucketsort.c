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
#include<unistd.h>

 /* Function declarations */
int serialsort(int size, int unsorted[], int tempA[]);
int mergeSort(int start, int stop, int unsorted[], int tempA[]);
int merge(int start, int middle, int stop, int unsorted[], int tempA[]);
int validateSerial();
int validateParallel();
void printArray(int arr[], int size);
int createPivots();
int* bucketSort();


/* Global variables */
int comm_sz;
long n; //array size
int *vecSerial;
int *vecParallel;
int *temp;
int *pivots;
int *local_vecParallel;
int local_n;
int my_id, root_process, ierr;

struct node {
    int value;
    struct node *next;
};
typedef struct node node;

struct bucket {
    int size;
    struct node *linkedList;
};
typedef struct bucket bucket;

/*--------------------------------------------------------------------*/
int main(int argc, char* argv[]){
    MPI_Status status;
    ierr = MPI_Init(&argc, &argv);
    ierr = MPI_Comm_rank(MPI_COMM_WORLD, &my_id);
    ierr = MPI_Comm_size(MPI_COMM_WORLD, &comm_sz);
    pivots = (int *) malloc(sizeof(int) * comm_sz-1);

    // Process 0
    if( my_id == 0 ) {
        // Get n from standard input
        printf("Enter the size of the array:\n");
        scanf("%ld", &n);
        
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
        // Calculate the pivots
        createPivots();

        // Broadcast n and pivots to other procs
        MPI_Bcast(&n, 1, MPI_LONG, 0, MPI_COMM_WORLD);
        MPI_Bcast(pivots, comm_sz - 1, MPI_INT, 0, MPI_COMM_WORLD);

        // Distribute vecParallel to different processes with block distribution
        // local_n is number of elems per proc
        local_n = n / comm_sz;
        local_vecParallel = (int *)malloc(sizeof(int) * local_n);
        MPI_Scatter(vecParallel, local_n, MPI_INT, local_vecParallel, local_n,
            MPI_INT, 0, MPI_COMM_WORLD);
        free(vecParallel);
        bucketSort();

        gettimeofday(&tv2, NULL); // stop timing
        double parallelTime = (double) (tv2.tv_usec - tv1.tv_usec) / 1000000 +
            (double) (tv2.tv_sec - tv1.tv_sec);
        printf("Parallel time = %e\n", parallelTime);
        //printArray(vecParallel, n);
        //TODO: Validate parallel 

        // Print results
        double speedup = serialTime / parallelTime;
        double efficiency = speedup / comm_sz;
        /*printf("Number of processes: %d\n", comm_sz);
        printf("Array Size: %ld\n", n);
        printf("Time to sort with serial merge sort: %e\n", serialTime);
        printf("Time to sort with parallel bucket sort: %e\n", parallelTime);
        printf("Speedup: %e\n", speedup);
        printf("Efficincy: %e\n", efficiency);*/

        free(vecSerial);
        free(temp);
    } else {
        // Broadcast to recieve n
        MPI_Bcast(&n, 1, MPI_LONG, 0, MPI_COMM_WORLD);
        MPI_Bcast(pivots, comm_sz - 1, MPI_INT, 0, MPI_COMM_WORLD);

        // Distribute vecParallel to different processes with block distribution
        local_n = n / comm_sz;  // local_n is number of elems per proc
        local_vecParallel = (int *)malloc(sizeof(int) * local_n);
        MPI_Scatter(vecParallel, local_n, MPI_INT, local_vecParallel, local_n,
            MPI_INT, 0, MPI_COMM_WORLD);
        for(int j = 0; j < local_n; j++){
            printf("Process %d: %i\n", my_id, local_vecParallel[j]);
        }
        bucketSort();

    }
    ierr = MPI_Finalize();
    return 0;
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

// Creates the pivots for procs to know to which proc to send elems
int createPivots(){
    // Process 0 computes pivots
    int s = (int) 10 * comm_sz * log2(n);
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

    for(i = 0; i < comm_sz - 1; i++){
        pivots[i] = samples[((i+1) * s) / comm_sz];
    }
    printf("pivots:\n");
    printArray(pivots, comm_sz - 1);
    return 0;
}

int* bucketSort(){
    int *keepVals = NULL;
    int i, j;
    bucket *buckets = malloc(sizeof(bucket) * comm_sz);
    for(i = 0; i < comm_sz; i++){
        buckets[i].size = 0;
        buckets[i].linkedList = NULL;
    }
    for(i = 0; i < local_n; i++){
        // Create node with the value
        node *curr = malloc(sizeof(node));
        curr->value = local_vecParallel[i];
        curr->next = NULL;
        // Determine owner of the elem value
        int owner = -1;
        for(j = 0; j < comm_sz - 1; j++){
            if(local_vecParallel[i] < pivots[j]){
                owner = j;
                break;
            }
        }
        if(owner == -1){
            owner = comm_sz - 1;
        }
        // Add node to the owner's bucket
        buckets[owner].size++;
        curr->next = buckets[owner].linkedList;
        buckets[owner].linkedList = curr;
        printf("Proc %d element %i inside bucket %d\n", my_id, curr->value, owner);
    }
    // Send values in buckets to other procs
    for(i = 0; i < comm_sz; i++){
        if(i != my_id){
            MPI_Send(&buckets[i].size, 1, MPI_INT, i, 0, MPI_COMM_WORLD);
        } 
        if (buckets[i].size == 0){
            continue;
        }
        // Create array from values in linkedlist
        int *values = (int*) malloc(sizeof(int) * buckets[i].size);
        node *currentNode = buckets[i].linkedList;
        for(j = 0; j < buckets[i].size; j++){
            values[j] = currentNode->value;
            currentNode = currentNode->next;
        }
        if(i == my_id){
            keepVals = values;
        } else {
            MPI_Send(values, buckets[i].size, MPI_INT, i, 0, MPI_COMM_WORLD);
        }
    }
    return keepVals;

}