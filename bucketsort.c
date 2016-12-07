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
int bucketSort();
int divideIntoBuckets();


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
int *valsInBuckets;
int *myArrToSort;
int *bucketStop;

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
        while(n % comm_sz != 0){
            printf("Please enter an array size divisable by the number of processes:\n");
            scanf("%ld", &n);
        }
        
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
        divideIntoBuckets();

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
        divideIntoBuckets();
    }
    free(bucketStop);
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
    free(samples);
    free(samples_temp);
    printf("pivots:\n");
    printArray(pivots, comm_sz - 1);
    return 0;
}


int divideIntoBuckets(){
    int i;
    int *tempbucket = (int *) malloc(sizeof(int) * local_n);
    serialsort(local_n, local_vecParallel, tempbucket);
    free(tempbucket);
    bucketStop = (int *) malloc(sizeof(int) * comm_sz);
    int bucketNum = 0;
    
    for(i = 0; i < local_n; i++){
        // Determine bucket stop
        if(local_vecParallel[i] >= pivots[bucketNum]){
            while(local_vecParallel[i] >= pivots[bucketNum]){
                printf("Proc %d bucketnum %i stop is %i\n", my_id, bucketNum, i);
                bucketStop[bucketNum] = i;
                bucketNum++;
                if(bucketNum == comm_sz - 1){
                    break;
                }
            }
        }
        if(bucketNum == comm_sz - 1){
            break;
        }
    }
    while(bucketNum < comm_sz){
        bucketStop[bucketNum] = local_n;
        bucketNum++;
    }
    printf("Proc %d i %i bucket indices:", my_id, i);
    printArray(bucketStop, comm_sz);
    printArray(local_vecParallel, local_n);
    return 0;
}



int bucketSort(){
    int i, j, k, sizeRecv;
    int sizeMyVals = 0;
    int *sizeFromOthers = (int *) malloc(sizeof(int)*comm_sz);
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
    free(local_vecParallel);
    free(pivots);
    // Send # of values in buckets to other procs
    for(i = 0; i < comm_sz; i++){
        if(i != my_id){
            // Send number of values proc should expect to receive
            printf("Proc %d sending to %i\n", my_id, i);
            MPI_Send(&buckets[i].size, 1, MPI_INT, i, 0, MPI_COMM_WORLD);
        } else {
            sizeMyVals += buckets[i].size;
            // Recieve number of values proc should expect to receive
            for(j = 0; j < comm_sz; j++){
                if(j == my_id){
                    continue;
                } else {
                    printf("Proc %d receiving from %i\n", my_id, j);
                    MPI_Recv(&sizeRecv, 1, MPI_INT, j, 0, MPI_COMM_WORLD, MPI_STATUS_IGNORE);
                    sizeMyVals += sizeRecv;
                    sizeFromOthers[j] = sizeRecv;
                }
            }
        }
    }
    printf("Proc %d Size of my vals %i\n", my_id, sizeMyVals);

    // allocate memory for an array of vals to sort
    if(sizeMyVals > 0){
        myArrToSort = (int *) malloc(sizeof(int)*sizeMyVals);
    }
    
    for(i = 0; i < comm_sz; i++){
        // Create array from values in linkedlist
        int *values;
        if(buckets[i].size != 0){
            values = (int *) malloc(sizeof(int) * buckets[i].size);
            node *currentNode = buckets[i].linkedList;
            for(j = 0; j < buckets[i].size; j++){
                values[j] = currentNode->value;
                currentNode = currentNode->next;
                printf("Proc %d adding val %i to values for proc %d\n", my_id, values[j], i);
            }
            //printArray(values, buckets[i].size);
        }
        
        if(i == my_id){
            int index = 0;
            if(buckets[i].size != 0){
                // Add my values to myArrToSort
                for(j = 0; j < buckets[i].size; j++){
                    printf("Proc %d adding val %i to myArrToSort index %i\n", my_id, values[j], index);
                    myArrToSort[index] = values[j];
                    index++;
                }
            }
            // Recieve values from other procs
            for(j = 0; j < comm_sz; j++){
                if(j == my_id || sizeFromOthers[j] == 0){
                    continue;
                } else {
                    int *recVal = (int *) malloc(sizeof(int)*sizeFromOthers[j]);
                    printf("Proc %d receiving from %i, %i elems\n", my_id, j, sizeFromOthers[j]);
                    //MPI_Recv(&recVal, sizeFromOthers[j], MPI_INT, j, 0, MPI_COMM_WORLD, MPI_STATUS_IGNORE);
                    printf("Proc %d received value %i\n", my_id, recVal[0]);
                    // Add j's values to myArrToSort
                    for(k = 0; k < sizeFromOthers[j]; k++){
                        printf("Proc %d index is %i\n", my_id, index);
                        myArrToSort[index] = values[k];
                        index++;
                    }
                }
            }
            if(sizeMyVals > 0){
                printArray(myArrToSort, sizeMyVals);
            }
            
        } else {
            if(buckets[i].size != 0){
                //MPI_Send(&values, buckets[i].size, MPI_INT, i, 0, MPI_COMM_WORLD);
                printf("Proc %d sent value %i, %i elems\n", my_id, values[0], buckets[i].size);
            }
        }
    }
    return 0;

}
