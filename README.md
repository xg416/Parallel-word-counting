This project is implemented by Xingguang Zhang and Haofei Tian

#### The serial version: see the run_serial.sh
| compile:
gcc serial.c -o serial -fopenmp
| the arguments are 1. path to all files 2. repeat numbers of the files, for example:
./serial ../files 4


####The OpenMP version: see the run.sh
| compile:
gcc omp.c -o omp -fopenmp
| Our OpenMP version has 3 variants as described in the report: the parallel.c is the version A, the omp.c is the version C, the omp_RQ.c is the version B. The input arguments for version B and C are the same.
| the arguments are 1. number of all threads 2. number of reader threads 3. path to all files 4. repeat numbers of the files, for example:
./omp 16 4 ../files 4
| the version A doesn't specify the number of reader threads, so you just need 1,3, and 4, for example:
./omp 16 ../files 4


#### The MPI version: see the run_mpi.sh
| compile:
mpicc MPI.c -o MPI -fopenmp
| the arguments are the same as the OpenMP version, but you need to specify the number of nodes explicitly by mpirun -n, for example if we use 16 nodes:
mpirun -n 16 ./MPI 4 1 ../files 64
