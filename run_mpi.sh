mpicc MPI.c -o MPI -fopenmp
mpirun ./MPI -n 1
rm MPI