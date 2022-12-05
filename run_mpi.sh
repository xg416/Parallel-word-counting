mpicc MPI.c -o MPI -fopenmp
mpirun ./MPI -n 16
rm MPI