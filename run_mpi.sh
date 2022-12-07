mpicc MPI2.c -o MPI -fopenmp
mpirun ./MPI -n 1
rm MPI