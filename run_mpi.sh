mpicc MPI2.c -o MPI -fopenmp
mpirun -n 4 ./MPI
rm MPI