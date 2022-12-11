mpicc MPI.c -o MPI -fopenmp
mpirun -n 4 ./MPI 4 1 ../files 2
rm MPI