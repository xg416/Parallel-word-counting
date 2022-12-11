gcc omp.c -o omp -fopenmp
# ./omp num_threads num_readers file_dir n_repeat
./omp 16 4 ../files 2
rm omp