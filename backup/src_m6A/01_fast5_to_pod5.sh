#!/bin/bash
#SBATCH -J convert_fast5_to_pod5
#SBATCH -A HENDERSON-SL3-CPU
#SBATCH -p icelake
#SBATCH --nodes=1
#SBATCH --ntasks=1
#SBATCH --cpus-per-task=16
#SBATCH --time=4:00:00
#SBATCH --mem=50G
#SBATCH -o convert_%j.out
#SBATCH -e convert_%j.err

# Activate conda environment if needed
source ~/miniforge3/etc/profile.d/conda.sh
conda activate myenv

cd /home/ns2040/rds/rds-pollen-7KhEnGLnebA/namil/data/Leduque24_Fiber-seq/SAM-seq5

# Convert FAST5 to POD5
pod5 convert fast5 fast5/ --output SAM-seq5.pod5 --recursive

echo "Conversion complete"
ls -lh SAM-seq5.pod5
