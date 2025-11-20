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

dir_fast5="path/to/fast5/dir/"
# Directory where fast5 files exist
output="path/to/output.pod5"
# Path to output pod5 file
cd "$wd"

# Convert FAST5 to POD5
pod5 convert fast5 ${dir_fast5} --output ${output} --recursive
# -r, --recursive: Search for input files recursively matching `*.pod5` (default: False)
# required if you're converting multiple fast5 files

echo "Conversion complete"
ls -lh ${output}

# For detailed manual for pod5, visit
# https://pod5-file-format.readthedocs.io/en/0.3.6/docs/tools.html#pod5-convert-fast5
