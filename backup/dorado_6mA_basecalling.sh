#!/bin/bash
#SBATCH -J dorado_6mA_SAMseq5
#SBATCH -A HENDERSON-SL3-GPU
#SBATCH -p ampere
#SBATCH --nodes=1
#SBATCH --ntasks=1
#SBATCH --gres=gpu:4
#SBATCH --cpus-per-task=64
#SBATCH --time=12:00:00
#SBATCH --mem=200G
#SBATCH --array=1-6%1  # Replace N with number of chunks, %1 means process one at a time
#SBATCH -o logs/dorado_6mA_chunk_%A_%a.out
#SBATCH -e logs/dorado_6mA_chunk_%A_%a.err

# ==============================================================================
# SLURM Job Array Script for Dorado Basecalling with 6mA Modification Detection
# ==============================================================================
# This script processes POD5 chunks sequentially using SLURM job arrays.
# Each chunk is processed one at a time to ensure resilience to failures.
# 
# INSTRUCTIONS:
# 1. First, run split_pod5.py to create chunks:
#    python split_pod5.sh SAM-seq5.pod5
#
# 2. Count the number of chunks created and update --array parameter above:
#    ls pod5_chunks/*.pod5 | wc -l
#    Replace N in --array=1-N%1 with the number of chunks
#
# 3. Create logs directory:
#    mkdir -p logs
#
# 4. Submit the job:
#    sbatch dorado_basecalling_array.sh
# ==============================================================================

set -e  # Exit on error
set -u  # Exit on undefined variable

# Configuration
WORK_DIR="/home/ns2040/rds/rds-pollen-7KhEnGLnebA/namil/data/Leduque24_Fiber-seq/SAM-seq5"
POD5_CHUNKS_DIR="${WORK_DIR}/pod5_chunks"
OUTPUT_DIR="${WORK_DIR}/bam_chunks"
FINAL_OUTPUT="${WORK_DIR}/SAM-seq5_sup_6mA.bam"
DORADO_BIN="${HOME}/bin/dorado"
MODEL_DIR="${HOME}/.cache/dorado/models"
MODEL_NAME="dna_r10.4.1_e8.2_400bps_sup@v5.2.0"
KIT_NAME="SQK-LSK109"

# Activate environment
source ~/miniforge3/etc/profile.d/conda.sh
conda activate myenv
module load samtools

# Create output directory if it doesn't exist
mkdir -p "${OUTPUT_DIR}"

# Get the chunk file for this array task
CHUNK_FILES=(${POD5_CHUNKS_DIR}/*.pod5)
CHUNK_FILE="${CHUNK_FILES[$((SLURM_ARRAY_TASK_ID-1))]}"
CHUNK_BASENAME=$(basename "${CHUNK_FILE}" .pod5)
OUTPUT_BAM="${OUTPUT_DIR}/${CHUNK_BASENAME}_sup_6mA.bam"

echo "========================================================================"
echo "Job Array ID: ${SLURM_ARRAY_JOB_ID}"
echo "Task ID: ${SLURM_ARRAY_TASK_ID}"
echo "Chunk file: ${CHUNK_FILE}"
echo "Output BAM: ${OUTPUT_BAM}"
echo "========================================================================"

# Check if output already exists (for restarting failed jobs)
if [ -f "${OUTPUT_BAM}" ]; then
    echo "Output file already exists. Checking validity..."
    if samtools quickcheck "${OUTPUT_BAM}" 2>/dev/null; then
        echo "Valid BAM file found. Skipping this chunk."
        exit 0
    else
        echo "Invalid BAM file found. Reprocessing..."
        rm -f "${OUTPUT_BAM}"
    fi
fi

# Check if chunk file exists
if [ ! -f "${CHUNK_FILE}" ]; then
    echo "ERROR: Chunk file not found: ${CHUNK_FILE}"
    exit 1
fi

# Display GPU information
echo ""
echo "GPU Information:"
nvidia-smi
echo ""

# Start timing
START_TIME=$(date +%s)
echo "Starting basecalling at: $(date)"
echo ""

# Run dorado basecaller
# The -x cuda:all option will use all 4 GPUs
"${DORADO_BIN}" basecaller \
    --models-directory "${MODEL_DIR}" \
    --modified-bases 6mA \
    --no-trim \
    -x cuda:all \
    "${MODEL_NAME}" \
    "${CHUNK_FILE}" \
    > "${OUTPUT_BAM}"

#    --kit-name "${KIT_NAME}" \

# Check if basecalling was successful
if [ $? -ne 0 ]; then
    echo "ERROR: Basecalling failed for chunk ${SLURM_ARRAY_TASK_ID}"
    exit 1
fi

# End timing
END_TIME=$(date +%s)
ELAPSED=$((END_TIME - START_TIME))
HOURS=$((ELAPSED / 3600))
MINUTES=$(((ELAPSED % 3600) / 60))
SECONDS=$((ELAPSED % 60))

echo ""
echo "========================================================================"
echo "Basecalling completed at: $(date)"
echo "Elapsed time: ${HOURS}h ${MINUTES}m ${SECONDS}s"
echo "========================================================================"

# Verify output
if [ -f "${OUTPUT_BAM}" ]; then
    echo "Output file created successfully"
    FILE_SIZE=$(du -h "${OUTPUT_BAM}" | cut -f1)
    echo "File size: ${FILE_SIZE}"
    
    if samtools quickcheck "${OUTPUT_BAM}"; then
        echo "BAM file is valid"
        
        # Get read count
        READ_COUNT=$(samtools view -c "${OUTPUT_BAM}")
        echo "Number of reads in BAM: ${READ_COUNT}"
    else
        echo "ERROR: BAM file is invalid"
        exit 1
    fi
else
    echo "ERROR: Output BAM file not created"
    exit 1
fi

echo ""
echo "========================================================================"
echo "Task ${SLURM_ARRAY_TASK_ID} completed successfully!"
echo "========================================================================"
