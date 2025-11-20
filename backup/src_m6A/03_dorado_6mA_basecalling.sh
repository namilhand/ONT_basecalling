#!/bin/bash
#SBATCH -J dorado_6mA_SAMseq5
#SBATCH -A HENDERSON-SL2-GPU
#SBATCH -p ampere
#SBATCH --nodes=1
#SBATCH --ntasks=1
#SBATCH --gres=gpu:4
#SBATCH --cpus-per-task=64
#SBATCH --time=24:00:00
#SBATCH --mem=200G
#SBATCH --array=1-6%1  # Replace N with number of chunks, %1 means process one at a time
#SBATCH -o logs/dorado_6mA_chunk_%A_%a.out
#SBATCH -e logs/dorado_6mA_chunk_%A_%a.err

# ==============================================================================
# SLURM Job Array Script for Dorado Basecalling with 6mA Modification Detection
# ==============================================================================
# FIXED: Proper validation for unaligned BAM files
# ==============================================================================

set -e  # Exit on error
set -u  # Exit on undefined variable

# Configuration
WORK_DIR="/home/ns2040/rds/rds-pollen-7KhEnGLnebA/namil/data/Leduque24_Fiber-seq/SAM-seq5"
POD5_CHUNKS_DIR="${WORK_DIR}/pod5_chunks"
OUTPUT_DIR="${WORK_DIR}/bam_chunks"
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
    
    # Check if we can read the BAM and it has reads
    set +e
    READ_COUNT=$(samtools view -c "${OUTPUT_BAM}" 2>/dev/null)
    CHECK_EXIT=$?
    set -e
    
    if [ $CHECK_EXIT -eq 0 ] && [ -n "$READ_COUNT" ] && [ $READ_COUNT -gt 100000 ]; then
        echo "Valid BAM file found with ${READ_COUNT} reads. Skipping this chunk."
        exit 0
    else
        echo "Existing BAM appears incomplete or invalid. Reprocessing..."
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
# IMPORTANT: Output redirection happens AFTER command completes
"${DORADO_BIN}" basecaller \
    --models-directory "${MODEL_DIR}" \
    --modified-bases 6mA \
    --no-trim \
    -x cuda:all \
    "${MODEL_NAME}" \
    "${CHUNK_FILE}" \
    > "${OUTPUT_BAM}"

# Capture dorado exit code
DORADO_EXIT=$?

# Check if basecalling was successful
if [ $DORADO_EXIT -ne 0 ]; then
    echo "ERROR: Basecalling failed for chunk ${SLURM_ARRAY_TASK_ID} (exit code: $DORADO_EXIT)"
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
    ACTUAL_SIZE=$(stat -f%z "${OUTPUT_BAM}" 2>/dev/null || stat -c%s "${OUTPUT_BAM}")
    echo "File size: ${FILE_SIZE} (${ACTUAL_SIZE} bytes)"
    
    # For unaligned BAM files, samtools quickcheck complains about missing targets
    # Use samtools view -c to verify instead (works with unaligned BAMs)
    echo "Verifying BAM file (unaligned, no reference sequence)..."
    
    # Disable exit-on-error temporarily for validation
    set +e
    READ_COUNT=$(samtools view -c "${OUTPUT_BAM}" 2>/dev/null)
    VALIDATION_EXIT=$?
    set -e
    
    if [ $VALIDATION_EXIT -eq 0 ] && [ -n "$READ_COUNT" ]; then
        echo "✓ BAM file is valid (unaligned)"
        echo "✓ Number of reads in BAM: ${READ_COUNT}"
        
        # Sanity check - should be close to 1M reads per chunk
        if [ $READ_COUNT -lt 100000 ]; then
            echo "⚠️  WARNING: Fewer reads than expected (${READ_COUNT})"
            echo "   Check if basecalling filtered out many reads"
        fi
        
        # Check file size is reasonable (should be > 100MB for 1M reads)
        if [ $ACTUAL_SIZE -lt 100000000 ]; then
            echo "⚠️  WARNING: File size is unusually small (${FILE_SIZE})"
            echo "   Expected >100MB for ~1M reads"
        fi
    else
        echo "✗ ERROR: Cannot read BAM file - may be corrupt"
        echo "   samtools exit code: $VALIDATION_EXIT"
        exit 1
    fi
else
    echo "✗ ERROR: Output BAM file not created"
    exit 1
fi

echo ""
echo "========================================================================"
echo "Task ${SLURM_ARRAY_TASK_ID} completed successfully!"
echo "========================================================================"
