#!/bin/bash

# ==============================================================================
# POD5 Split - Keep Only Non-Duplicated Reads
# ==============================================================================
# Strategy: Only process reads that appear EXACTLY ONCE in the original file
# This avoids the duplicate issue entirely
# ==============================================================================

# To keep log, (not sure if this redirection correct)
# bash ${input_pod5} ${reads_per_chunk} ${output_dir} 2>&1 | tee pod5_split.log

set -e
set -u

INPUT_POD5="${1:-SAM-seq5.pod5}"
READS_PER_CHUNK="${2:-1000000}"
OUTPUT_DIR="${3:-pod5_chunks}"

echo "========================================================================"
echo "POD5 Split - Non-Duplicated Reads Only"
echo "========================================================================"
echo "Input: $INPUT_POD5"
echo "Output: $OUTPUT_DIR"
echo "Strategy: Only keep reads that appear EXACTLY once"
echo "========================================================================"
echo ""

BASENAME=$(basename "$INPUT_POD5" .pod5)
TEMP_DIR=".temp_nodups_$$"
mkdir -p "$TEMP_DIR"
mkdir -p "$OUTPUT_DIR"

# Step 1: Find read IDs that appear EXACTLY once
echo "[1/3] Analyzing read ID frequencies..."
ALL_IDS="${TEMP_DIR}/all_ids.txt"
pod5 view "$INPUT_POD5" --ids --no-header > "$ALL_IDS"

TOTAL=$(wc -l < "$ALL_IDS")
echo "  Total read entries: $(printf "%'d" $TOTAL)"

echo "  Finding reads that appear exactly once..."
# This awk command counts occurrences and only prints IDs that appear once
SINGLETON_IDS="${TEMP_DIR}/singleton_ids.txt"

awk '{count[$1]++} END {
    for (id in count) {
        if (count[id] == 1) {
            print id
        }
    }
}' "$ALL_IDS" | sort > "$SINGLETON_IDS"

SINGLETON_COUNT=$(wc -l < "$SINGLETON_IDS")
DUPLICATE_READS=$((TOTAL - SINGLETON_COUNT))

echo "  Reads appearing exactly once: $(printf "%'d" $SINGLETON_COUNT)"
echo "  Reads to be excluded (appear >1 time): $(printf "%'d" $DUPLICATE_READS)"
echo ""

# Find which IDs are duplicated (for reporting)
echo "  Identifying duplicated read IDs..."
DUPLICATE_IDS="${TEMP_DIR}/duplicate_ids.txt"

awk '{count[$1]++; ids[$1]} END {
    for (id in count) {
        if (count[id] > 1) {
            print id, count[id]
        }
    }
}' "$ALL_IDS" | sort > "$DUPLICATE_IDS"

NUM_DUP_IDS=$(wc -l < "$DUPLICATE_IDS")
echo "  Number of unique read IDs that are duplicated: $(printf "%'d" $NUM_DUP_IDS)"

echo ""
echo "  Sample of duplicated read IDs (first 10):"
head -10 "$DUPLICATE_IDS" | while read id count; do
    echo "    $id (appears $count times - will be excluded)"
done

echo ""
echo "Proceeding with $SINGLETON_COUNT non-duplicated reads..."
echo ""

# Step 2: Split singleton IDs into final chunk batches
echo "[2/3] Creating batches of ${READS_PER_CHUNK} reads..."

split -l $READS_PER_CHUNK -d -a 3 "$SINGLETON_IDS" "${TEMP_DIR}/chunk_"

CHUNK_FILES=(${TEMP_DIR}/chunk_*)
NUM_CHUNKS=${#CHUNK_FILES[@]}

echo "  Created $NUM_CHUNKS chunks"
echo ""

# Step 3: Extract reads directly to final chunks
echo "[3/3] Extracting reads into final chunks..."
echo ""

CHUNK_NUM=0
START_TIME=$(date +%s)

for CHUNK_FILE in "${CHUNK_FILES[@]}"; do
    CHUNK_NUM=$((CHUNK_NUM + 1))
    CHUNK_SUFFIX=$(basename "$CHUNK_FILE" | sed 's/chunk_//')

    NUM_READS=$(wc -l < "$CHUNK_FILE")
    PERCENT=$((CHUNK_NUM * 100 / NUM_CHUNKS))

    printf "  [%3d%%] Chunk %03d/%03d (%'d reads)... " \
           $PERCENT $CHUNK_NUM $NUM_CHUNKS $NUM_READS

    # Create CSV mapping directly to final output
    MAPPING="${TEMP_DIR}/map_${CHUNK_SUFFIX}.csv"
    FINAL_OUTPUT="${OUTPUT_DIR}/${BASENAME}_chunk${CHUNK_SUFFIX}.pod5"
    OUTPUT_NAME=$(basename "$FINAL_OUTPUT")

    echo "target,read_id" > "$MAPPING"
    while read -r read_id; do
        echo "${OUTPUT_NAME},${read_id}" >> "$MAPPING"
    done < "$CHUNK_FILE"

    # Run pod5 subset directly to final output
    if timeout 600 pod5 subset "$INPUT_POD5" \
           --csv "$MAPPING" \
           --output "$OUTPUT_DIR" 2>&1 | grep -qi "error\|duplicate\|segmentation"; then
        echo "FAILED"
        echo "  Unexpected error - check if input file has issues"
        rm -rf "$TEMP_DIR"
        exit 1
    fi

    if [ -f "$FINAL_OUTPUT" ]; then
        # Verify
        CHUNK_TOTAL=$(pod5 view "$FINAL_OUTPUT" --ids --no-header 2>/dev/null | wc -l)
        CHUNK_UNIQUE=$(pod5 view "$FINAL_OUTPUT" --ids --no-header 2>/dev/null | sort -u | wc -l)
        SIZE=$(du -h "$FINAL_OUTPUT" | cut -f1)

        if [ $CHUNK_TOTAL -eq $CHUNK_UNIQUE ] && [ $CHUNK_TOTAL -eq $NUM_READS ]; then
            echo "✓ Perfect (${SIZE}, $(printf "%'d" $CHUNK_TOTAL) reads)"
        elif [ $CHUNK_TOTAL -eq $CHUNK_UNIQUE ]; then
            echo "OK (${SIZE}, $(printf "%'d" $CHUNK_TOTAL) reads, expected $(printf "%'d" $NUM_READS))"
        else
            echo "WARNING (${SIZE}, $(printf "%'d" $CHUNK_TOTAL) total, $(printf "%'d" $CHUNK_UNIQUE) unique)"
        fi
    else
        echo "FAILED (no output)"
        rm -rf "$TEMP_DIR"
        exit 1
    fi

    rm -f "$MAPPING"

    # Progress estimate
    if [ $((CHUNK_NUM % 5)) -eq 0 ] && [ $CHUNK_NUM -lt $NUM_CHUNKS ]; then
        CURRENT=$(date +%s)
        ELAPSED=$((CURRENT - START_TIME))
        AVG=$((ELAPSED / CHUNK_NUM))
        REMAIN=$((NUM_CHUNKS - CHUNK_NUM))
        EST=$((AVG * REMAIN / 60))

        if [ $EST -gt 0 ]; then
            echo "       (Est. remaining: ${EST} min)"
        fi
    fi
done

EXTRACTION_TIME=$(date +%s)
TOTAL_ELAPSED=$((EXTRACTION_TIME - START_TIME))

echo ""
echo "  Extraction time: $((TOTAL_ELAPSED / 60))m $((TOTAL_ELAPSED % 60))s"

# Cleanup
echo ""
echo "Cleaning up temporary files..."
rm -rf "$TEMP_DIR"

echo ""
echo "========================================================================"
echo "COMPLETE!"
echo "========================================================================"
echo "Total time: $((TOTAL_ELAPSED / 60))m $((TOTAL_ELAPSED % 60))s"
echo ""

NUM_CHUNKS=$(ls "$OUTPUT_DIR"/${BASENAME}_chunk*.pod5 2>/dev/null | wc -l)
echo "Results:"
echo "  Input file: $INPUT_POD5"
echo "  Total reads in input: $(printf "%'d" $TOTAL)"
echo "  Singleton reads (used): $(printf "%'d" $SINGLETON_COUNT)"
echo "  Duplicated reads (excluded): $(printf "%'d" $DUPLICATE_READS)"
echo "  Output chunks created: $NUM_CHUNKS"
echo ""

# Final verification
echo "Final chunk verification:"
TOTAL_OUTPUT=0
ALL_CLEAN=1

for chunk in "$OUTPUT_DIR"/${BASENAME}_chunk*.pod5; do
    if [ -f "$chunk" ]; then
        CHUNK_TOTAL=$(pod5 view "$chunk" --ids --no-header 2>/dev/null | wc -l)
        CHUNK_UNIQUE=$(pod5 view "$chunk" --ids --no-header 2>/dev/null | sort -u | wc -l)
        CHUNK_NAME=$(basename "$chunk")
        SIZE=$(du -h "$chunk" | cut -f1)
        
        TOTAL_OUTPUT=$((TOTAL_OUTPUT + CHUNK_TOTAL))
        
        if [ $CHUNK_TOTAL -eq $CHUNK_UNIQUE ]; then
            printf "  ✓ %-35s %8s  %'10d unique reads\n" "$CHUNK_NAME" "$SIZE" "$CHUNK_TOTAL"
        else
            printf "  ✗ %-35s %8s  %'10d total, %'10d unique (ERROR!)\n" \
                   "$CHUNK_NAME" "$SIZE" "$CHUNK_TOTAL" "$CHUNK_UNIQUE"
            ALL_CLEAN=0
        fi
    fi
done

echo ""
echo "Total reads in all chunks: $(printf "%'d" $TOTAL_OUTPUT)"

if [ $ALL_CLEAN -eq 1 ] && [ $TOTAL_OUTPUT -eq $SINGLETON_COUNT ]; then
    echo ""
    echo "✓✓✓ SUCCESS! All chunks are clean with only non-duplicated reads ✓✓✓"
    echo ""
    echo "========================================================================"
    echo "Next steps:"
    echo "  1. Update dorado_basecalling_array.sh:"
    echo "     #SBATCH --array=1-${NUM_CHUNKS}%1"
    echo ""
    echo "  2. Submit basecalling:"
    echo "     sbatch dorado_basecalling_array.sh"
    echo ""
    echo "Note: Basecalling will use $(printf "%'d" $SINGLETON_COUNT) reads"
    echo "      ($(printf "%'d" $DUPLICATE_READS) duplicated reads were excluded)"
    echo "========================================================================"
else
    echo ""
    echo "⚠️  WARNING: Issues detected in output"
    
    if [ $ALL_CLEAN -eq 0 ]; then
        echo "  - Some chunks contain duplicates"
    fi
    
    if [ $TOTAL_OUTPUT -ne $SINGLETON_COUNT ]; then
        echo "  - Read count mismatch"
    fi
fi
