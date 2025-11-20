#!/bin/bash

# BAM to FASTQ conversion script for interactive session
# Usage: ./bam_to_fastq.sh

# Set number of parallel jobs (adjust based on your interactive session allocation)
NCPU=30

module load samtools

# Create output directory
mkdir -p fastq

echo "Starting BAM to FASTQ conversion..."
echo "Using $NCPU parallel jobs"
echo "Total BAM files: $(ls bam/*.bam | wc -l)"

# Method 1: Using GNU Parallel (if available)
if command -v parallel &> /dev/null; then
    echo "Using GNU Parallel..."
    ls bam/*.bam | parallel -j $NCPU 'echo "Converting {}..."; samtools fastq -T MM,ML {} | gzip > fastq/{/.}.fastq.gz'
    
# Method 2: Using xargs (fallback)
else
    echo "GNU Parallel not found, using xargs..."
    ls bam/*.bam | xargs -P $NCPU -I {} bash -c 'echo "Converting {}..."; samtools fastq -T MM,ML {} | gzip > fastq/$(basename {} .bam).fastq.gz'
fi

echo "Conversion complete!"
echo "Output files in fastq/ directory:"
ls fastq/*.fastq.gz | wc -l
