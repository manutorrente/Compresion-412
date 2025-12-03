#!/bin/bash

# HDFS File Compression Script (Child Process)
# Compresses files listed in a text file, verifies success, and removes originals

# Configuration
INPUT_FILE="${1:-hdfs_paths.txt}"
PROCESS_NUM="${PROCESS_NUM:-0}"
LOG_FILE="${PROCESS_LOG:-compression_$(date +%Y%m%d_%H%M%S).log}"
TEMP_CHECK_FILE="/tmp/hdfs_compress_check_$"

# Logging function with process identifier
log() {
    echo "[$(date '+%Y-%m-%d %H:%M:%S')] [PROC-$PROCESS_NUM] $1" >> "$LOG_FILE"
}

# Error logging function with process identifier
log_error() {
    echo "[$(date '+%Y-%m-%d %H:%M:%S')] [PROC-$PROCESS_NUM] ERROR: $1" >> "$LOG_FILE"
}

# Function to format bytes as human-readable
format_bytes() {
    local bytes=$1
    if [ "$bytes" -lt 1024 ]; then
        echo "${bytes}B"
    elif [ "$bytes" -lt 1048576 ]; then
        echo "$(echo "scale=2; $bytes / 1024" | bc 2>/dev/null || echo "N/A")KB"
    elif [ "$bytes" -lt 1073741824 ]; then
        echo "$(echo "scale=2; $bytes / 1048576" | bc 2>/dev/null || echo "N/A")MB"
    else
        echo "$(echo "scale=2; $bytes / 1073741824" | bc 2>/dev/null || echo "N/A")GB"
    fi
}

# Check if input file exists
if [ ! -f "$INPUT_FILE" ]; then
    log_error "Input file '$INPUT_FILE' not found"
    exit 1
fi

log "Starting compression process"
log "Input file: $INPUT_FILE"
log "Log file: $LOG_FILE"
log "Process number: $PROCESS_NUM"
log "----------------------------------------"

# Statistics
TOTAL=0
SUCCESS=0
FAILED=0
TOTAL_SAVED_BYTES=0

# Process each HDFS path
while IFS= read -r hdfs_path || [ -n "$hdfs_path" ]; do
    # Skip empty lines and comments
    [[ -z "$hdfs_path" || "$hdfs_path" =~ ^[[:space:]]*# ]] && continue
    
    # Trim whitespace
    hdfs_path=$(echo "$hdfs_path" | xargs)
    
    TOTAL=$((TOTAL + 1))
    log "Processing ($TOTAL): $hdfs_path"
    
    # Define compressed file path
    compressed_path="${hdfs_path}.gz"
    
    # Step 1: Verify file exists (in case it was deleted between parent scan and child execution)
    if ! hdfs dfs -ls "$hdfs_path" &>/dev/null; then
        log_error "Source file does not exist: $hdfs_path"
        FAILED=$((FAILED + 1))
        continue
    fi
    
    # Step 2: Check if compressed file already exists
    if hdfs dfs -ls "$compressed_path" &>/dev/null; then
        log_error "Compressed file already exists: $compressed_path"
        FAILED=$((FAILED + 1))
        continue
    fi
    
    # Step 3: Compress using streaming with pipes
    log "  Compressing to: $compressed_path"
    if hdfs dfs -cat "$hdfs_path" | gzip | hdfs dfs -put - "$compressed_path" 2>>"$LOG_FILE"; then
        log "  Compression completed"
    else
        log_error "Compression failed for: $hdfs_path"
        FAILED=$((FAILED + 1))
        continue
    fi
    
    # Step 4: Verify compressed file exists and has content
    log "  Verifying compressed file..."
    if ! hdfs dfs -ls "$compressed_path" &>/dev/null; then
        log_error "Verification failed: compressed file not found: $compressed_path"
        FAILED=$((FAILED + 1))
        continue
    fi
    
    # Check if compressed file has content (size > 0)
    compressed_size=$(hdfs dfs -du -s "$compressed_path" 2>/dev/null | awk '{print $1}')
    if [ -z "$compressed_size" ] || [ "$compressed_size" -eq 0 ]; then
        log_error "Verification failed: compressed file is empty: $compressed_path"
        FAILED=$((FAILED + 1))
        continue
    fi
    
    # Step 5: Additional verification - test gzip integrity
    log "  Testing gzip integrity..."
    if hdfs dfs -cat "$compressed_path" | gzip -t 2>>"$LOG_FILE"; then
        log "  Gzip integrity check passed"
    else
        log_error "Gzip integrity check failed for: $compressed_path"
        FAILED=$((FAILED + 1))
        continue
    fi
    
    # Step 6: Get original file size for logging
    original_size=$(hdfs dfs -du -s "$hdfs_path" 2>/dev/null | awk '{print $1}')
    compression_ratio=$(echo "scale=2; ($original_size - $compressed_size) * 100 / $original_size" | bc 2>/dev/null || echo "N/A")
    space_saved=$((original_size - compressed_size))
    
    log "  Original size: $original_size bytes"
    log "  Compressed size: $compressed_size bytes"
    log "  Space saved: $space_saved bytes (${compression_ratio}%)"
    
    # Step 7: Delete original file only after all verifications pass
    log "  Deleting original file..."
    if hdfs dfs -rm "$hdfs_path" 2>>"$LOG_FILE"; then
        log "  SUCCESS: Original file deleted"
        SUCCESS=$((SUCCESS + 1))
        TOTAL_SAVED_BYTES=$((TOTAL_SAVED_BYTES + space_saved))
        log " Total space saved so far: $(format_bytes $TOTAL_SAVED_BYTES) ($TOTAL_SAVED_BYTES bytes)"
    else
        log_error "Failed to delete original file: $hdfs_path"
        log_error "Compressed file exists at: $compressed_path"
        FAILED=$((FAILED + 1))
    fi
    
    # Remove the processed line from the partition file (always, regardless of success/failure)
    log "  Removing processed file from partition..."
    grep -vF "$hdfs_path" "$INPUT_FILE" > "${INPUT_FILE}.tmp" && mv "${INPUT_FILE}.tmp" "$INPUT_FILE"
    
    log "----------------------------------------"
    
done < "$INPUT_FILE"

# Final summary with structured output for easy parsing
log "Compression process completed"
log "FINAL_RESULTS|PROCESS=$PROCESS_NUM|TOTAL=$TOTAL|SUCCESS=$SUCCESS|FAILED=$FAILED|BYTES_SAVED=$TOTAL_SAVED_BYTES|BYTES_SAVED_FORMATTED=$(format_bytes $TOTAL_SAVED_BYTES)"

exit 0
