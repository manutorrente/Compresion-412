#!/bin/bash

# HDFS File Compression Script (Parent Orchestrator)
# Manages multiple child processes to compress files from landing routes

# Configuration
ROUTES_FILE="${1:-rutas_landing.txt}"
NUM_CHILD_PROCESSES="${2:-4}"
ALL_FILES_MODE=0
DAYS_BACK=7
MIN_FILE_SIZE_THRESHOLD=1048576  # 1MB minimum file size
EXCLUDED_EXTENSIONS="dat|gz|bz2|zip|rar|xz|lz4|zst|gzip"  # Extensions to skip

# Check for --all flag
for arg in "$@"; do
    if [ "$arg" = "--all" ]; then
        ALL_FILES_MODE=1
    fi
done

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
CHILD_SCRIPT="$SCRIPT_DIR/compress_hdfs_child.sh"

# Execution timestamp
EXEC_TIMESTAMP=$(date +%Y%m%d_%H%M%S)
WORK_DIR="$SCRIPT_DIR/compress_work_${EXEC_TIMESTAMP}"
MAIN_LOG="$WORK_DIR/compression_main.log"

# Function to log messages to main log
main_log() {
    echo "[$(date '+%Y-%m-%d %H:%M:%S')] $1" | tee -a "$MAIN_LOG"
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

# Validation
if [ ! -f "$ROUTES_FILE" ]; then
    echo "ERROR: Routes file '$ROUTES_FILE' not found"
    exit 1
fi

if [ ! -f "$CHILD_SCRIPT" ]; then
    echo "ERROR: Child script '$CHILD_SCRIPT' not found"
    exit 1
fi

if ! command -v hdfs &> /dev/null; then
    echo "ERROR: hdfs command not found. Ensure Hadoop is installed and configured."
    exit 1
fi

# Create working directory
mkdir -p "$WORK_DIR"
main_log "=========================================="
main_log "HDFS File Compression - Parent Process"
main_log "=========================================="
main_log "Execution timestamp: $EXEC_TIMESTAMP"
main_log "Working directory: $WORK_DIR"
main_log "Number of child processes: $NUM_CHILD_PROCESSES"
main_log "Routes file: $ROUTES_FILE"
main_log "File selection mode: $([ $ALL_FILES_MODE -eq 1 ] && echo 'ALL FILES' || echo 'Last 7 days')"
main_log "Child script: $CHILD_SCRIPT"
main_log "=========================================="

# Collect all filepaths from routes (files created in last 7 days)
main_log "Collecting filepaths from routes (last 7 days)..."
ALL_FILEPATHS_FILE="$WORK_DIR/all_filepaths.txt"
> "$ALL_FILEPATHS_FILE"  # Clear the file

while IFS= read -r route || [ -n "$route" ]; do
    # Skip empty lines and comments
    [[ -z "$route" || "$route" =~ ^[[:space:]]*# ]] && continue
    
    # Trim whitespace
    route=$(echo "$route" | xargs)
    
    main_log "  Scanning route: $route"
    
    # Find all files in the route modified in the last 7 days (or all if --all flag)
    if [ $ALL_FILES_MODE -eq 1 ]; then
        # Get all files - use ls recursively and extract paths with size filtering
        hdfs dfs -ls -R "$route" 2>/dev/null | grep "^-" | awk \
            -v min_size="$MIN_FILE_SIZE_THRESHOLD" \
            '
        {
            filepath = $NF
            size = $5
            
            # Skip files with excluded extensions
            if (filepath ~ /\.($EXCLUDED_EXTENSIONS)$/) {
                next
            }
            
            # Skip files smaller than threshold
            if (size < min_size) {
                next
            }
            
            print filepath
        }' >> "$ALL_FILEPATHS_FILE"
        
        file_count=$(wc -l < "$ALL_FILEPATHS_FILE" 2>/dev/null || echo 0)
        if [ "$file_count" -gt 0 ]; then
            main_log "    Found $file_count eligible files (after size and extension filters)"
        else
            main_log "    Warning: Could not scan route or no eligible files found"
        fi
    else
        # Get files modified in the last N days with size and extension filtering
        # Calculate cutoff timestamp (current time - DAYS_BACK days)
        cutoff_timestamp=$(($(date +%s) - (DAYS_BACK * 86400)))
        
        # Use awk to parse hdfs ls output and filter by date, size, and extension
        hdfs dfs -ls -R "$route" 2>/dev/null | grep "^-" | awk \
            -v cutoff="$cutoff_timestamp" \
            -v min_size="$MIN_FILE_SIZE_THRESHOLD" \
            -v excl_ext="$EXCLUDED_EXTENSIONS" \
            '
        {
            # Fields: perms repl owner group size month day time/year ... filepath
            filepath = $NF
            size = $5
            month = $6
            day = $7
            time_or_year = $8
            
            # Skip files with excluded extensions
            if (filepath ~ /\.($EXCLUDED_EXTENSIONS)$/) {
                next
            }
            
            # Skip files smaller than threshold
            if (size < min_size) {
                next
            }
            
            # Parse date string and convert to timestamp
            if (time_or_year ~ /^[0-9]{4}$/) {
                # Year format: older file
                datestr = month " " day " " time_or_year " 00:00"
            } else {
                # Time format: current year
                current_year = strftime("%Y", systime())
                datestr = month " " day " " current_year " " time_or_year
            }
            
            # Convert to timestamp using system command
            cmd = "date -d \"" datestr "\" +%s 2>/dev/null"
            if ((cmd | getline file_ts) > 0) {
                close(cmd)
                if (file_ts > cutoff) {
                    print filepath
                }
            } else {
                close(cmd)
            }
        }' >> "$ALL_FILEPATHS_FILE"
        
        file_count=$(wc -l < "$ALL_FILEPATHS_FILE" 2>/dev/null || echo 0)
        if [ "$file_count" -gt 0 ]; then
            main_log "    Found $file_count eligible files (after date, size, and extension filters)"
        else
            main_log "    Warning: Could not scan route or no eligible files found"
        fi
    fi
done < "$ROUTES_FILE"

total_files=$(wc -l < "$ALL_FILEPATHS_FILE")
main_log "Total files found: $total_files"

if [ "$total_files" -eq 0 ]; then
    main_log "No files found to compress. Exiting."
    exit 0
fi

# Divide filepaths into n files for child processes
main_log "Dividing filepaths into $NUM_CHILD_PROCESSES files..."
files_per_process=$(( (total_files + NUM_CHILD_PROCESSES - 1) / NUM_CHILD_PROCESSES ))
main_log "Approximate files per process: $files_per_process"

split -l "$files_per_process" "$ALL_FILEPATHS_FILE" "$WORK_DIR/filepaths_chunk_"
main_log "Filepaths split completed"

# Start child processes
main_log "=========================================="
main_log "Starting $NUM_CHILD_PROCESSES child processes"
main_log "=========================================="

CHILD_PIDS=()
PROC_NUM=0

for chunk_file in "$WORK_DIR"/filepaths_chunk_*; do
    if [ -f "$chunk_file" ]; then
        CHILD_LOG="$WORK_DIR/compression_proc_${PROC_NUM}.log"
        
        main_log "Starting child process $PROC_NUM with file: $(basename $chunk_file)"
        
        # Start child process in background
        PROCESS_LOG="$CHILD_LOG" PROCESS_NUM="$PROC_NUM" bash "$CHILD_SCRIPT" "$chunk_file" &
        
        CHILD_PIDS+=($!)
        PROC_NUM=$((PROC_NUM + 1))
    fi
done

main_log "All child processes started. Waiting for completion..."

# Wait for all child processes to complete
failed_processes=0
for i in "${!CHILD_PIDS[@]}"; do
    if wait "${CHILD_PIDS[$i]}"; then
        main_log "Child process $i completed successfully"
    else
        main_log "Child process $i failed with exit code $?"
        failed_processes=$((failed_processes + 1))
    fi
done

# Aggregate results from all child logs
main_log "=========================================="
main_log "AGGREGATING RESULTS"
main_log "=========================================="

total_processed=0
total_success=0
total_failed=0
total_bytes_saved=0

for proc_num in $(seq 0 $((PROC_NUM - 1))); do
    proc_log="$WORK_DIR/compression_proc_${proc_num}.log"
    
    if [ -f "$proc_log" ]; then
        # Extract structured results from child log (format: FINAL_RESULTS|key=value|key=value...)
        results_line=$(grep "^FINAL_RESULTS|" "$proc_log" 2>/dev/null | tail -1)
        
        if [ -n "$results_line" ]; then
            # Parse key=value pairs from the structured line
            processed=$(echo "$results_line" | grep -oP 'TOTAL=\K[0-9]+')
            success=$(echo "$results_line" | grep -oP 'SUCCESS=\K[0-9]+')
            failed=$(echo "$results_line" | grep -oP 'FAILED=\K[0-9]+')
            bytes_saved=$(echo "$results_line" | grep -oP 'BYTES_SAVED=\K[0-9]+')
        else
            # Fallback to old parsing method if structured format not found
            processed=$(grep "^Total files processed:" "$proc_log" 2>/dev/null | tail -1 | awk '{print $NF}')
            success=$(grep "^Successful:" "$proc_log" 2>/dev/null | tail -1 | awk '{print $NF}')
            failed=$(grep "^Failed:" "$proc_log" 2>/dev/null | tail -1 | awk '{print $NF}')
            bytes_saved=$(grep "^Total space saved:" "$proc_log" 2>/dev/null | tail -1 | grep -oE '[0-9]+' | head -1)
        fi
        
        processed=${processed:-0}
        success=${success:-0}
        failed=${failed:-0}
        bytes_saved=${bytes_saved:-0}
        
        main_log "Process $proc_num - Processed: $processed, Success: $success, Failed: $failed, Bytes saved: $bytes_saved"
        
        total_processed=$((total_processed + processed))
        total_success=$((total_success + success))
        total_failed=$((total_failed + failed))
        total_bytes_saved=$((total_bytes_saved + bytes_saved))
    fi
done

# Final summary
main_log "=========================================="
main_log "FINAL SUMMARY"
main_log "=========================================="
main_log "Total files processed: $total_processed"
main_log "Total files successfully compressed: $total_success"
main_log "Total files failed: $total_failed"
main_log "Total space saved: $(format_bytes $total_bytes_saved) ($total_bytes_saved bytes)"
main_log "Child processes that failed: $failed_processes"
main_log "=========================================="
main_log "Compression orchestration completed"

# Exit with success only if all child processes succeeded
if [ "$failed_processes" -eq 0 ] && [ "$total_failed" -eq 0 ]; then
    exit 0
else
    exit 1
fi
