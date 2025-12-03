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

# Check for --all flag and handle argument parsing
for arg in "$@"; do
    if [ "$arg" = "--all" ]; then
        ALL_FILES_MODE=1
    elif [ "$arg" != "$ROUTES_FILE" ] && [ "$arg" != "$NUM_CHILD_PROCESSES" ]; then
        # If it looks like a number, use it as NUM_CHILD_PROCESSES
        if [[ "$arg" =~ ^[0-9]+$ ]]; then
            NUM_CHILD_PROCESSES="$arg"
        fi
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
    
    # Debug: check if route exists
    if ! hdfs dfs -ls "$route" &>/dev/null; then
        main_log "    Debug: Route does not exist or not accessible: $route"
        continue
    fi
    main_log "    Debug: Route is accessible"
    
    # Find all files in the route modified in the last 7 days (or all if --all flag)
    if [ $ALL_FILES_MODE -eq 1 ]; then
        # Get all files - use ls recursively and extract paths with size filtering
        main_log "    Debug: Mode=ALL, collecting all files >= $MIN_FILE_SIZE_THRESHOLD bytes"
        
        file_before=$(wc -l < "$ALL_FILEPATHS_FILE" 2>/dev/null || echo 0)
        
        # Use temporary file to avoid subshell issues with pipes
        hdfs_ls_temp=$(mktemp)
        hdfs dfs -ls -R "$route" 2>/dev/null | grep "^-" > "$hdfs_ls_temp"
        
        while read -r line; do
            # Parse the line
            size=$(echo "$line" | awk '{print $5}')
            filepath=$(echo "$line" | awk '{print $NF}')
            
            # Skip files with excluded extensions
            if [[ "$filepath" =~ \.(dat|gz|bz2|zip|rar|xz|lz4|zst|gzip)$ ]]; then
                continue
            fi
            
            # Skip files smaller than threshold
            if [ "$size" -lt "$MIN_FILE_SIZE_THRESHOLD" ]; then
                continue
            fi
            
            echo "$filepath" >> "$ALL_FILEPATHS_FILE"
        done < "$hdfs_ls_temp"
        
        rm -f "$hdfs_ls_temp"
        
        file_after=$(wc -l < "$ALL_FILEPATHS_FILE" 2>/dev/null || echo 0)
        files_added=$((file_after - file_before))
        main_log "    Debug: Added $files_added files from this route"
        
        if [ "$files_added" -gt 0 ]; then
            main_log "    Found $files_added eligible files (after size and extension filters)"
        else
            main_log "    Warning: Could not scan route or no eligible files found"
        fi
    else
        # For date filtering, first collect all eligible files by size/extension, then filter by date
        # This is simpler and more reliable than trying to parse dates in awk
        cutoff_timestamp=$(($(date +%s) - (DAYS_BACK * 86400)))
        cutoff_date=$(date -d @"$cutoff_timestamp" '+%Y-%m-%d %H:%M:%S')
        main_log "    Debug: Mode=DATE (last $DAYS_BACK days), cutoff=$cutoff_date"
        
        file_before=$(wc -l < "$ALL_FILEPATHS_FILE" 2>/dev/null || echo 0)
        debug_parsed=0
        debug_skipped_ext=0
        debug_skipped_size=0
        debug_skipped_date=0
        debug_collected=0
        
        # Use temporary file to avoid subshell issues with pipes
        hdfs_ls_temp=$(mktemp)
        hdfs dfs -ls -R "$route" 2>/dev/null | grep "^-" > "$hdfs_ls_temp"
        
        while read -r line; do
            # Parse the line
            size=$(echo "$line" | awk '{print $5}')
            month=$(echo "$line" | awk '{print $6}')
            day=$(echo "$line" | awk '{print $7}')
            time_or_year=$(echo "$line" | awk '{print $8}')
            filepath=$(echo "$line" | awk '{print $NF}')
            
            debug_parsed=$((debug_parsed + 1))
            
            # Skip files with excluded extensions
            if [[ "$filepath" =~ \.(dat|gz|bz2|zip|rar|xz|lz4|zst|gzip)$ ]]; then
                debug_skipped_ext=$((debug_skipped_ext + 1))
                continue
            fi
            
            # Skip files smaller than threshold
            if [ "$size" -lt "$MIN_FILE_SIZE_THRESHOLD" ]; then
                debug_skipped_size=$((debug_skipped_size + 1))
                continue
            fi
            
            # Convert file timestamp
            if [[ "$time_or_year" =~ ^[0-9]{4}$ ]]; then
                # Year format: older file
                file_ts=$(date -d "$month $day $time_or_year" +%s 2>/dev/null || echo 0)
            else
                # Time format: current year - parse as HH:MM
                current_year=$(date +%Y)
                file_ts=$(date -d "$month $day $current_year $time_or_year" +%s 2>/dev/null || echo 0)
            fi
            
            # Add file if it's within cutoff
            if [ "$file_ts" -gt "$cutoff_timestamp" ]; then
                echo "$filepath" >> "$ALL_FILEPATHS_FILE"
                debug_collected=$((debug_collected + 1))
            else
                debug_skipped_date=$((debug_skipped_date + 1))
            fi
        done < "$hdfs_ls_temp"
        
        rm -f "$hdfs_ls_temp"
        
        file_after=$(wc -l < "$ALL_FILEPATHS_FILE" 2>/dev/null || echo 0)
        files_added=$((file_after - file_before))
        
        main_log "    Debug: Parsed=$debug_parsed, SkippedExt=$debug_skipped_ext, SkippedSize=$debug_skipped_size, SkippedDate=$debug_skipped_date, Collected=$debug_collected"
        
        # Debug: show first file's date parsing
        if [ "$debug_parsed" -gt 0 ]; then
            first_line=$(head -1 "$hdfs_ls_temp" 2>/dev/null)
            if [ -n "$first_line" ]; then
                first_month=$(echo "$first_line" | awk '{print $6}')
                first_day=$(echo "$first_line" | awk '{print $7}')
                first_time=$(echo "$first_line" | awk '{print $8}')
                first_file=$(echo "$first_line" | awk '{print $NF}')
                if [[ "$first_time" =~ ^[0-9]{4}$ ]]; then
                    sample_ts=$(date -d "$first_month $first_day $first_time" +%s 2>/dev/null || echo "PARSE_ERROR")
                else
                    current_year=$(date +%Y)
                    sample_ts=$(date -d "$first_month $first_day $current_year $first_time" +%s 2>/dev/null || echo "PARSE_ERROR")
                fi
                sample_date=$(date -d @"$sample_ts" '+%Y-%m-%d %H:%M:%S' 2>/dev/null || echo "INVALID")
                main_log "    Debug: Sample file date parsing: '$first_month $first_day $first_time' -> $sample_date (ts=$sample_ts, cutoff=$cutoff_timestamp)"
                main_log "    Debug: Sample file: $first_file"
            fi
        fi
        
        if [ "$files_added" -gt 0 ]; then
            main_log "    Found $files_added eligible files (after date, size, and extension filters)"
        else
            main_log "    Warning: Could not scan route or no eligible files found"
        fi
    fi
done < "$ROUTES_FILE"

total_files=$(wc -l < "$ALL_FILEPATHS_FILE")
main_log "Total files found: $total_files"

# Debug: show sample of collected files
if [ "$total_files" -gt 0 ]; then
    main_log "Debug: Sample collected files (first 5):"
    head -5 "$ALL_FILEPATHS_FILE" | while read -r file; do
        main_log "  - $file"
    done
fi

if [ "$total_files" -eq 0 ]; then
    main_log "No files found to compress. Exiting."
    exit 0
fi

# Divide filepaths into n files for child processes
main_log "Dividing filepaths into $NUM_CHILD_PROCESSES files..."
files_per_process=$(( (total_files + NUM_CHILD_PROCESSES - 1) / NUM_CHILD_PROCESSES ))
main_log "Approximate files per process: $files_per_process"

split -l "$files_per_process" "$ALL_FILEPATHS_FILE" "$WORK_DIR/filepaths_chunk_"

# Debug: verify chunks were created
chunk_count=$(ls "$WORK_DIR"/filepaths_chunk_* 2>/dev/null | wc -l)
main_log "Debug: Split created $chunk_count chunk files"
for chunk in "$WORK_DIR"/filepaths_chunk_*; do
    chunk_lines=$(wc -l < "$chunk")
    main_log "Debug: $(basename $chunk) contains $chunk_lines files"
done

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
