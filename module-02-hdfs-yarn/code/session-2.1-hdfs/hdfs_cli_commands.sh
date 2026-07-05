#!/bin/bash
# =============================================================================
# Session 2.1 — HDFS CLI Commands Practice
# Big Data Engineering Training — Module 02
# =============================================================================
# Run these commands in your Hadoop cluster terminal (SSH into namenode or
# use the Hadoop container shell).
# Each section is independent — run block by block.
# =============================================================================


# -----------------------------------------------------------------------------
# PART 1: Check HDFS status and NameNode info
# -----------------------------------------------------------------------------

# Check overall HDFS health
hdfs dfsadmin -report

# Show NameNode status
hdfs dfsadmin -safemode get

# Check HDFS filesystem capacity
hdfs dfs -df -h


# -----------------------------------------------------------------------------
# PART 2: Directory Operations — mkdir, ls, chmod
# -----------------------------------------------------------------------------

# Create a project directory under HDFS
hdfs dfs -mkdir /user/training
hdfs dfs -mkdir /user/training/module02
hdfs dfs -mkdir /user/training/module02/raw_data
hdfs dfs -mkdir /user/training/module02/output

# List root directory
hdfs dfs -ls /

# List recursively
hdfs dfs -ls -R /user/training

# Change permissions (owner can read/write/execute, others read)
hdfs dfs -chmod 755 /user/training/module02

# Change ownership (if you have sudo/hdfs admin access)
# hdfs dfs -chown training:hadoop /user/training/module02


# -----------------------------------------------------------------------------
# PART 3: Upload files — put and copyFromLocal
# -----------------------------------------------------------------------------

# Upload the sample CSV from local to HDFS
hdfs dfs -put /home/training/sample_employees.csv /user/training/module02/raw_data/

# Verify the upload
hdfs dfs -ls /user/training/module02/raw_data/

# Alternative: copyFromLocal (same as -put)
# hdfs dfs -copyFromLocal /home/training/sample_employees.csv /user/training/module02/raw_data/employees_copy.csv

# Upload an entire local directory
# hdfs dfs -put /home/training/data/ /user/training/module02/raw_data/


# -----------------------------------------------------------------------------
# PART 4: Read / View file contents — cat, tail, text
# -----------------------------------------------------------------------------

# Print file contents (like Linux cat)
hdfs dfs -cat /user/training/module02/raw_data/sample_employees.csv

# View only first 5 lines (pipe with head)
hdfs dfs -cat /user/training/module02/raw_data/sample_employees.csv | head -5

# View last few lines
hdfs dfs -tail /user/training/module02/raw_data/sample_employees.csv

# Read compressed/sequence files as text
# hdfs dfs -text /user/training/module02/raw_data/data.snappy


# -----------------------------------------------------------------------------
# PART 5: Copy and Move files inside HDFS — cp, mv
# -----------------------------------------------------------------------------

# Copy file within HDFS
hdfs dfs -cp /user/training/module02/raw_data/sample_employees.csv \
             /user/training/module02/raw_data/employees_backup.csv

# Move/rename a file within HDFS
hdfs dfs -mv /user/training/module02/raw_data/employees_backup.csv \
             /user/training/module02/raw_data/employees_archive.csv

# Verify
hdfs dfs -ls /user/training/module02/raw_data/


# -----------------------------------------------------------------------------
# PART 6: Download files — get and copyToLocal
# -----------------------------------------------------------------------------

# Download from HDFS to local
hdfs dfs -get /user/training/module02/raw_data/sample_employees.csv \
              /home/training/downloaded_employees.csv

# Alternative: copyToLocal
# hdfs dfs -copyToLocal /user/training/module02/raw_data/sample_employees.csv \
#                        /home/training/employees_local.csv

# Verify local download
ls -lh /home/training/downloaded_employees.csv


# -----------------------------------------------------------------------------
# PART 7: File metadata and block info
# -----------------------------------------------------------------------------

# Show file size in human-readable format
hdfs dfs -du -h /user/training/module02/raw_data/

# Show summary of directory size
hdfs dfs -du -s -h /user/training/module02/

# Check block locations and replication info for a file
hdfs fsck /user/training/module02/raw_data/sample_employees.csv -files -blocks -locations

# Check replication factor of a specific file
hdfs dfs -stat "%r" /user/training/module02/raw_data/sample_employees.csv

# Change replication factor (e.g., set to 2)
hdfs dfs -setrep -w 2 /user/training/module02/raw_data/sample_employees.csv


# -----------------------------------------------------------------------------
# PART 8: Count files and check health
# -----------------------------------------------------------------------------

# Count files, directories, and bytes in a path
hdfs dfs -count /user/training/module02/

# Check HDFS file system integrity
hdfs fsck /user/training/module02/ -files -blocks


# -----------------------------------------------------------------------------
# PART 9: Delete files and directories — rm, rmdir
# -----------------------------------------------------------------------------

# Delete a specific file
hdfs dfs -rm /user/training/module02/raw_data/employees_archive.csv

# Delete directory and all contents (like rm -rf)
hdfs dfs -rm -r /user/training/module02/output

# Skip trash (permanent delete, use carefully)
hdfs dfs -rm -skipTrash /user/training/module02/raw_data/employees_archive.csv

# Empty the trash
# hdfs dfs -expunge


# -----------------------------------------------------------------------------
# PART 10: Quick Reference Summary
# -----------------------------------------------------------------------------
# hdfs dfs -ls <path>          → list files/directories
# hdfs dfs -mkdir <path>       → create directory
# hdfs dfs -put <local> <hdfs> → upload from local to HDFS
# hdfs dfs -get <hdfs> <local> → download from HDFS to local
# hdfs dfs -cat <path>         → print file content
# hdfs dfs -tail <path>        → last 1KB of file
# hdfs dfs -cp <src> <dst>     → copy within HDFS
# hdfs dfs -mv <src> <dst>     → move/rename within HDFS
# hdfs dfs -rm <path>          → delete file
# hdfs dfs -rm -r <path>       → delete directory recursively
# hdfs dfs -chmod <perm> <path>→ change permissions
# hdfs dfs -du -h <path>       → disk usage
# hdfs dfs -df -h              → filesystem capacity
# hdfs dfs -count <path>       → count files/dirs/bytes
# hdfs dfs -setrep <n> <path>  → set replication factor
# hdfs fsck <path>             → filesystem health check
# hdfs dfsadmin -report        → cluster status
