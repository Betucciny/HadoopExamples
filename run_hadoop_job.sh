#!/bin/bash

# SSH connection details
SSH_HOST="localhost"
SSH_USER="hadoop"
SSH_PASSWORD="123"
SSH_PORT="3022"

# Local path to the JAR file
LOCAL_JAR_FILE="build/hadoop/main.jar"

# Remote path where the JAR file will be copied on the virtual machine
REMOTE_JAR_PATH="/home/hadoop/hadoop_ssh/main.jar"

#Hadoop location
HADOOP_COMMAND="/home/hadoop/hadoop-3.3.5/bin/hadoop"

# Command to execute Hadoop job
HADOOP_JAR_COMMAND="$HADOOP_COMMAND jar $REMOTE_JAR_PATH $1 $2"

# Transfer the JAR file to the virtual machine using SCP
sshpass -p "$SSH_PASSWORD" scp -P "$SSH_PORT" "$LOCAL_JAR_FILE" "$SSH_USER"@"$SSH_HOST":"$REMOTE_JAR_PATH"

# Execute the Hadoop job via SSH on the virtual machine
sshpass -p "$SSH_PASSWORD" ssh -p "$SSH_PORT" "$SSH_USER"@"$SSH_HOST" "$HADOOP_JAR_COMMAND"

HADOOP_OUTPUT_DIR="$2"

echo "Reading the output of the Hadoop job"

sshpass -p "$SSH_PASSWORD" ssh -p "$SSH_PORT" "$SSH_USER"@"$SSH_HOST" "$HADOOP_COMMAND fs -ls $HADOOP_OUTPUT_DIR | grep '^-' | awk '{print \$8}' | while read -r file; do echo \"File: \$file\"; $HADOOP_COMMAND fs -cat \$file; echo \"----------------------------------------\"; done"
