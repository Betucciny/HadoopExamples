#!/bin/bash

# SSH connection details
SSH_HOST="localhost"
SSH_USER="betucciny"
SSH_PASSWORD="123"

# Command to execute Hadoop job
HADOOP_JAR_COMMAND="hadoop jar build/hadoop/main.jar"

# Command to execute Hadoop job
HADOOP_JAR_COMMAND="hadoop jar $HADOOP_JAR_COMMAND $@"

# Execute the Hadoop job via SSH
sshpass -p "$SSH_PASSWORD" ssh "$SSH_USER"@"$SSH_HOST" "$HADOOP_JAR_COMMAND"
