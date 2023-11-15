#!/bin/bash

CITUS_EXEC_PATH=$1
CPU_TYPE=$(srun dpkg --print-architecture)
if [ $CPU_TYPE = "arm64" ]
then 
    CITUS_EXEC_PATH=$CITUS_EXEC_PATH/"citus-arm64"
elif [ $CPU_TYPE = "amd64" ]
then
    CITUS_EXEC_PATH=$CITUS_EXEC_PATH/"citus-amd64"
else 
	echo "unrecognized cpu type $CpuType"
	exit 1
fi

srun $CITUS_EXEC_PATH $@
