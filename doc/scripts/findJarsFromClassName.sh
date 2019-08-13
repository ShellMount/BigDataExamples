#!/bin/bash


if [ $# -lt 2 ]; then
    echo "Usage ./findJarsFromClassName.sh /path/to/directory org.apache.xxx.yyy.zzz"
    exit
fi

find $1 -type f | grep jar$ | while read path
do
    if [[ $path =~ jar$ ]]; then
        jar -tvf $path | grep -i ${2/\./\/}
        if [ $? -eq 0 ]; then
	   echo "---> "  $path
	fi
    fi
done

