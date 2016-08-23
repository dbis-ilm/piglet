#!/bin/bash

FILE=$1
PORT=$2

nc -p $PORT -l -o $FILE --append-output --recv-only --keep-open
