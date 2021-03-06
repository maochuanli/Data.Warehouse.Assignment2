#/bin/bash

function abspath() {
    # generate absolute path from relative path
    # $1     : relative filename
    # return : absolute path
    if [ -d "$1" ]; then
        # dir
        (cd "$1"; pwd)
    elif [ -f "$1" ]; then
        # file
        if [[ $1 == */* ]]; then
            echo "$(cd "${1%/*}"; pwd)/${1##*/}"
        else
            echo "$(pwd)/$1"
        fi
    fi
    
}

scriptPath=$(abspath $0)
APP_HOME=$(dirname $(dirname $scriptPath))
source $APP_HOME/env.sh

sh $DERBY_HOME/bin/ij $APP_HOME/DDL/Assign2.DW.Query.sql


