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
APP_HOME=$(dirname $scriptPath)
source $APP_HOME/env.sh


echo "APP_HOME" $APP_HOME
echo "Derby Database Home:" $DERBY_HOME
sh $DERBY_HOME/bin/startNetworkServer

