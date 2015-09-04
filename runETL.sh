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


CP=$APP_HOME/CountDown.Data.Warehouse.jar:$APP_HOME/lib/ojdbc6.jar:$APP_HOME/lib/derbyclient.jar:$APP_HOME/lib/commons-pool2-2.4.2.jar

$JAVA_HOME/bin/java -cp $CP com.countdown.Main 