#/bin/bash

# for mac osx
# export JAVA_HOME=/Library/Java/JavaVirtualMachines/jdk1.7.0_45.jdk/Contents/Home/
# for linux
export JAVA_HOME=/home/mao/jdk1.7.0_80

export DERBY_HOME=$APP_HOME/derby.home
export DERBY_OPTS=-Dderby.system.home=$DERBY_HOME

echo "JAVA_HOME=" $JAVA_HOME
echo "APP_HOME" $APP_HOME
echo "DERBY_HOME=" $DERBY_HOME
echo ""