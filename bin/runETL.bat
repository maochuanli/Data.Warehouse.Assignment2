set echo off

set APP_HOME=%~dp0..
CALL "%APP_HOME%\env.bat" 

set CP=%APP_HOME%\lib\CountDown.Data.Warehouse.jar;%APP_HOME%\lib\ojdbc6.jar;%APP_HOME%\lib\derbyclient.jar;%APP_HOME%\lib\commons-pool2-2.4.2.jar

"%JAVA_HOME%\bin\java"  -DAPP_HOME=%APP_HOME% -cp %CP% com.countdown.Main 
