set APP_HOME=%~dp0



CALL "%~dp0env.bat" 



echo "JAVA_HOME=" %JAVA_HOME%

echo "APP_HOME=" %APP_HOME%

echo "Derby Database Home=" %DERBY_HOME%

echo 



set CP=%APP_HOME%\CountDown.Data.Warehouse.jar;%APP_HOME%\lib\ojdbc6.jar;%APP_HOME%\lib\derbyclient.jar;%APP_HOME%\lib\commons-pool2-2.4.2.jar


"%JAVA_HOME%\bin\java" -cp %CP% com.countdown.Main 
