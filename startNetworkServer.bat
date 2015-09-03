
rem %~dp0 is expanded pathname of the current script under NT


set APP_HOME=%~dp0


CALL "%~dp0env.bat" 



echo "APP_HOME=" %APP_HOME%

echo "Derby Database Home=" %DERBY_HOME%


call %DERBY_HOME%\bin\startNetworkServer.bat

