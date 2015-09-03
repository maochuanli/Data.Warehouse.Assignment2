set APP_HOME=%~dp0



CALL "%~dp0env.bat" 



echo "APP_HOME=" %APP_HOME%

echo "Derby Database Home=" %DERBY_HOME%


call %DERBY_HOME%\bin\ij.bat %APP_HOME%\DDL\Assign2.DW.Query.sql


