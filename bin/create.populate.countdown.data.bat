@echo off

set APP_HOME=%~dp0..
CALL "%APP_HOME%\env.bat"  

call %DERBY_HOME%\bin\ij.bat %APP_HOME%\DDL\Input.DB.DDL.sql




