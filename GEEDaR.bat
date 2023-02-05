@echo off
rem Run GEEDaR script with the provided command line arguments.

set condaFolder=%USERPROFILE%\Anaconda3

%condaFolder%\python.exe %condaFolder%\cwp.py %condaFolder%\envs\geedar %condaFolder%\envs\geedar\python.exe "%~dp0GEEDaR.py" %*
