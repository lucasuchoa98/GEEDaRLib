@echo off
:: Get user credentials for using the GEE API.

chcp 1252>nul

:: Quick-check of conda installation.
set condaFolder=%USERPROFILE%\Anaconda3
if not exist "%condaFolder%\_conda.exe" goto :NoConda

:: If the geedar environment does not exist, create it and install the API.
if not exist "%condaFolder%\envs\geedar\." goto :NoEE
:Auth
"%condaFolder%\python.exe" "%condaFolder%\cwp.py" "%condaFolder%\envs\geedar" "%condaFolder%\envs\geedar\python.exe" "%condaFolder%\envs\geedar\Scripts\earthengine-script.py" authenticate
goto :End

:NoEE
:: Environment setup.
echo Activating conda...
call "%condaFolder%\condabin\activate.bat"
call conda activate
echo Creating and activating geedar environment...
call conda env create -f geedar.yaml
call conda activate geedar
:: Get user credentials.
echo EE user authentication...
earthengine authenticate
goto :End

:NoConda
echo Anaconda3 was not found in %condaFolder%. Please, install it.
goto :End

:End
pause
