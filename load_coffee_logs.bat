@echo off

rem https://gist.github.com/maximlt/531419545b039fa33f8845e5bc92edd6

call conda activate coffee_logs

python load_coffee_guru_logs.py

call conda deactivate
