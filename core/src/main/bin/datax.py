#!/usr/bin/python
# -*- coding: UTF-8 -*-

# 根据机器上py版本，自动选择调用py2还是py3


import sys
import os
currentFilePath = os.path.dirname(os.path.abspath(__file__))
print("currentFilePath===" + currentFilePath)

argvs=sys.argv[1:]
params = " ".join(argvs)
print("params===" + params)

if sys.version > '3':
    pyPath = "py3"
else:
    pyPath = "py2"

execPy = "python " + currentFilePath +os.sep +"py" +os.sep + pyPath + os.sep + "datax.py " + params
print("execPy===" + execPy)
os.system(execPy)