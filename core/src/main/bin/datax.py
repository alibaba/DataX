#!/usr/bin/python
# -*- coding: UTF-8 -*-

# 根据机器上py版本，自动选择调用py2还是py3


import sys
import os

jsonFile=sys.argv[1]
print(jsonFile)

if sys.version > '3':
    pyPath = "py3"
else:
    pyPath = "py2"

execPy = "python " + pyPath + "/datax.py " + jsonFile
print (execPy)
os.system(execPy)