## importing relevant libraries

import matplotlib.pyplot as plt
import numpy as np
import numpy.ma as ma
import pickle
import sys
import os
import glob
import math
%matplotlib inline
print("test")
from importlib import reload
import re
from astropy.table import Table

sys.path.append('/users/akepley/code')
import test_rig


## getting base data set

testDir = '../casa610-105_realdata'
tests = glob.glob(os.path.join(testDir,"*.*.*.*_*"))

casa610results = {}

for test in tests:
    project = os.path.basename(test)
    print(project)
    casa610results[project] = test_rig.tCleanTime_newlogs_simple(test)

print(len(casa610results))

# getting the test data set

testDir = '../build84_realdata_norestart' 
tests = glob.glob(os.path.join(testDir,"*.*.*.*_*"))

#tests = [os.path.join(testDir,project) for project in projectList]
tests.remove(os.path.join(testDir,'2017.1.00019.S_2020_03_25T21_23_20.339'))

build84results = {}

for test in tests:
    project = os.path.basename(test)
    print(project)
    build84results[project] = test_rig.tCleanTime_newlogs_simple(test,serial=True)

print(len(build84results))

reload(test_rig)
results = test_rig.makeAstropyTimingTable(casa610results,build84results,serial=True)

## Getting the mask difference results

maskfile = '/lustre/naasc/sciops/comm/akepley/casa_test/cas9386/mask_diff_casa610-105_build84/casa610-105_build84_mask_comp.csv'

maskdb = Table.read(maskfile,format='ascii.csv',header_start=2)

results.add_column(np.zeros(len(results['imagename'])), name='nPixMaskBase')
results.add_column(np.zeros(len(results['imagename'])), name='nPixMaskTest')
results.add_column(ma.zeros(len(results['imagename'])), name='fracDiff')
results.add_column(np.zeros(len(results['imagename'])), name='nChanMaskBase')
results.add_column(np.zeros(len(results['imagename'])), name='nChanMaskTest')
results.add_column(np.zeros(len(results['imagename'])), name='maxMaskBase')
results.add_column(np.zeros(len(results['imagename'])), name='maxMaskTest')
results.add_column(np.zeros(len(results['imagename'])), name='FOVPixelsBase')
results.add_column(np.zeros(len(results['imagename'])), name='FOVPixelsTest')

fracDiffMask = np.ones(len(results['imagename']),dtype=bool)

for mask in maskdb:
    imagename = mask['Mask'].replace('iter1','iter0')
    
    idx = results['imagename'] == imagename
    
    if mask['fracDiff'] != '--':
        results['fracDiff'][idx] = float(mask['fracDiff'])
        fracDiffMask[idx] = False
    else:
        results['fracDiff'][idx] = 99
        fracDiffMask[idx] = True

    results['nPixMaskBase'][idx] = mask['nPixMaskBase']
    results['nPixMaskTest'][idx] = mask['nPixMaskTest']
    results['nChanMaskBase'][idx] = mask['nChanMaskBase']
    results['nChanMaskTest'][idx] = mask['nChanMaskTest']
    results['maxMaskBase'][idx] = mask['maxMaskBase']
    results['maxMaskTest'][idx] = mask['maxMaskTest']
    results['FOVPixelsBase'][idx] = mask['FOVPixelsBase']
    results['FOVPixelsTest'][idx] = mask['FOVPixelsTest']
        
results['fracDiff'].mask = fracDiffMask

## getting b75 results

b75 = Table.read('/lustre/naasc/sciops/comm/akepley/casa_test/cas9386/b75_calc/test_data_b75.csv',format='ascii.csv',header_start=1)

results.add_column(np.zeros(len(results['imagename'])), name='b75')

for line in b75 :
    idx = results['project'] == line['Project']
    nimage = len(results['project'][idx])
    
    results['b75'][idx] = np.repeat(line['b75'],nimage)

## Plotting different in time.

plt.figure(figsize=(8,6))
sp_12m = (results['specmode'] == 'cube') & (results['iter'] == 'iter0') & (results['gridder'] == 'standard') & (results['array'] == '12m') 
mosaic_12m = (results['specmode'] == 'cube') & (results['iter'] == 'iter0') & (results['gridder'] == 'mosaic') & (results['array'] == '12m')
sp_7m = (results['specmode'] == 'cube') & (results['iter'] == 'iter0') & (results['gridder'] == 'standard') & (results['array'] == '7m') 
mosaic_7m = (results['specmode'] == 'cube') & (results['iter'] == 'iter0') & (results['gridder'] == 'mosaic') & (results['array'] == '7m')
ephem = (results['project'] == '2017.1.00750.T_2020_03_22T21_23_03.693') & (results['specmode'] == 'cubesource') & (results['iter'] == 'iter0') 

plt.scatter(results[sp_12m]['totalTime_casa610']/3600.0, results[sp_12m]['totalTime_pdiff'],label='single 12m')
plt.scatter(results[mosaic_12m]['totalTime_casa610']/3600.0,results[mosaic_12m]['totalTime_pdiff'],label='mosaic 12m')
plt.scatter(results[sp_7m]['totalTime_casa610']/3600.0,results[sp_7m]['totalTime_pdiff'],label='single 7m')
plt.scatter(results[mosaic_7m]['totalTime_casa610']/3600.0,results[mosaic_7m]['totalTime_pdiff'],label='mosaic 7m')
plt.scatter(results[ephem]['totalTime_casa610']/3600.0,results[ephem]['totalTime_pdiff'],marker='s',color='black',label='ephem')

plt.xlim(-1,12.5)
plt.ylim(-100,100)

plt.xlabel('Total Time for Whole Clean (hr)')
plt.ylabel("% Diff between CASA 6.1.0 and Build 84")
plt.title("Cube")
plt.axhline(0.0,color='gray',lw=2,linestyle=':')
plt.axhspan(-10,10,color='blue',alpha=0.25)
plt.axvspan(-0.25,0.4,color='blue',alpha=0.25)

plt.legend()


labelidx = ((results['specmode'] == 'cube') & (results['iter'] == 'iter0') 
            & (results['totalTime_pdiff'] > 30) & (results['totalTime_casa610']/3600.0 > 2.0))
for image in results[labelidx]:
    name = '.'.join(image['imagename'].split('.')[2:4])
    plt.text(image['totalTime_casa610']/3600.00,image['totalTime_pdiff'],name,rotation=45)
    
labelidx = ((results['specmode'] == 'cube') & (results['iter'] == 'iter0') 
            & (results['totalTime_pdiff'] > 10) & (results['totalTime_casa610']/3600.0 > 2.0))
for image in results[labelidx]:
    name = '.'.join(image['imagename'].split('.')[2:4])
    plt.text(image['totalTime_casa610']/3600.00,image['totalTime_pdiff'],name,rotation=45)
