# routines useful for uvcontsub testing

# assumes using in casalith
from casaplotms import plotms
from casatools import msmetadata as msmdtool

import os

def uvcontsubplot(origvis='',origdatacol='data',contsubvis='',contsubdatacol='data',spwlist=[],srclist=[],plotprefix='',plotdir='.'):

    '''
    Purpose: plotting continuum subtraction results in visibility space
    
    Date        Description of Changes          Programmer
    ----------------------------------------------------------------------
    4/20/2022   Original Code                   A.A. Kepley
    '''
    
    # assumes using in casalith
    #from casaplotms import plotms
    #from casatools import msmetadata as msmdtool


    height = 600
    width = 800

    
    srclist = [str(i) for i in srclist]
    spwlist = [str(i) for i in spwlist]

    for src in srclist:
        for spw in spwlist:
                
            filename = os.path.join(plotdir,plotprefix+'uvcontsub_field'+str(src)+'_spw'+str(spw)+'.png')

            if os.path.exists(filename):
                os.remove(filename)

            plotms(vis=origvis, gridcols=2, 
                   rowindex=0,
                   colindex=0,
                   xaxis='frequency',
                   yaxis='real',ydatacolumn=origdatacol,
                   field=src,spw=spw,
                   avgtime='1e8',avgscan=True,avgbaseline=True,
                   transform=True,freqframe='LSRK',showgui=False,
                   height=height,
                   width=width)

            plotms(vis=contsubvis, gridcols=2,plotindex=1, clearplots=False,
                   rowindex=0,
                   colindex=0,
                   xaxis='frequency',
                   yaxis='real',ydatacolumn=contsubdatacol,
                   field=src,spw=spw,
                   avgtime='1e8',avgscan=True,avgbaseline=True,
                   transform=True,freqframe='LSRK',showgui=False,
                   customsymbol=True,symbolcolor='ff00ff',
                   height=height,
                   width=width)

            plotms(vis=origvis, gridcols=2,plotindex=2,clearplots=False,
                   rowindex=0,
                   colindex=1,
                   xaxis='frequency',
                   yaxis='imag',ydatacolumn=origdatacol,
                   field=src,spw=spw,
                   avgtime='1e8',avgscan=True,
                   avgbaseline=True,
                   transform=True,freqframe='LSRK',showgui=False,
                   height=height,
                   width=width)

            plotms(vis=contsubvis, gridcols=2,plotindex=3,clearplots=False,
                   rowindex=0,
                   colindex=1,
                   xaxis='frequency',
                   yaxis='imag',ydatacolumn=contsubdatacol,
                   field=src,spw=spw,
                   avgtime='1e8',avgscan=True,
                   avgbaseline=True,
                   transform=True,freqframe='LSRK',showgui=False,
                   customsymbol=True,symbolcolor='ff00ff',
                   height=height,
                   width=width,
                   plotfile=filename)


def convert_to_uvcontsub2021(script,outfile,dictformat=False):
    '''
    convert pipeline uvcontcube command to uvcontsub2021

    Date        Programmer      Description of Changes
    ----------------------------------------------------------------------
    4/20/2022   A.A. Kepley     Original Code

    '''

    msmd = msmdtool()

    import re
    import pdb
    import ast
    import numpy as np

    uvcontfitRE = re.compile(r"""
    uvcontfit\(vis='(?P<vis>.*?)'
    .*
    field='(?P<field>.*?)'
    .*
    intent='(?P<intent>.*?)'
    .*
    spw='(?P<fitspw>.*?)' ## spws to fit
    .*
    fitorder=(?P<fitorder>.*?),
    .*
    """,re.VERBOSE)

    applycalRE = re.compile(r"""
    applycal\(vis='(?P<vis>.*?)'
    .*
    field='(?P<field>.*?)'
    .*
    spw='(?P<spw>.*?)' # spws to do continuum subtraction on
    .*
    """, re.VERBOSE)

    results = {}

    filein = open(script,'r')
    for line in filein: 

        # all uvcontfit commands first
        if uvcontfitRE.search(line):            
            cmd = uvcontfitRE.search(line)            
            vis = cmd.group('vis')
            field = cmd.group('field')

            # this will break if source name has a comma
            if len(field.split(',')) > 1:
                fieldno = ast.literal_eval(field) # convert string to array               
                msmd.open(vis)
                tmp = msmd.namesforfields(fieldno) # get name for field
                msmd.close()
                field = np.unique(tmp)[0] # only get unique


            if vis not in results.keys():
                results[vis] = {}
            if field not in results[vis].keys():
                results[vis][field] = {}
            results[vis][field]['fitspw'] = cmd.group('fitspw')
            results[vis][field]['fitorder'] = cmd.group('fitorder')
        
        

        # then all the applycal commands
        if applycalRE.search(line):
            cmd2 = applycalRE.search(line)
            vis = cmd2.group('vis')
            field = cmd2.group('field')

            spw = cmd2.group('spw')
            results[vis][field]['spw'] = spw



    fout = open(outfile,'w')

    # now create uvcontsub2021 commands
    for vis in results.keys():
        outputvis = os.path.basename(vis).replace(".ms","_uvcontsub.ms")

        # spw select
        spwselect = ''
        fieldselect = ''
        fitspwstr = ''
        fitspecstr = ''
        for field in results[vis].keys():
            
            msmd.open(vis)
            fieldid = msmd.fieldsforname(field.replace('"',''))
            msmd.close()
                        
            if len(fieldid) == 1:
                fieldstr = str(fieldid[0])
            if len(fieldid) > 1:
                tmp = [str(i) for i in fieldid]
                fieldstr = ','.join(tmp)
            
            if not fieldselect:
                fieldselect = fieldstr
            else:
                fieldselect = fieldselect + "," + fieldstr
                
            # set spwselect to value for first field. should be same
            # for all fields, but that's an assumption.
            if not spwselect:
                spwselect = results[vis][field]['spw']

            if not fitspwstr:
                fitspwstr = '['
            else:
                fitspwstr = fitspwstr + ','

            fitspwstr = fitspwstr + "['"+fieldstr + "','" + results[vis][field]['fitspw']+"']"

            if not fitspecstr:
                fitspecstr = "{"
            else:
                fitspecstr = fitspecstr + ','
            fitspecstr = fitspecstr + "'" + fieldstr + "' : {" 
            
            fitspwList = results[vis][field]['fitspw'].split(",")

            punct = ''
            for fitspw in fitspwList:
                myspw, mychan = fitspw.split(':')
                fitspecstr =  fitspecstr + punct + "'" + myspw + "': {'chan':'" + mychan + "', 'fitorder':" +   results[vis][field]['fitorder'] + "}"
                punct=', '
            
            fitspecstr = fitspecstr + "}" # close the dictionary for the field

        # close fitspwstr
        fitspwstr = fitspwstr + ']'

        # close fitspecstr
        fitspecstr = fitspecstr + '}'
        
        print(fitspecstr)

        if dictformat:
            # create command string
            cmdstr = "uvcontsub2021(vis='"+vis+"',outputvis='"+outputvis+"',field='"+fieldselect+"',spw='"+spwselect+"',fitspec="+fitspecstr+")\n"

        else:
            # create command string
            cmdstr = "uvcontsub2021(vis='"+vis+"',outputvis='"+outputvis+"',field='"+fieldselect+"',spw='"+spwselect+"',fitspw="+fitspwstr+", fitorder=" + results[vis][field]['fitorder'] + ")\n"

        # write to output file
        fout.write(cmdstr)



    fout.close()

def setup_uvcontsub_test(benchmarkDir,testDir,dictformat=False):
    '''

    Automatically populate a test directory with directories for
    individual data sets and copy over relevant run scripts

    '''

    # Purpose: automatically populate a test directory with directories
    #          individual data sets and copy over the relevant scripts
    #
    # Input:
    #   benchmarkDir: directory with benchmarks
    #   testDir: directory to run test in
    #
    # Output:
    #   scripts and directory structure for test
    
    # NOTES: based on regenerateTcleanCmds and setupTest for tclean tests

    # Date              Programmer              Description of Code
    #----------------------------------------------------------------------
    # 4/22/2022         A.A. Kepley             Original Code
    
    import glob
    import os
    import re
    
    # if the benchmark directory exists
    if os.path.exists(benchmarkDir):
        
        # get all the benchmarks
        dataDirs = os.listdir(benchmarkDir)
        
        for mydir in dataDirs:
            benchmarkName = re.findall('\w\w\w\w\.\w.\d\d\d\d\d\.\w_\d\d\d\d_\d\d_\d\dT\d\d_\d\d_\d\d\.\d\d\d',mydir)[0]
            
            uvcontsubscript = os.path.join(benchmarkDir,mydir, benchmarkName+'_uvcontsub.py')

            outDir = os.path.join(testDir,benchmarkName)

            if not os.path.exists(outDir):
                os.mkdir(outDir)

            outfile = os.path.join(outDir,benchmarkName+'_uvcontsub2021.py')
            convert_to_uvcontsub2021(uvcontsubscript,outfile,dictformat=dictformat)
                        

def parseLog_uvcontsub(logfile):
    ''' 
    Parse an individual log file and return a dictionary with the data in it.
    
    Date        Programmer      Description of Changes
    ----------------------------------------------------------------------
    5/10/2022   A.A. Kepley     Original Code
    '''

    import re
    from datetime import datetime
    import numpy as np
    
    # logfile = 'casa-20220509-172008.log' # old log
    # logfile = 'casa-20220506-170405.log' # new log

    uvcontfitBeginRE = re.compile(r"Begin Task: uvcontfit")
    uvcontfitEndRE = re.compile(r"End Task: applycal")
    applyStartRE = re.compile(r"Begin Task: applycal")

    uvcontsub2021BeginRE = re.compile(r"Begin Task: uvcontsub2021")
    uvcontsub2021EndRE = re.compile(r"End Task: uvcontsub2021")

    dateFmtRE = re.compile(r"(?P<timedate>\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2})")

    filein = open(logfile,'r')

    results = {}

    for line in filein:

        # only want first one
        if (uvcontsub2021BeginRE.search(line) or uvcontfitBeginRE.search(line)) and 'startTime' not in results.keys():
            startTimeStr = dateFmtRE.search(line)
            results['startTime'] = datetime.strptime(startTimeStr.group('timedate'),'%Y-%m-%d %H:%M:%S')

        # keep the first applycal time so I can compare restore vs. rerun
        if (applyStartRE.search(line) and ('applyStartTime' not in results.keys())):
            applyStartTimeStr = dateFmtRE.search(line)
            results['applyStartTime'] = datetime.strptime(applyStartTimeStr.group('timedate'),'%Y-%m-%d %H:%M:%S')


        # only keep last end time.
        if (uvcontsub2021EndRE.search(line) or uvcontfitEndRE.search(line)):
            endTimeStr = dateFmtRE.search(line)
            results['endTime'] = datetime.strptime(endTimeStr.group('timedate'),'%Y-%m-%d %H:%M:%S')


    results['uvcontsubTime'] = results['endTime'] - results['startTime']

    if 'applyStartTime' in results.keys():
        results['applyTime'] = results['endTime'] - results['applyStartTime']

    filein.close()

    return results

def uvcontsubTime(testDir):
    '''
    get timings for uvcontsub runs
    
    Date        Programmer              Description of Changes
    ----------------------------------------------------------------------
    5/10/2022   A.A. Kepley             Original Code

    '''

    import os
    import glob
    import re

    if os.path.exists(testDir):
        
        # initialize results
        results = {}

        # get file name
        logfile = sorted(glob.glob(os.path.join(testDir,"casa-????????-??????.log")))[0]

        results = parseLog_uvcontsub(logfile)
        
    else:
        print("no path found")
        results = {}

    return results

def collect_uvcontsubTimings(testDir, projectList = None, excludeList = None):
    '''

    collect all the timings from various projects

    '''

    import glob

    if os.path.exists(testDir):
        if not projectList:
            tests = glob.glob(os.path.join(testDir, "*.*.*.*_*"))
        else:
            tests = [os.path.join(testDir,project) for project in projectList]

    if excludeList:
        for project in excludeList:
            tests.remove(project)

    results = {}

    for test in tests:
        project = os.path.basename(test)
        print(project)
        results[project] = uvcontsubTime(test)

    return results

def create_table(inDict1, inDict2, label1='old',label2='new'):
    '''
    make an astropy table of timing data for uvcontsub
    '''
    
    from astropy.table import Table
    import numpy as np
    
    projectArr = np.array([])
    time1Arr = np.array([])
    time2Arr = np.array([])
    ratioArr = np.array([])
    applyArr = np.array([])

    for project in inDict1.keys():
        if project in inDict2.keys():
            projectArr = np.append(project,projectArr)
            time1Arr = np.append(inDict1[project]['uvcontsubTime'].seconds, time1Arr)
            time2Arr = np.append(inDict2[project]['uvcontsubTime'].seconds,time2Arr)
            ratioArr = np.append(inDict2[project]['uvcontsubTime'].seconds/inDict1[project]['uvcontsubTime'].seconds, ratioArr)
            
            applyArr = np.append(inDict1[project]['applyTime'].seconds, applyArr)

    t = Table([projectArr,time1Arr, time2Arr, ratioArr,applyArr],
              names = ('project','time_'+label1,'time_'+label2,'ratio','applytime'))

    return t
