# routines useful for uvcontsub testing

# assumes using in casalith

from casaplotms import plotms
import os
from casatools import msmetadata as msmdtool

def uvcontsubplot(origvis='',origdatacol='data',contsubvis='',contsubdatacol='data',spwlist=[],srclist=[],plotprefix=''):

    '''
    Purpose: plotting continuum subtraction results in visibility space
    
    Date        Description of Changes          Programmer
    ----------------------------------------------------------------------
    4/20/2022   Original Code                   A.A. Kepley
    '''

    height = 600
    width = 800

    for src in srclist:
        for spw in spwlist:

            filename = 'uvcontsub_field'+src+'_spw'+spw+'.png'

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


def convert_to_uvcontsub2021(script):
    '''
    convert pipeline uvcontcube command to uvcontsub2021

    Date        Programmer      Description of Changes
    ----------------------------------------------------------------------
    4/20/2022   A.A. Kepley     Original Code

    '''

    import re
    import pdb

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

    msmd = msmdtool()

    # now create uvcontsub2021 commands
    for vis in results.keys():
        outputvis = os.path.basename(vis).replace(".ms","_uvcontsub.ms")

        # spw select
        spwselect = ''
        fieldselect = ''
        fitspwstr = ''
        for field in results[vis].keys():
            
            msmd.open(vis)
            fieldid = msmd.fieldsforname(field.replace('"',''))
            msmd.close()
                        
            if len(fieldid) == 1:
                fieldstr = str(fieldid[0])
            if len(fieldid) > 1:
                tmp = [str(i) for i in fieldid]
                fieldstr = ','.join(fieldstr)
            
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
            
        # close fitspwstr
        fitspwstr = fitspwstr + ']'

        # create command string
        cmdstr = "uvcontsub2021(vis='"+vis+"',outputvis='"+outputvis+"',field='"+fieldselect+"',spw='"+spwselect+"',fitspw="+fitspwstr+")"

        return cmdstr

def setup_uvcontsub_test(benchmarkDir,testDir):
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
            newcmd = convert_to_uvcontsub2021(uvcontsubscript)
            
            outDir = os.path.join(testDir,benchmarkName)

            if not os.path.exists(outDir):
                os.mkdir(outDir)

            outfile = os.path.join(outDir,benchmarkName+'_uvcontsub2021.py')
            
            f = open(outfile,'w+')
            f.write(newcmd + '\n')
            f.close()
            
## How do I want the structure?

## current paradigm:
## -- cycle through data directories with scripts. get script. convert. write out script to a new directory.



