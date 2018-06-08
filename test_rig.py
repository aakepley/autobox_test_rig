def extractDataFromPipeline(pipelineDir,outDir,stages=[29,31,33]):
    '''
    copy the *target.ms and casalog data from imaging stages of a pipeline
    run and put them in their own test directory
    '''


    # Purpose: extract the tclean commands and target.ms files from the
    # Cycle 5 pipeline tests.
    # 
    # Input:
    #       List of pipeline data directories
    #       List of stages to extract casalog files for
    #       output directory name
    
    # Output:
    #       target files and casalog files for particular stages
    #
    # TODO:
    #       -- Make a driver function that takes list of files 
    
    # Date          Programmer              Description of Code
    # ----------------------------------------------------------------------
    # 10/26/2017    A.A. Kepley             Original Code
    

    import os.path
    import os
    import glob
    import shutil
    import re

    # the directory path exists 
    if os.path.exists(pipelineDir):    

        # find and copy over the benchmark data sets
        dataDir = os.path.split(pipelineDir)[0]
        targetFiles = glob.glob(os.path.join(dataDir,'*target.ms'))
        
        benchmarkName = re.findall('\w\w\w\w\.\w.\d\d\d\d\d\.\w_\d\d\d\d_\d\d_\d\dT\d\d_\d\d_\d\d\.\d\d\d',dataDir)[0]
      
        benchmarkDir = os.path.join(outDir,benchmarkName)

        if not os.path.exists(benchmarkDir):
            print "Creating directory for data: ", benchmarkDir
            os.mkdir(benchmarkDir)

        for myfile in targetFiles:
            targetFileName = os.path.basename(myfile)
            benchmarkData = os.path.join(benchmarkDir,targetFileName)
            if not os.path.exists(benchmarkData):
                print "Copying over data: ", targetFileName
                shutil.copytree(myfile,benchmarkData)

        # find, copy, and modify over the relevant casalog files
        for stage in stages:
            
            stageDir = os.path.join(pipelineDir,'html/stage'+str(stage))
            stageLog = os.path.join(stageDir,'casapy.log')
            
            outStageLog = os.path.join(benchmarkDir,'stage'+str(stage)+'.log')
            
            if not os.path.exists(outStageLog):
                print "Copying over stage " + str(stage)+ " log" 
                shutil.copy(stageLog,outStageLog)

            # extracting tclean commands from the casalog file
            outTcleanCmd = os.path.join(benchmarkDir,benchmarkName+'_stage'+str(stage)+'.py')
            extractTcleanFromLog(outStageLog,benchmarkDir,outTcleanCmd)

        os.system("cat " + os.path.join(benchmarkDir,benchmarkName)+"*.py > "+os.path.join(benchmarkDir,benchmarkName)+".py")
            

    else:
        print "path doesn't exist: " + pipelineDir

#----------------------------------------------------------------------

def extractTcleanFromLog(casalogfile,dataDir,outfile):
    '''
    extract the tclean commands from a casalogfile and create a
    executable script that will run tclean commands.
    '''

    # Purpose: extract tclean commands from casalogfile and create an
    # executable script
    # 
    # Input:
    #       
    #       casalogfile: logfile name
    #       outfile: output file name
    #       dataDir: data directory name
    
    # Output:
    #       
    #       executable casa script to run tests.

    # Date          Programmer              Description of Code
    # ----------------------------------------------------------------------
    # 10/26/2017    A.A. Kepley             Original Code
    # 11/29/2017    A.A. Kepley             Fixing aggregate continuum.

    import os.path
    import re
    import ast
    
    if os.path.exists(casalogfile):
    
        tcleanCmd = re.compile(r"""
        (?P<cmd>tclean\(   ## tclean command
        .*                 ## skip junk in command
        vis=(?P<vis>\[.*?\]) ## visibility name
        .*                 ## skip junk in command
        imagename='(?P<imagename>.*?)' ## imagename
        .*
        deconvolver='(?P<deconvolver>.*?)' ## deconvolver
        .*\) ## end of tclean command
        ) 
        """,re.VERBOSE)

        ntermsRE = re.compile("nterms=(?P<nterms>.*?),")

        filein = open(casalogfile,'r')
        fileout = open(outfile,'w')

        # this may need to be modified for mfs images
        imageExt = ['.pb','.psf','.residual','.sumwt','.weight']
        
        for line in filein:

            # look for tclean command
            findtclean = tcleanCmd.search(line)

            # if you find the tclean command go to work
            if findtclean:

                # extract key values
                cmd = findtclean.group('cmd')
                vis = findtclean.group('vis')
                imagename = findtclean.group('imagename')
                deconvolver = findtclean.group('deconvolver')

                # update the data directory in the command for the new data location
                newvis = [dataDir+'/'+val for val in ast.literal_eval(vis)]                
                newcmd = cmd.replace(vis,repr(newvis))

                # write out the necessary commands to a file.
                fileout.write(newcmd+'\n\n')

                # follow the iter0 with commands to copy iter0 to iter1
                if re.search('iter0',imagename):

                    # dealing with the mtmfs case.
                    if deconvolver == 'mtmfs':

                        nterms = ntermsRE.search(line).group('nterms')

                        for ext in imageExt:
                            if ext == '.pb':
                                fileout.write("os.system('cp -ir "+imagename+ext+'.tt0'+' '+ imagename.replace('iter0','iter1')+ext+'.tt0'+"')\n")
                            if ext == '.psf':
                                for term in range(int(nterms)+1):
                                    fileout.write("os.system('cp -ir "+imagename+ext+'.tt'+str(term)+' '+ imagename.replace('iter0','iter1')+ext+'.tt'+str(term)+"')\n")

                            if ext == '.residual':
                                for term in range(int(nterms)):
                                    fileout.write("os.system('cp -ir "+imagename+ext+'.tt'+str(term)+' '+ imagename.replace('iter0','iter1')+ext+'.tt'+str(term)+"')\n")

                            if ext == '.sumwt':
                                for term in range(int(nterms)+1):
                                    fileout.write("os.system('cp -ir "+imagename+ext+'.tt'+str(term)+' '+ imagename.replace('iter0','iter1')+ext+'.tt'+str(term)+"')\n")

                            if ext == '.weight':
                                for term in range(int(nterms)+1):
                                    fileout.write("os.system('cp -ir "+imagename+ext+'.tt'+str(term)+' '+ imagename.replace('iter0','iter1')+ext+'.tt'+str(term)+"')\n")
                        


                    # dealing with the rest of the caes.            
                    else:
                        for ext in imageExt:
                            fileout.write("os.system('cp -ir "+imagename+ext+' '+ imagename.replace('iter0','iter1')+ext+"')\n")


                    fileout.write('\n')
    
        filein.close()
        fileout.close()

    else:
        print "Couldn't open file: " + casalogfile

#----------------------------------------------------------------------

def setupTest(benchmarkDir,testDir):
    '''
    Automatically populate a test directory with directories for
    individual data sets and copies over the relevant run scripts.

    '''

    # Purpose:  automatically populate a test directory with directories for 
    #          individual data sets and copy over the relevant scripts
    #
    # 
    # Input:
    #       benchmarkDir: directory with benchmarks
    #       testDir: directory to run test in.
    #
    # 
    # Output:
    #       scripts and directory structure for test
    #       

    # TO-DO: 
    #        -- needs to be tested.
    #        -- also could add something in here to modify parameters, although the regex's would have to be carefully constructed. Note that I don't need this for now.

    # Date          Programmer              Description of Code
    # ---------- ------------------------------------------------------------
    # 11/02/2017   A.A. Kepley             Original Code

    import shutil
    import glob
    import os
    import os.path

    # if the benchmark directory exists
    if os.path.exists(benchmarkDir):

        # get all the benchmarks
        dataDirs = os.listdir(benchmarkDir)
        
        # go to test directory, create directory structure, and copy scripts
        currentDir = os.getcwd()

        if not os.path.exists(testDir):
            os.mkdir(testDir)

        os.chdir(testDir)
        for mydir in dataDirs:

            if not os.path.exists(mydir):
                os.mkdir(mydir)

            scripts = glob.glob(os.path.join(benchmarkDir,mydir)+"/*.py")
            for myscript in scripts:
                scriptDir = os.path.join(testDir,mydir)
                scriptPath = os.path.join(scriptDir,os.path.basename(myscript))
                if not os.path.isfile(scriptPath):                    
                    shutil.copy(myscript,scriptDir)

        # switch back to original directory
        os.chdir(currentDir)




#----------------------------------------------------------------------

def tCleanTime(testDir):
    '''
    Time how long the parts in clean take for an individual test directory.
    '''

    # Purpose: mine logs for information about timing.
    
    # Input: 

    #   testDir: I'm assuming that the testDirectory contains one
    #   casalog file, but may want to add the option to specify a log

    # Output:

    #    a structure with all the timing information, plus vital stats
    #    on the data set. Right now I'm keeping the imagename, whether
    #    or not it's a cube and number of cycles. Some other
    #    information that might be useful to keep: 
    #       image size on disk -- get from os
    #       threshold? -- from tclean input
    #       nchan - will have to calculate -- can get from msmd? Ask Remy about this part.
    #
    #  Date             Programmer              Description of Changes
    #----------------------------------------------------------------------
    # 2017/11/08        A.A. Kepley             Original Code


    import os
    import os.path
    import glob
    import re
    from datetime import datetime
    import copy
    import pdb

    if os.path.exists(testDir):
    
        # get the file name
        logfile = glob.glob(os.path.join(testDir,"*.log"))    

        if len(logfile) > 1:
            print "Multiple logs found. Using the first one"
            mylog = logfile[0]
        elif len(logfile) == 0:
            print "no logs found returning"
            return 
        else:
            mylog = logfile[0]
            print "using log: ", mylog

        # regex patterns for below.
        tcleanBeginRE = re.compile(r"Begin Task: tclean")
        imagenameRE = re.compile(r'imagename=\"(?P<imagename>.*?)\"')
        specmodeRE = re.compile(r'specmode=\"(?P<specmode>.*?)\"')
        startMaskRE = re.compile(r'Generating AutoMask')
        startMinorCycleRE = re.compile(r'Run Minor Cycle Iterations')
        endMajorCycleRE = re.compile(r'Completed \w+ iterations.')
        startMajorCycleRE = re.compile(r'Major Cycle (?P<cycle>\w+?)')
        startPrune1RE = re.compile(r'Pruning the current mask')
        startGrowRE = re.compile(r'Growing the previous mask')
        startPrune2RE = re.compile(r'Pruning the growed previous mask')
        startNegativeThresholdRE = re.compile(r'Creating a mask for negative features.')
        endNegativeThresholdRE = re.compile(r'No negative region was found by auotmask.')
        endCleanRE = re.compile(r'Reached global stopping criterion : (?P<stopreason>.*)')
        tcleanEndRE = re.compile(r"End Task: tclean")
        #tcleanFailRE = re.compile(r"An error occurred running task tclean.") ## catch artifact of running in batch mode.
        tcleanFailRE = re.compile(r'Exception from task_tclean : couldn\'t connect to display ":0"') ## catch artifact of running batch mode without xbuffer

        dateFmtRE = re.compile(r"(?P<timedate>\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2})")

        # open file
        filein = open(mylog,'r')

        #initialize loop status        
        imagename = ''
        allresults = {}
        results = {}
        cycleresults = {}
        cycle = '0'
        specmode=''
        
        # go through file
        for line in filein:
            #print line

            # capture start of tclean
            if tcleanBeginRE.search(line):
                startTimeStr = dateFmtRE.search(line)
                if startTimeStr:                
                    results['startTime'] = datetime.strptime(startTimeStr.group('timedate'),'%Y-%m-%d %H:%M:%S')

            # capture image name
            if imagenameRE.search(line):
                imagename = imagenameRE.search(line).group('imagename')
            
            # capture line vs. continuum
            if specmodeRE.search(line):
                if re.match(specmodeRE.search(line).group('specmode'),'cube'):
                    specmode='cube'
                else:
                    specmode='cont'

            # if imagename is iter1, record automasking information.
            if re.search('iter1', imagename):

                # capture the start of the mask
                if startMaskRE.search(line):
                    maskStartTimeStr = dateFmtRE.search(line)
                    if maskStartTimeStr:
                        cycleresults['maskStartTime'] = datetime.strptime(maskStartTimeStr.group('timedate'),'%Y-%m-%d %H:%M:%S')
                    
                # capture first prune 
                if startPrune1RE.search(line):
                    startPrune1TimeStr = dateFmtRE.search(line)
                    if startPrune1TimeStr:
                        cycleresults['startPrune1Time'] = datetime.strptime(startPrune1TimeStr.group('timedate'),'%Y-%m-%d %H:%M:%S')

                # capture start of grow
                if startGrowRE.search(line):
                    startGrowTimeStr = dateFmtRE.search(line)
                    if startGrowTimeStr:
                        cycleresults['startGrowTime'] = datetime.strptime(startGrowTimeStr.group('timedate'),'%Y-%m-%d %H:%M:%S')

                # capture 2nd prune
                if startPrune2RE.search(line):
                    startPrune2TimeStr = dateFmtRE.search(line)
                    if startPrune2TimeStr:
                        cycleresults['startPrune2Time'] = datetime.strptime(startPrune2TimeStr.group('timedate'),'%Y-%m-%d %H:%M:%S')

                # capture the negative threshold start
                if startNegativeThresholdRE.search(line):
                    startNegativeThresholdTimeStr = dateFmtRE.search(line)
                    if startNegativeThresholdTimeStr:
                        cycleresults['startNegativeThresholdTime'] = datetime.strptime(startNegativeThresholdTimeStr.group('timedate'),'%Y-%m-%d %H:%M:%S')


                # capture the negative threshold end
                if endNegativeThresholdRE.search(line):
                    endNegativeThresholdTimeStr = dateFmtRE.search(line)
                    if endNegativeThresholdTimeStr:
                        cycleresults['endNegativeThresholdTime'] = datetime.strptime(endNegativeThresholdTimeStr.group('timedate'),'%Y-%m-%d %H:%M:%S')

                # capture the start of the Major Cycle
                if startMajorCycleRE.search(line):
                    cycle = startMajorCycleRE.search(line).group('cycle')
                    startMajorCycleTimeStr = dateFmtRE.search(line)
                    if startMajorCycleTimeStr:
                        cycleresults['startMajorCycleTime'] = datetime.strptime(startMajorCycleTimeStr.group('timedate'),'%Y-%m-%d %H:%M:%S')


                # capture the start of the minor cycle
                if startMinorCycleRE.search(line):
                    startMinorCycleTimeStr = dateFmtRE.search(line)
                    if startMinorCycleTimeStr:
                        cycleresults['startMinorCycleTime'] = datetime.strptime(startMinorCycleTimeStr.group('timedate'),'%Y-%m-%d %H:%M:%S')

                # capture the end of the major cycle
                if endMajorCycleRE.search(line):
                    endMajorCycleTimeStr = dateFmtRE.search(line)
                    if endMajorCycleTimeStr:
                        cycleresults['endMajorCycleTime'] = datetime.strptime(endMajorCycleTimeStr.group('timedate'),'%Y-%m-%d %H:%M:%S')
                
                        # calculate times
                        cycleresults['totalMaskTime'] = cycleresults['startMinorCycleTime'] - cycleresults['maskStartTime']
                        cycleresults['thresholdTime'] = cycleresults['startPrune1Time'] - cycleresults['maskStartTime']
                        cycleresults['cycleTime'] = cycleresults['endMajorCycleTime'] - cycleresults['maskStartTime']

                        if cycleresults.has_key('startGrowTime'):
                            cycleresults['prune1Time'] = cycleresults['startGrowTime'] - cycleresults['startPrune1Time']
                            cycleresults['growTime'] = cycleresults['startPrune2Time'] - cycleresults['startGrowTime']

                            if cycleresults.has_key('startNegativeThresholdTime'):
                                cycleresults['prune2Time'] = cycleresults['startNegativeThresholdTime'] - cycleresults['startPrune2Time']
                            else:
                                cycleresults['prune2Time'] = cycleresults['startMinorCycleTime'] - cycleresults['startPrune2Time']
                        else: 
                            cycleresults['prune1Time'] = cycleresults['startMinorCycleTime'] - cycleresults['startPrune1Time']
                   
                        if cycleresults.has_key('negativeThresholdTime'):
                            cycleresults['negativeThresholdTime'] = cycleresults['endNegativeThresholdTime'] - cycleresults['startNegativeThresholdTime']

                        ## save major cycle information here
                        results[cycle] = cycleresults
                        cycleresults={}

                # if  clean terminates catch this
                if endCleanRE.search(line):
                    endCleanStr = dateFmtRE.search(line)
                    if endCleanStr:
                        cycleresults['endCleanTime'] = datetime.strptime(endCleanStr.group('timedate'),'%Y-%m-%d %H:%M:%S')
              
                    # save final cycle results    
                    cycleresults['totalMaskTime'] = cycleresults['endCleanTime'] - cycleresults['maskStartTime']
                    cycleresults['thresholdTime'] = cycleresults['startPrune1Time'] - cycleresults['maskStartTime']
                    cycleresults['cycleTime'] = cycleresults['endCleanTime'] - cycleresults['maskStartTime']


                    if cycleresults.has_key('startGrowTime'):
                        cycleresults['prune1Time'] = cycleresults['startGrowTime'] - cycleresults['startPrune1Time']
                        cycleresults['growTime'] = cycleresults['startPrune2Time'] - cycleresults['startGrowTime']

                        if cycleresults.has_key('startNegativeThresholdTime'):
                                cycleresults['prune2Time'] = cycleresults['startNegativeThresholdTime'] - cycleresults['startPrune2Time']
                        else:
                            cycleresults['prune2Time'] = cycleresults['endCleanTime'] - cycleresults['startPrune2Time']
                    else: 
                        cycleresults['prune1Time'] = cycleresults['endCleanTime'] - cycleresults['startPrune1Time']

                    if cycleresults.has_key('startNegativeThresholdTime'):
                        cycleresults['negativeThresholdTime'] = cycleresults['endNegativeThresholdTime'] - cycleresults['startNegativeThresholdTime']
                    
                    # get exit criteria
                    if endCleanRE.search(line):
                        results['stopreason'] = endCleanRE.search(line).group('stopreason')

                    ## save major cycle information here
                    results[cycle] = cycleresults
                    cycleresults={}

                # capture the end of the clean
                if tcleanEndRE.search(line) or tcleanFailRE.search(line):
                    endTimeStr = dateFmtRE.search(line)
                    if endTimeStr:
                        results['endTime'] = datetime.strptime(endTimeStr.group('timedate'),'%Y-%m-%d %H:%M:%S')

                    # calculate overall statistics.
                    results['tcleanTime'] = results['endTime']-results['startTime']
                    results['ncycle'] = cycle
                    results['specmode'] = specmode

                    
                    # if iter1 image, save results and clear variables.
                    if re.search('iter1',imagename):
                        allresults[imagename] = results
                        #pdb.set_trace()

                    # clear for next clean run
                    results = {} 
                    cycleresults = {}
                    imagename = ''
                    cycle = '0'
                    specmode=''

        filein.close()

    else:
        print "no path found"
        allresults = {}
            
    return allresults
            
        
#----------------------------------------------------------------------

def flattenTimingData(inDict):
    '''
    flatten the array of dictionaries so that I can more easily plot
    things. I'm assuming that it's a dictionary of dictionaries with
    the top level dictionary being the project. I could also
    potentially turn into an astropy table here.

    I'm also thinking of turning the images into seconds, but could do
    that earlier in the extract timing data. The latter is more effective.

    '''

    import numpy as np

    flatDict = {'project': [],
                'imagename': [],
                'ncycle': [],
                'stopreason': [],
                'startTime': [],
                'cycle': [],
                'specmode': [],
                'tcleanTime': [],
                'endTime': [],
                'startMinorCycleTime': [],
                'cycleTime': [],
                'startPrune2Time': [],
                'startGrowTime': [],
                'totalMaskTime': [],
                'startPrune1Time': [],
                'endMajorCycleTime': [],
                'prune2Time': [],
                'thresholdTime': [],
                'startMajorCycleTime': [],
                'prune1Time': [],
                'maskStartTime': [],
                'growTime': [],
                'endCleanTime': [],
                'startNegativeThresholdTime': [],
                'endNegativeThresholdTime': [],
                'negativeThresholdTime': []}
    
    durationKeys = ['negativeThresholdTime','cycleTime','prune2Time','growTime','thresholdTime','prune1Time','totalMaskTime']
    timeKeys = ['startPrune2Time','startGrowTime']
    cycleKeys = ['startMinorCycleTime', 'cycleTime', 'startPrune2Time', 'startGrowTime', 'totalMaskTime', 'startPrune1Time', 'endMajorCycleTime', 'prune2Time', 'thresholdTime', 'startMajorCycleTime', 'prune1Time', 'maskStartTime', 'growTime','negativeThresholdTime']


    for (project,images) in inDict.iteritems():
        for (image,data) in images.iteritems():
            for cycle in map(str,range(0,int(data['ncycle'])+1)):

                flatDict['project'].append(project)
                flatDict['imagename'].append(image)
                flatDict['cycle'].append(cycle)

                # Get the base info from the top level of the data structure
                flatDict['ncycle'].append(data['ncycle'])
                flatDict['stopreason'].append(data['stopreason'])
                flatDict['startTime'].append(data['startTime'])
                flatDict['specmode'].append(data['specmode'])
                flatDict['tcleanTime'].append(float(data['tcleanTime'].seconds))
                flatDict['endTime'].append(data['endTime'])

                # doing something a little bit fancy.
                for akey in cycleKeys:
                    if data[cycle].has_key(akey):
                        if akey in durationKeys:
                            flatDict[akey].append(float(data[cycle][akey].seconds))
                        else:
                            flatDict[akey].append(data[cycle][akey])
                    else:
                        #print "Key", akey, " not in cycle. Inserting blank value"
                        flatDict[akey].append(999)

    # now I need to turn everything into numpy arrays so that they work in matplotlib.
    for akey in flatDict.keys():
        tmp = np.array(flatDict[akey])
        flatDict[akey] = np.ma.array(tmp, mask=(tmp == 999))

    

    return flatDict

#----------------------------------------------------------------------

def createBatchScript(testDir, casaPath):
    '''
    generate a pipeline batch script quickly to send jobs to nodes
    '''

    import os
    import glob
    import re

    projectRE = re.compile("(?P<project>\d{4}\.\w\.\d{5}\.\w_\d{4}_\d{2}_\d{2}T\d{2}_\d{2}_\d{2}\.\d{3})")

    if os.path.exists(testDir):
        f = open(os.path.join(testDir,'pipelinerun'),'w')
        dataDirs = os.listdir(testDir)
        for mydir in dataDirs:
            if projectRE.match(mydir):
                project = projectRE.match(mydir).group('project')
                projectDir = os.path.join(testDir,mydir)
                script = os.path.join(projectDir,project)+'.py'

                outline = "cd " +projectDir + \
                          "; export PATH="+os.path.join(casaPath, "bin") + ":${PATH}" + \
                          "; xvfb-run -d casa --nogui -c " + script +"\n"
                f.write(outline)
        f.close()

#----------------------------------------------------------------------

## Here I want to write some code to compare masks. Note that all the
## data comes out of tclean as casa images, so I need to deal with
## those rather than fits images, unless I want to add a step that
## converts to fits.

# I'm going to assume I'm doing this in CASA because that's just going
# to be easier.

## below I need to figure out some way to call the code only if I'm in
## casa. May need to make the mask comparison a separate code file.


#casa --nologger --log2term


#from taskinit import *
#ia = iatool()
    
def maskComparison(baseDir, testDir, outFile):

    '''
    Compare the test masks to the base masks and see if anything has changed.
    '''

    # Input: 
    #       baseDir: base speed directory
    #       testDir: test directory
    #       outFile: text file for results
    #
    # Output:
    #       text file with results 
    #
    # TO DO:
    #    -- check that this works for nterms>2 images
    #
    # Date              Programmer              Description of Changes
    #----------------------------------------------------------------------
    # 11/16/2017?       A.A. Kepley             Original Code
    # 12/08/2017        A.A. Kepley             Modified original code to extract more stats

    import os
    import os.path
    import re
    import glob
    import csv
    import numpy as np
    import pdb

    projectRE = re.compile("\d{4}\.\w\.\d{5}\.\w_\d{4}_\d{2}_\d{2}T\d{2}_\d{2}_\d{2}\.\d{3}")
 
    if os.path.exists(baseDir):

        dataDirs = os.listdir(baseDir)
        
        with open(outFile,'w') as csvfile:
            writer = csv.writer(csvfile,delimiter=',')

            writer.writerow(["# Base: "+baseDir])
            writer.writerow(["# Test: "+testDir])
            writer.writerow(["Project","Mask","nPixMaskBase","nPixMaskTest","nPixDiff","nPixDiffBase","nPixDiffTest","fracDiff"])

            ## print a header comparison.
            for mydir in dataDirs:
                if projectRE.match(mydir):
                    baseProject = os.path.join(baseDir,mydir)
                    baseMaskList = [os.path.basename(mask) for mask in glob.glob(os.path.join(baseProject,"*.mask"))]                                     
                    testProject = os.path.join(testDir,mydir)
                    if os.path.exists(testProject):            

                        for mask in baseMaskList:
                            baseMaskPath = os.path.join(baseProject,mask)
                            testMaskPath = os.path.join(testProject,mask)

                            ia.open(baseMaskPath)
                            baseImageStats = ia.statistics()
                            ia.close()
                            nPixMask = baseImageStats['sum'][0]

                            if os.path.exists(testMaskPath):
                                ia.open(testMaskPath)
                                testMaskStats = ia.statistics()
                                ia.close()

                                nPixMaskTest = testMaskStats['sum'][0]

                                diffImage = mask+'.diff'
                                if not os.path.exists(diffImage):
                                    divexpr = '\"'+testMaskPath + '\"-\"'+ baseMaskPath+'\"'
                                    myim = ia.imagecalc(diffImage,divexpr,overwrite=True)
                                    myim.done()

                                ia.open(diffImage)
                                diffImagePix = ia.getchunk(dropdeg=True)
                                ia.done()
                                    

                                nPixDiff = np.sum(abs(diffImagePix))
                                nPixDiffBase = np.sum(diffImagePix < 0)
                                nPixDiffTest = np.sum(diffImagePix > 0)

                                #pdb.set_trace()

                                if nPixMask > 0:
                                    fracDiff = nPixDiff/nPixMask
                                    writer.writerow([mydir,mask,nPixMask,nPixMaskTest,nPixDiff,nPixDiffBase,nPixDiffTest,fracDiff])
                                else:
                                    writer.writerow([mydir,mask,nPixMask,nPixMaskTest,nPixDiff,nPixDiffBase,nPixDiffTest, '--'])
                              
                                # write to file

                                
                            else:

                                writer.writerow([mydir,mask,nPixMask,nPixMaskTest,'--','--','--','--'])


                    else:
                            print "no corresponding test project: ", mydir
                            writer.writerow([mydir,mask,nPixMask,'--','--','--','--','--'])

            ia.done()

    else:
        print "test directory doesn't exist:", mydir
            

#----------------------------------------------------------------------

def extractBeamInfoFromLog(casalogfile,outfile):

    '''
    Get the beam information for various robust values from the log
    and put it into a text file.
    '''

    # Purpose:  Get the beam information for various robust values from the log
    # and put it into a text file.
    
    # Input: casalogfile from imageprecheck stage (currently stage 18)
    #
    # Output: text file with each line giving the beam size for a
    # particular robust value
    
    # Date              Programmer              Description of Code
    #----------------------------------------------------------------------
    # 02/15/2018        A.A. Kepley             Original Code

    import os.path
    import re
    import ast
    
    if os.path.exists(casalogfile):
        filein = open(casalogfile,'r')
        fileout = open(outfile,'w')

        robustLineRE = re.compile(r"""
        robust\s=\s(?P<robust>\S+)
        """,re.VERBOSE)

        beamLineRE = re.compile(r"""
        Beam\s:\s
        (?P<bmax>\S+?)\sarcsec,\s+
        (?P<bmin>\S+?)\sarcsec,\s+
        (?P<bpa>\S+?)\sdeg
        """,re.VERBOSE)

        for line in filein:
            findRobustLine = robustLineRE.search(line)
            if findRobustLine:
                robust = findRobustLine.group('robust')

            findBeamLine = beamLineRE.search(line)
            if findBeamLine:
                bmax = findBeamLine.group('bmax')
                bmin = findBeamLine.group('bmin')
                bpa = findBeamLine.group('bpa')
                fileout.write(robust +' '+bmax+' '+bmin+' '+bpa+'\n')


        filein.close()
        fileout.close()

     
    else:
        print "Couldn't open file: " + casalogfile


#----------------------------------------------------------------------

def makeBeamInfoFiles(dataDir,stage=18):

    '''
    Go through each of the projects in the pipeline directory,
    figure out their beam parameters, and copy to the output data directory.
    '''

    # Purpose: extract beam information for various robust values, so
    # that I can test the effects of the choice of robust values on the data.
    # 
    # Input:
    #   dataDir: pdata directories holding logs
    #   stage: stage number that has the beam info (right now 18, but that could change)
    #
    # Output:
    #   text file in each data directory in dataDir with beam information
    #
    # Date              Programmer              Description of Code
    #----------------------------------------------------------------------
    # 2/15/2018         A.A. Kepley             Original Code

    import os.path
    import os
    import glob
    import re
    import pdb

    projectRE = re.compile("(?P<project>\d{4}\.\w\.\d{5}\.\w_\d{4}_\d{2}_\d{2}T\d{2}_\d{2}_\d{2}\.\d{3})")
  
    if os.path.exists(dataDir):
        dataDirs = os.listdir(dataDir)
        for mydir in dataDirs:
            project = projectRE.match(mydir).group('project')

            projectDir = os.path.join(dataDir,mydir)
            filename = 'stage'+str(stage)+'.log'
            casalogfile = os.path.join(projectDir,filename)
            outfile = os.path.join(projectDir,"beam_info.dat")
            extractBeamInfoFromLog(casalogfile,outfile)

    else:
        print "Data directory doesn't exist: " + dataDir
            
#----------------------------------------------------------------------

def modifyRobust(inScript,beamInfo,outScript,robust=2.0,ptsPerBeam=5.0):

    '''
    modify the robust, cellsize, imsize, and threshold in the given
    test scripts

    '''

    # Purpose: modify the robust, cellsize, imsize, and threshold in the
    # given test scripts

    # Input:
    #   inScript:
    #
    # Output:
    #   outScript:

    # Questions: -- Do I want to modify in place? Or do I want to
    # copy, then modify?  -- I'm thinking that modifying in place
    # might simplest? Although it might make testing harder? I'm
    # thinking if I modify the the setupTest to have a parameter to
    # modify the script rather than just straight copy. That might be
    # easiest. It would also avoid overwriting files
    
    # Date              Programmer              Description of Changes
    #----------------------------------------------------------------------
    # 2/15/2018         A.A. Kepley             Original Code

    
    import re
    import os.path
    import pdb
    import math
    
    tcleanCmd = re.compile(r"""
    (?P<cmd>tclean\(   ## tclean command
    .*
    (?P<thresholdStr>threshold='(?P<threshold>.*?)(?P<unit>[a-zA-z]+)')
    .*
    (?P<imsizeStr>imsize=\[(?P<imsize1>.*?),(?P<imsize2>.*?)\])
    .*
    (?P<cellStr>cell=\['(?P<cell>.*?)arcsec'\])
    .*
    (?P<robustStr>robust=(?P<robust>.*?)),
    .*\) ## end of tclean command
    ) 
    """,re.VERBOSE)

    if os.path.exists(beamInfo):
        filein = open(beamInfo)
        for line in filein:
            (robustTmp, bmaxTmp, bminTmp,bpaTmp) = line.rstrip().split(' ')
            if float(robustTmp) == robust:
                bmax = bmaxTmp
                bmin = bminTmp
                bpa = bpaTmp

    if 'bmax' not in locals():
        print "Given robust value not found in beam info file: ", beamInfo
        print "exiting"
    
    else:
        # let's get to work

        cellOut = min(float(bmin),float(bmax))/ptsPerBeam

        # values below derived from Todd's experiments in CAS-10153
        if robust == -0.5:
            thresholdFactor= 1.33         
        elif robust == 2.0:
            thresholdFactor = 0.95
        else:
            thresholdFactor=1.0
    
        if os.path.exists(inScript):
            filein = open(inScript,'r')
            fileout = open(outScript,'w')

            for line in filein:
                findtclean = tcleanCmd.search(line)
                if findtclean:
                    #print line
                    robustIn = findtclean.group('robust')
                    #print robustIn
                    if float(robustIn) == robust:
                        print "Not changing script. Input and output robust are the same",robust
                    else:
                        cellIn = float(findtclean.group('cell'))
                        imsize1In = float(findtclean.group('imsize1'))
                        imsize2In = float(findtclean.group('imsize2'))
                        thresholdIn = float(findtclean.group('threshold'))

                        imsize1Out = int(math.floor(imsize1In * (cellIn/cellOut)))
                        imsize2Out = int(math.floor(imsize2In * (cellIn/cellOut)))
                        thresholdOut = thresholdIn*thresholdFactor
                        
                        thresholdOutStr = "threshold='"+str(thresholdOut)+findtclean.group('unit')+"'"

                        robustOutStr = "robust="+str(robust)
                        cellOutStr = "cell=['"+str(cellOut)+"arcsec']"
                        imsizeOutStr = "imsize=["+str(imsize1Out)+', '+str(imsize2Out)+"]"

                        newline = line.replace(findtclean.group('thresholdStr'),thresholdOutStr)
                        newline1 = newline.replace(findtclean.group('cellStr'),cellOutStr)
                        newline2 = newline1.replace(findtclean.group('imsizeStr'),imsizeOutStr)
                        newline3 = newline2.replace(findtclean.group('robustStr'),robustOutStr)

                        fileout.write(newline3)


                else:
                    fileout.write(line)


            filein.close()
            fileout.close()

#----------------------------------------------------------------------

def setupRobustTest(benchmarkDir,testDir,robust=2,ptsPerBeam=5.0):
    '''
    Automatically populate a test directory with directories for
    individual data sets, modifies the script for a new robust value
    and copies over to a directory

    '''

    # Purpose:  automatically populate a test directory with directories for 
    #          individual data sets and copy over the relevant scripts
    #
    # 
    # Input:
    #       benchmarkDir: directory with benchmarks
    #       testDir: directory to run test in.
    #
    # 
    # Output:
    #       scripts and directory structure for test
    #       

    # Date          Programmer              Description of Code
    # ---------- ------------------------------------------------------------
    # 11/02/2017   A.A. Kepley             Original Code
    # 2/15/2018    A.A. Kepley             Modified from setupTest
    

    import shutil
    import glob
    import os
    import os.path
    import pdb

    # if the benchmark directory exists
    if os.path.exists(benchmarkDir):

        # get all the benchmarks
        dataDirs = os.listdir(benchmarkDir)
        
        # go to test directory, create directory structure, and copy scripts
        currentDir = os.getcwd()

        if not os.path.exists(testDir):
            os.mkdir(testDir)

        os.chdir(testDir)
        for mydir in dataDirs:

            if not os.path.exists(mydir):
                os.mkdir(mydir)

            scripts = glob.glob(os.path.join(benchmarkDir,mydir)+"/????.?.?????.?_????_??_??T??_??_??.???.py")
            scripts.extend(glob.glob(os.path.join(benchmarkDir,mydir)+"/????.?.?????.?_????_??_??T??_??_??.???_stage??.py"))

            for myscript in scripts:
                myOutScript = myscript.replace('.py','_r'+str(robust)+'_ptsPerBeam'+str(ptsPerBeam)+'.py') 
                
                scriptDir = os.path.join(testDir,mydir)
                scriptPath = os.path.join(scriptDir,os.path.basename(myOutScript))

                if not os.path.isfile(scriptPath):
                    beamInfoFile = os.path.join(os.path.join(benchmarkDir,mydir),'beam_info.dat')
                    modifyRobust(myscript,beamInfoFile,myOutScript,robust=robust,ptsPerBeam=ptsPerBeam)
                    shutil.copy(myOutScript,scriptDir)

        # switch back to original directory
        os.chdir(currentDir)

#----------------------------------------------------------------------

def strip_logs(inlogs, channels=[195]):


    '''
    Purpose: Grab some relevant lines out of the logs to check cyclethreshold issue
    '''

    import re

    for filein in inlogs:

        f = open(filein, 'r')
        fileout = filein.replace(".log","_trim.log")
        fout = open(fileout,'w')

        for line in f:
            if re.search('imagename=".+"',line):
                fout.write(line)

            if re.search('threshold=".+Jy"',line):
                fout.write(line)

            if re.search("Peak residual \(max,min\)",line):
                fout.write(line)

            if re.search("Total Model Flux",line):
                fout.write(line)

            if channels:
                
                for achannel in channels:

                    if re.search("chan " + str(achannel) + " ",line):
                        fout.write(line)
                        
                    if re.search("C"+str(achannel)+"\]",line):
                        fout.write(line)
                        
                    if re.search("set chanFlag\(to stop updating automask\)  to True for chan="+str(achannel)+"\n",line):
                        fout.write(line)



            if re.search("Number of pixels in the clean mask",line):
                fout.write(line)

            if re.search("CycleThreshold=",line):
                fout.write(line)

            if re.search("Total model flux",line):
                fout.write(line)

            if re.search("Completed .+ iterations.",line):
                fout.write(line)

            if re.search("Run Major Cycle",line):
                fout.write(line)

            if re.search("Run \(Last\) Major Cycle",line):
                fout.write(line)

            if re.search("grow iter done=",line):
                fout.write(line)
                

        fout.close()
        f.close()

