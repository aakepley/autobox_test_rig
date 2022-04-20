import ipdb

def extractDataFromPipeline(pipelineDir,outDir,stages=[29,31,33],copyCont=False, contsubstages=None):
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
    #       copyCont: copy continuum
    #       contsubstages: continuum subtraction stages to copy

    
    # Output:
    #       target files and casalog files for particular stages
    #
    # TODO:
    #       -- Make a driver function that takes list of files 
    
    # Date          Programmer              Description of Code
    # ----------------------------------------------------------------------
    # 10/26/2017    A.A. Kepley             Original Code
    # 10/14/2021    A.A. Kepley             Original Code


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
            print("Creating directory for data: ", benchmarkDir)
            os.mkdir(benchmarkDir)

        for myfile in targetFiles:
            targetFileName = os.path.basename(myfile)
            benchmarkData = os.path.join(benchmarkDir,targetFileName)
            if not os.path.exists(benchmarkData):
                print("Copying over data: ", targetFileName)
                shutil.copytree(myfile,benchmarkData)

        # find, copy, and modify over the relevant casalog files
        for stage in stages:
            
            stageDir = os.path.join(pipelineDir,'html/stage'+str(stage))
            stageLog = os.path.join(stageDir,'casapy.log')
            
            outStageLog = os.path.join(benchmarkDir,'stage'+str(stage)+'.log')
            
            if not os.path.exists(outStageLog):
                print("Copying over stage " + str(stage)+ " log" )
                shutil.copy(stageLog,outStageLog)

            # extracting tclean commands from the casalog file
            outTcleanCmd = os.path.join(benchmarkDir,benchmarkName+'_stage'+str(stage)+'.py')
            extractTcleanFromLog(outStageLog,benchmarkDir,outTcleanCmd)

        os.system("cat " + os.path.join(benchmarkDir,benchmarkName)+"_stage??.py > "+os.path.join(benchmarkDir,benchmarkName)+".py")
        
        # copy over the cont.data file
        if copyCont:
            contFile = os.path.join(os.path.split(pipelineDir)[0],'cont.dat')
            outContFile = os.path.join(benchmarkDir,'cont.dat')
            if not os.path.exists(outContFile):
                print("Copying over cont.dat file")
                shutil.copy(contFile,outContFile)
            
            contTabs = glob.glob(os.path.join(os.path.split(pipelineDir)[0],"*.uvcont.tbl"))
            for tab in contTabs:
                outFile = os.path.join(benchmarkDir,os.path.basename(tab))
                if not os.path.exists(outFile):
                    print("copying over uvcontsub tables")
                    shutil.copytree(tab, outFile)


        # find, copy, and modify over relevant casalog files for uvcontsub
        if contsubstages:
            for stage in contsubstages:
                stageDir = os.path.join(pipelineDir,'html/stage'+str(stage))
                stageLog = os.path.join(stageDir,'casapy.log')
                
                outStageLog = os.path.join(benchmarkDir,'stage'+str(stage)+'.log')
            
                if not os.path.exists(outStageLog):
                    print("Copying over stage " + str(stage)+ " log" )
                    shutil.copy(stageLog,outStageLog)

                outcontsubCmd = os.path.join(benchmarkDir,benchmarkName+'_stage'+str(stage)+'_uvcontsub.py')
                extractContsubFromLog(outStageLog,benchmarkDir,outcontsubCmd)
        
        os.system("cat " + os.path.join(benchmarkDir,benchmarkName)+"_stage??_uvcontsub.py > " + os.path.join(benchmarkDir,benchmarkName)+"_uvcontsub.py")

    else:
        print("path doesn't exist: " + pipelineDir)

#----------------------------------------------------------------------

def regenerateTcleanCmds(benchmarkDir, stages=[31,33,35]):
    ''' 
    Purpose: regenerate Tclean commands from existing pipeline logs
    '''

    import os
    import glob
    import re

    # if the benchmark directory exists
    if os.path.exists(benchmarkDir):

        # get all the benchmarks
        dataDirs = os.listdir(benchmarkDir)

        for mydir in dataDirs:
            benchmarkName = re.findall('\w\w\w\w\.\w.\d\d\d\d\d\.\w_\d\d\d\d_\d\d_\d\dT\d\d_\d\d_\d\d\.\d\d\d',mydir)[0]

            for stage in stages:
                outTcleanCmd = os.path.join(benchmarkDir,mydir,benchmarkName+'_stage'+str(stage)+'.py')
                outStageLog = os.path.join(benchmarkDir,mydir,'stage'+str(stage)+'.log')
            
                extractTcleanFromLog(outStageLog,os.path.join(benchmarkDir,mydir),outTcleanCmd)

            os.system("cat " + os.path.join(benchmarkDir,mydir,benchmarkName)+"_stage??.py > "+os.path.join(benchmarkDir,mydir,benchmarkName)+".py")

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
        (?P<cmd>tclean\(vis=(?P<vis>\[.*?\]) ## visibility name
        .*                 ## skip junk in command
        imagename='(?P<imagename>.*?)' ## imagename
        .*
        deconvolver='(?P<deconvolver>.*?)' ## deconvolver
        .*\) ## end of tclean command
        ) 
        """,re.VERBOSE)

        ntermsRE = re.compile("nterms=(?P<nterms>.*?),")
        copytreeRE = re.compile("(?P<copytree>copytree\(src=(?P<src>'.*?'),.*\))")
        filein = open(casalogfile,'r')
        fileout = open(outfile,'w')

        fileout.write('import shutil\n')
        fileout.write('import os\n')
        fileout.write('\n')

        # this may need to be modified for mfs images
        imageExt = ['.pb','.psf','.residual','.sumwt','.weight','.workdirectory','.gridwt_moswt']
        
        copytreePresent = False
        
        lastImageIter0 = False

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

                # follow the iter0 with commands to copy iter0 to iter1 if no copytree commands are present.
                if re.search('iter1',imagename) and not copytreePresent and lastImageIter0:
                    
                    # dealing with the mtmfs case.
                    if deconvolver == 'mtmfs':

                        nterms = ntermsRE.search(line).group('nterms')

                        for ext in imageExt: 
                            if  ext == '.pb':
                                fileout.write("if os.path.exists('"+imagename.replace('iter1','iter0')+ext+".tt0"+"'):\n")
                                fileout.write("\t shutil.copytree(src='"+imagename.replace('iter1','iter0')+ext+".tt0', dst='"+imagename+ext+".tt0')\n")
                            elif ext == '.workdirectory':
                                iter0work = imagename.replace('iter1','iter0')+ext
                                iter1work = imagename+ext

                                fileout.write("if os.path.exists('"+iter0work+"'):\n")
                                #fileout.write("\t shutil.copytree(src='"+iter0work+"', dst='"+iter1work+"')\n") 
                                # more complex method below not needed and causes tclean failures. -- I"m not sure this is true.
                                fileout.write("\t iter0work = '" + iter0work + "'\n")
                                fileout.write("\t iter1work = '" + iter1work + "'\n")
                                fileout.write("\t os.mkdir(iter1work)\n")
                                fileout.write("\t dirlist = os.listdir(iter0work)\n")
                                fileout.write("\t for mydir in dirlist:\n")
                                fileout.write("\t\t iter0 = os.path.join(iter0work,mydir)\n")
                                fileout.write("\t\t iter1 = os.path.join(iter1work,mydir.replace('iter0','iter1'))\n")
                                fileout.write("\t\t shutil.copytree(src=iter0, dst=iter1)\n")

                            elif ext == '.gridwt_moswt':

                                fileout.write("if os.path.exists('"+imagename.replace('iter1','iter0')+ext+"'):\n")
                                fileout.write("\t shutil.copytree(src='"+imagename.replace('iter1','iter0')+ext+"', dst='"+imagename+ext+"')\n")

                            elif ((ext == '.residual') or (ext == '.model')):
                                  for term in range(int(nterms)):
                                      fileout.write("if os.path.exists('"+imagename.replace('iter1','iter0')+ext+".tt"+str(term)+"'):\n")
                                      fileout.write("\t shutil.copytree(src='"+imagename.replace('iter1','iter0')+ext+".tt"+str(term)+"', dst='"+imagename+ext+".tt"+str(term)+"')\n")

                            else:
                                for term in range(int(nterms)+1):
                                    fileout.write("if os.path.exists('"+imagename.replace('iter1','iter0')+ext+".tt"+str(term)+"'):\n")
                                    fileout.write("\t shutil.copytree(src='"+imagename.replace('iter1','iter0')+ext+".tt"+str(term)+"', dst='"+imagename+ext+".tt"+str(term)+"')\n")
                                    

                    # dealing with the rest of the cases.            
                    else:
                        for ext in imageExt:
                            
                            if ext == '.workdirectory':
                                iter0work = imagename.replace('iter1','iter0')+ext
                                iter1work = imagename+ext

                                #fileout.write("if os.path.exists('"+iter0work+"'):\n")
                                #fileout.write("\t shutil.copytree(src='"+iter0work+"', dst='"+iter1work+"')\n")
                                
                                fileout.write("if os.path.exists('"+imagename.replace('iter1','iter0')+ext+"'):\n")
                                fileout.write("\t iter0work = '" + imagename.replace('iter1','iter0')+ext+"'\n")
                                fileout.write("\t iter1work = '" + imagename+ext+ "'\n")
                                fileout.write("\t os.mkdir(iter1work)\n")
                                fileout.write("\t dirlist = os.listdir(iter0work)\n")
                                fileout.write("\t for mydir in dirlist:\n")
                                fileout.write("\t\t iter0 = os.path.join(iter0work,mydir)\n")
                                fileout.write("\t\t iter1 = os.path.join(iter1work,mydir.replace('iter0','iter1'))\n")
                                fileout.write("\t\t shutil.copytree(src=iter0, dst=iter1)\n")

                            else:

                                fileout.write("if os.path.exists('"+imagename.replace('iter1','iter0')+ext+"'):\n")
                                fileout.write("\t shutil.copytree(src='"+imagename.replace('iter1','iter0')+ext+"', dst='"+imagename+ext+"')\n")
                    
                    fileout.write('\n')

                # Write out the necessary commands to a file.
                fileout.write(newcmd+'\n\n')


                if re.search('iter0',imagename):
                    lastImageIter0 = True
                else:
                    lastImageIter0 = False

            # find copy tree command
            findcopytree = copytreeRE.search(line)

            # if present then write out command and set the copytreePresent variable to True.
            if findcopytree:
                fileout.write('if os.path.exists('+findcopytree.group('src')+'):\n')
                fileout.write("    shutil."+findcopytree.group('copytree')+"\n\n")
                copytreePresent = True
    
           

        filein.close()
        fileout.close()

    else:
        print("Couldn't open file: " + casalogfile)

#----------------------------------------------------------------------

def extractContsubFromLog(casalogfile,dataDir,outfile):
    '''
    extract continuum fitting and subtraction commands from log

    Input:
           
        casalogfile: logfile name
        outfile: output file name
        dataDir: data directory name
    
    Date        Programmer              Description of Code
    ----------------------------------------------------------------------
    10/14/2021  A.A. Kepley             Original Code

    '''

    import os.path
    import re
    import ast

    if os.path.exists(casalogfile):
        uvcontfitCmd = re.compile(r"""
        (?P<cmd>uvcontfit\(vis='(?P<vis>.*?)' ## visibility name
        .*\) ## end of command
        )
        """,re.VERBOSE)


        applycalCmd = re.compile(r"""
        (?P<cmd>applycal\(vis='(?P<vis>.*?)' ## visibility name
        .*\) ## end of command
        )
        """,re.VERBOSE)

        filein = open(casalogfile,'r')
        fileout = open(outfile, 'w')
        
        for line in filein:
            finduvcontfit = uvcontfitCmd.search(line)
            findapplycal = applycalCmd.search(line)

            if finduvcontfit:
                cmd = finduvcontfit.group('cmd')
                vis = finduvcontfit.group('vis')
                newvis = dataDir+'/'+vis
                newcmd = cmd.replace(vis,newvis,1) # only replace first (vis) instance.
                fileout.write(newcmd+'\n')
                fileout.write('\n')

            if findapplycal:
                cmd = findapplycal.group('cmd')
                vis = findapplycal.group('vis')
                newvis = dataDir+'/'+vis
                newcmd = cmd.replace(vis,newvis,1) # only replace first (vis) instance.
                
                fileout.write(newcmd+'\n')
                fileout.write('\n')


        filein.close()
        fileout.close()


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
            print("Multiple logs found. Using the first one")
            mylog = logfile[0]
            print("using log: ", mylog)
        elif len(logfile) == 0:
            print("no logs found returning")
            return 
        else:
            mylog = logfile[0]
            print("using log: ", mylog)

        # regex patterns for below.
        tcleanBeginRE = re.compile(r"Begin Task: tclean")
        imagenameRE = re.compile(r'imagename=\"(?P<imagename>.*?)\"')
        specmodeRE = re.compile(r'specmode=\"(?P<specmode>.*?)\"')
        startMaskRE = re.compile(r'Generating AutoMask')

        sidelobeRE = re.compile(r'SidelobeLevel = ')
        modelFluxRE = re.compile(r'Total Model Flux : (?P<flux>\d*\.\d+|\d+)')

        startMinorCycleRE = re.compile(r'Run Minor Cycle Iterations')
        endMajorCycleRE = re.compile(r'Completed \w+ iterations.')
        startMajorCycleRE = re.compile(r'Major Cycle (?P<cycle>\d*)')
        startPrune1RE = re.compile(r'Pruning the current mask')
        startGrowRE = re.compile(r'Growing the previous mask')
        startPrune2RE = re.compile(r'Pruning the growed previous mask')
        startNegativeThresholdRE = re.compile(r'Creating a mask for negative features.')
        endNegativeThresholdRE = re.compile(r'No negative region was found by auotmask.')
        endCleanRE = re.compile(r'Reached global stopping criterion : (?P<stopreason>.*)')

        startRestoreRE = re.compile(r'Restoring model image')
        endRestoreRE = re.compile(r'Applying PB correction')

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

                # capture noise calculation
                if sidelobeRE.search(line):
                    sidelobeStartTimeStr = dateFmtRE.search(line)
                    if sidelobeStartTimeStr:
                        cycleresults['noiseEndTime'] = datetime.strptime(sidelobeStartTimeStr.group('timedate'),'%Y-%m-%d %H:%M:%S')
                        cycleresults['noiseTime'] = cycleresults['noiseEndTime'] - cycleresults['maskStartTime']

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
                # capture amount of flux in model
                if modelFluxRE.search(line):
                    modelFlux = modelFluxRE.search(line).group('flux')
                    cycleresults['modelFlux'] = float(modelFlux)

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

                        #if cycleresults.has_key('startGrowTime'):
                        if 'startGrowTime' in cycleresults.keys():
                            cycleresults['prune1Time'] = cycleresults['startGrowTime'] - cycleresults['startPrune1Time']
                            cycleresults['growTime'] = cycleresults['startPrune2Time'] - cycleresults['startGrowTime']

                            if 'startNegativeThresholdTime' in cycleresults.keys():
                                cycleresults['prune2Time'] = cycleresults['startNegativeThresholdTime'] - cycleresults['startPrune2Time']
                            else:
                                cycleresults['prune2Time'] = cycleresults['startMinorCycleTime'] - cycleresults['startPrune2Time']
                        else: 
                            cycleresults['prune1Time'] = cycleresults['startMinorCycleTime'] - cycleresults['startPrune1Time']
                   
                        if 'negativeThresholdTime' in cycleresults.keys():
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


                    if 'startGrowTime' in cycleresults.keys():
                        cycleresults['prune1Time'] = cycleresults['startGrowTime'] - cycleresults['startPrune1Time']
                        cycleresults['growTime'] = cycleresults['startPrune2Time'] - cycleresults['startGrowTime']

                        if 'startNegativeThresholdTime' in cycleresults.keys():
                                cycleresults['prune2Time'] = cycleresults['startNegativeThresholdTime'] - cycleresults['startPrune2Time']
                        else:
                            cycleresults['prune2Time'] = cycleresults['endCleanTime'] - cycleresults['startPrune2Time']
                    else: 
                        cycleresults['prune1Time'] = cycleresults['endCleanTime'] - cycleresults['startPrune1Time']

                    if 'startNegativeThresholdTime' in cycleresults.keys():
                        cycleresults['negativeThresholdTime'] = cycleresults['endNegativeThresholdTime'] - cycleresults['startNegativeThresholdTime']
                    
                    # get exit criteria
                    if endCleanRE.search(line):
                        results['stopreason'] = endCleanRE.search(line).group('stopreason')

                    ## save major cycle information here
                    results[cycle] = cycleresults
                    cycleresults={}

                # capture restore time here
                if startRestoreRE.search(line):
                    startRestoreStr = dateFmtRE.search(line)
                    if startRestoreStr:
                        results['startRestoreTime'] = datetime.strptime(startRestoreStr.group('timedate'),'%Y-%m-%d %H:%M:%S')

                if endRestoreRE.search(line):
                    endRestoreStr = dateFmtRE.search(line)
                    if endRestoreStr:
                        results['endRestoreTime'] = datetime.strptime(endRestoreStr.group('timedate'), '%Y-%m-%d %H:%M:%S')
                        results['restoreTime'] = results['endRestoreTime'] - results['startRestoreTime']
                            
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
        print("no path found")
        allresults = {}
            
    return allresults

#----------------------------------------------------------------------

def tCleanTime_newlogs(testDir):
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
    # 2018/06/08        A.A. Kepley             Created a new copy of this function to deal with the new log structure.
    # 2019/02/25        A.A. Kepley             Added features to deal with MPI logs.

    import os
    import os.path
    import glob
    import re
    #import ipdb

    mpiRE = re.compile(r"MPIServer-(?P<mpinum>\d+?)")
    mpiStopRE = re.compile(r"CASA Version")
    casaonlyRE = re.compile(r"::casa")

    if os.path.exists(testDir):
    
        # initialize lists
        allresults = {}

        # get the file name 
        logfilelist = glob.glob(os.path.join(testDir,"casa-????????-??????.log"))

        # go through logs and extract info.
        for logfile in logfilelist:

            f = open(logfile,'r')
            
            line = f.readline()
            
            if casaonlyRE.search(line) and not mpiRE.search(line):
                line = f.readline()
            
            # if it's in mpi mode, figure out how many nodes were used.
            if mpiRE.search(line):
                mpimax = 0

                while not mpiStopRE.search(line):
                                        
                    mpinum = int(mpiRE.search(line).group('mpinum'))
                    if mpinum > mpimax:
                        mpimax = mpinum
                    line = f.readline()
                    while not mpiRE.search(line):
                        line = f.readline()                
                    
                f.close()

                split_mpi_logs(logfile,n=mpimax+1)

                for mpi in range(1,mpimax+1):
                    mpistr = 'mpi'+str(mpi)
                    tmpresults = parseLog_newlog(logfile.replace('.log','_'+mpistr+'.log'))
                    
                    for imagename in tmpresults.keys():
                        if imagename in allresults:
                            allresults[imagename][mpistr] = tmpresults[imagename]
                        else:
                            allresults[imagename] = {}
                            allresults[imagename][mpistr] = tmpresults[imagename]
                    
                    #ipdb.set_trace()

            else:
                
                f.close()

                tmpresults = parseLog_newlog(logfile)
                #mpistr='mpi0'

                #for imagename in tmpresults.keys():
                #    if imagename in allresults:
                #        allresults[imagename][mpistr] = tmpresults[imagename]
                #    else:
                #        allresults[imagename] = {}
                #        allresults[imagename][mpistr] = tmpresults[imagename]
                 
                allresults = tmpresults

    else:
        print("no path found")
        allresults = {}
            
    return allresults

# ----------------------------------------------------------------------

def parseLog_newlog(logfile):
    '''
    Parse an individual log file and return an object with the data in it
    '''
    
    import re
    from datetime import datetime
    import copy
    #import ipdb

    # regex patterns for below.
    tcleanBeginRE = re.compile(r"Begin Task: tclean")

    imagenameRE = re.compile(r'imagename=\"(?P<imagename>.*?)\"')
    specmodeRE = re.compile(r'specmode=\"(?P<specmode>.*?)\"')

    startMaskRE = re.compile(r'Generating AutoMask')        
    sidelobeRE = re.compile(r'SidelobeLevel = ')
    
    startThresholdRE = re.compile(r'Start thresholding: create an initial mask by threshold')
    endThresholdRE = re.compile(r'End thresholding: time to create the initial threshold mask:')
    startPrune1RE = re.compile(r'Start pruning: the initial threshold mask')
    endPrune1RE = re.compile(r'End pruning: time to prune the initial threshold mask:')
    startSmooth1RE = re.compile(r'Start smoothing: the initial threshold mask')
    endSmooth1RE = re.compile(r'End smoothing: time to create the smoothed initial threshold mask:')
    
    startGrowRE = re.compile(r'Start grow mask: growing the previous mask')
    endGrowRE = re.compile(r'End grow mask:')
    startPrune2RE = re.compile(r'Start pruning: on the grow mask')
    endPrune2RE = re.compile(r'End pruning: time to prune the grow mask:')
    startSmooth2RE = re.compile(r'Start smoothing: the grow mask')
    endSmooth2RE = re.compile(r'End smoothing: time to create the smoothed grow mask:')
    
    startNegativeThresholdRE = re.compile(r'Start thresholding: create a negative mask')
    endNegativeThresholdRE = re.compile(r'End thresholding: time to create the negative mask:')
    
    modelFluxRE = re.compile(r'Total Model Flux : (?P<flux>\d*\.\d+|\d+)')
    
    startMinorCycleRE = re.compile(r'Run Minor Cycle Iterations')
    endMajorCycleRE = re.compile(r'Completed \w+ iterations.')
    startMajorCycleRE = re.compile(r'Major Cycle (?P<cycle>\d*)')
    
    endCleanRE = re.compile(r'Reached global stopping criterion : (?P<stopreason>.*)')

    startRestoreRE = re.compile(r'Restoring model image')
    endRestoreRE = re.compile(r'Applying PB correction')
    
    tcleanEndRE = re.compile(r"End Task: tclean")
    
    dateFmtRE = re.compile(r"(?P<timedate>\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2})")


    # open file
    filein = open(logfile,'r')

    #initialize loop status        
    imagename = ''
    allresults = {}
    results = {}
    cycleresults = {}
    cycle = '0'
    specmode=''

    # go through file
    for line in filein:

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

            # capture noise calculation
            if sidelobeRE.search(line):
                sidelobeStartTimeStr = dateFmtRE.search(line)
                if sidelobeStartTimeStr:
                    cycleresults['noiseEndTime'] = datetime.strptime(sidelobeStartTimeStr.group('timedate'),'%Y-%m-%d %H:%M:%S')
                    cycleresults['noiseTime'] = cycleresults['noiseEndTime'] - cycleresults['maskStartTime']

            # capture the threshold time
            if startThresholdRE.search(line):
                startThresholdTimeStr = dateFmtRE.search(line)
                if startThresholdTimeStr:
                    cycleresults['startThresholdTime'] = datetime.strptime(startThresholdTimeStr.group('timedate'),'%Y-%m-%d %H:%M:%S')
            if endThresholdRE.search(line):
                endThresholdTimeStr = dateFmtRE.search(line)
                if endThresholdTimeStr:
                    cycleresults['endThresholdTime'] = datetime.strptime(endThresholdTimeStr.group('timedate'),'%Y-%m-%d %H:%M:%S')

            # capture the prune time
            if startPrune1RE.search(line):
                startPrune1TimeStr = dateFmtRE.search(line)
                if startPrune1TimeStr:
                    cycleresults['startPrune1Time'] = datetime.strptime(startPrune1TimeStr.group('timedate'),'%Y-%m-%d %H:%M:%S')
            if endPrune1RE.search(line):
                endPrune1TimeStr = dateFmtRE.search(line)
                if endPrune1TimeStr:
                    cycleresults['endPrune1Time'] = datetime.strptime(endPrune1TimeStr.group('timedate'),'%Y-%m-%d %H:%M:%S')

            # capture the smooth time
            if startSmooth1RE.search(line):
                startSmooth1TimeStr = dateFmtRE.search(line)
                if startSmooth1TimeStr:
                    cycleresults['startSmooth1Time'] = datetime.strptime(startSmooth1TimeStr.group('timedate'),'%Y-%m-%d %H:%M:%S')
            if endSmooth1RE.search(line):
                endSmooth1TimeStr = dateFmtRE.search(line)
                if endSmooth1TimeStr:
                    cycleresults['endSmooth1Time'] = datetime.strptime(endSmooth1TimeStr.group('timedate'),'%Y-%m-%d %H:%M:%S')

            # capture the grow
            if startGrowRE.search(line):
                startGrowTimeStr = dateFmtRE.search(line)
                if startGrowTimeStr:
                    cycleresults['startGrowTime'] = datetime.strptime(startGrowTimeStr.group('timedate'),'%Y-%m-%d %H:%M:%S')
            if endGrowRE.search(line):
                endGrowTimeStr = dateFmtRE.search(line)
                if endGrowTimeStr:
                    cycleresults['endGrowTime'] = datetime.strptime(endGrowTimeStr.group('timedate'),'%Y-%m-%d %H:%M:%S')

            # capture 2nd prune
            if startPrune2RE.search(line):
                startPrune2TimeStr = dateFmtRE.search(line)
                if startPrune2TimeStr:
                    cycleresults['startPrune2Time'] = datetime.strptime(startPrune2TimeStr.group('timedate'),'%Y-%m-%d %H:%M:%S')
            if endPrune2RE.search(line):
                endPrune2TimeStr = dateFmtRE.search(line)
                if endPrune2TimeStr:
                    cycleresults['endPrune2Time'] = datetime.strptime(endPrune2TimeStr.group('timedate'),'%Y-%m-%d %H:%M:%S')

            # capture 2nd smooth
            if startSmooth2RE.search(line):
                startSmooth2TimeStr = dateFmtRE.search(line)
                if startSmooth2TimeStr:
                    cycleresults['startSmooth2Time'] = datetime.strptime(startSmooth2TimeStr.group('timedate'),'%Y-%m-%d %H:%M:%S')
            if endSmooth2RE.search(line):
                endSmooth2TimeStr = dateFmtRE.search(line)
                if endSmooth2TimeStr:
                    cycleresults['endSmooth2Time'] = datetime.strptime(endSmooth2TimeStr.group('timedate'),'%Y-%m-%d %H:%M:%S')

            # capture the negative threshold
            if startNegativeThresholdRE.search(line):
                startNegativeThresholdTimeStr = dateFmtRE.search(line)
                if startNegativeThresholdTimeStr:
                    cycleresults['startNegativeThresholdTime'] = datetime.strptime(startNegativeThresholdTimeStr.group('timedate'),'%Y-%m-%d %H:%M:%S')
            if endNegativeThresholdRE.search(line):
                endNegativeThresholdTimeStr = dateFmtRE.search(line)
                if endNegativeThresholdTimeStr:
                    cycleresults['endNegativeThresholdTime'] = datetime.strptime(endNegativeThresholdTimeStr.group('timedate'),'%Y-%m-%d %H:%M:%S')
            # capture amount of flux in model
            if modelFluxRE.search(line):
                modelFlux = modelFluxRE.search(line).group('flux')
                cycleresults['modelFlux'] = float(modelFlux)

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
                    cycleresults['cycleTime'] = cycleresults['endMajorCycleTime'] - cycleresults['maskStartTime']
                    cycleresults['thresholdTime'] = cycleresults['endThresholdTime'] - cycleresults['startThresholdTime']

                    # inserting this in just in case setting minbeamfrac=0.0 turns off the logger messages. Need to check this.
                    if 'startPrune1Time' in cycleresults:
                        # To keep this calculation consistent with the previous logs, the prune1Time is actually the Prune1Time+smooth1Time.
                        # I also calculate the smooth1Time, so that should give me how long the the smooth took compared to the prune+smooth.
                        if 'startGrowTime' in cycleresults:
                            cycleresults['prune1Time'] = cycleresults['startGrowTime'] - cycleresults['startPrune1Time']
                        else:
                            cycleresults['prune1Time'] = cycleresults['endSmooth1Time'] - cycleresults['startPrune1Time']

                    # should always smooth
                    cycleresults['smooth1Time'] = cycleresults['endSmooth1Time'] - cycleresults['startSmooth1Time']

                    # The following might not always happen depending on how the auto-multithresh parameters are set.
                    if 'startGrowTime' in cycleresults:
                        cycleresults['growTime'] = cycleresults['endGrowTime'] - cycleresults['startGrowTime']

                    if 'startPrune2Time' in cycleresults:
                        # Ditto the comments for prune1Time
                        if 'startNegativeThresholdTime' in cycleresults:
                            cycleresults['prune2Time'] = cycleresults['startNegativeThresholdTime'] - cycleresults['startPrune2Time']
                        else:
                            cycleresults['prune2Time'] = cycleresults['endSmooth2Time'] - cycleresults['startPrune2Time']

                    if 'startSmooth2Time' in cycleresults:
                        cycleresults['smooth2Time'] = cycleresults['endSmooth2Time'] - cycleresults['startSmooth2Time']

                    if 'startNegativeThresholdTime' in cycleresults:
                        cycleresults['negativeThresholdTime'] = cycleresults['endNegativeThresholdTime'] - cycleresults['startNegativeThresholdTime']

                    ## save major cycle information here
                    results[cycle] = cycleresults
                    cycleresults={}

            # if  clean stops  catch this. 
            if endCleanRE.search(line):
                endCleanStr = dateFmtRE.search(line)
                if endCleanStr:
                    cycleresults['endCleanTime'] = datetime.strptime(endCleanStr.group('timedate'),'%Y-%m-%d %H:%M:%S')

                # calculate times. Note that here I need to capture the case where the minor cycle doesn't happen.
                if 'startMinorCycleTime' in cycleresults:
                    cycleresults['totalMaskTime'] = cycleresults['startMinorCycleTime'] - cycleresults['maskStartTime']
                else: 
                    cycleresults['totalMasktime'] = cycleresults['endCleanTime'] - cycleresults['maskStartTime']

                if 'startMajorCycleTime' in cycleresults:
                    cycleresults['cycleTime'] = cycleresults['endCleanTime'] - cycleresults['startMajorCycleTime']
                else:
                    cycleresults['cycleTime'] = cycleresults['endCleanTime'] - cycleresults['maskStartTime']

                cycleresults['thresholdTime'] = cycleresults['endThresholdTime'] - cycleresults['startThresholdTime']

                # inserting this in just in case setting minbeamfrac=0.0 turns off the logger messages. Need to check this.
                if 'startPrune1Time' in cycleresults:
                    if 'startGrowTime' in cycleresults:
                        cycleresults['prune1Time'] = cycleresults['startGrowTime'] - cycleresults['startPrune1Time']
                    else:
                        cycleresults['prune1Time'] = cycleresults['endSmooth1Time'] - cycleresults['startPrune1Time']

                # should always smooth
                cycleresults['smooth1Time'] = cycleresults['endSmooth1Time'] - cycleresults['startSmooth1Time']

                # The following might not always happen depending on how the auto-multithresh parameters are set.
                if 'startGrowTime' in cycleresults:
                    cycleresults['growTime'] = cycleresults['endGrowTime'] - cycleresults['startGrowTime']

                if 'startPrune2Time' in cycleresults:
                    if 'startNegativeThresholdTime' in cycleresults:
                        cycleresults['prune2Time'] = cycleresults['startNegativeThresholdTime'] - cycleresults['startPrune2Time']
                    else:
                        cycleresults['prune2Time'] = cycleresults['endSmooth2Time'] - cycleresults['startPrune2Time']

                if 'startSmooth2Time' in cycleresults:
                    cycleresults['smooth2Time'] = cycleresults['endSmooth2Time'] - cycleresults['startSmooth2Time']

                if 'startNegativeThresholdTime' in cycleresults:
                    cycleresults['negativeThresholdTime'] = cycleresults['endNegativeThresholdTime'] - cycleresults['startNegativeThresholdTime']

                ## save major cycle information here
                results[cycle] = cycleresults
                results['stopreason'] = endCleanRE.search(line).group('stopreason')
                cycleresults={}

            # capture restore time here
            if startRestoreRE.search(line):
                startRestoreStr = dateFmtRE.search(line)
                if startRestoreStr:
                    results['startRestoreTime'] = datetime.strptime(startRestoreStr.group('timedate'),'%Y-%m-%d %H:%M:%S')

            if endRestoreRE.search(line):
                endRestoreStr = dateFmtRE.search(line)
                if endRestoreStr:
                    results['endRestoreTime'] = datetime.strptime(endRestoreStr.group('timedate'), '%Y-%m-%d %H:%M:%S')
                    results['restoreTime'] = results['endRestoreTime'] - results['startRestoreTime']

            # capture the end of the clean
            if tcleanEndRE.search(line):
                endTimeStr = dateFmtRE.search(line)
                if endTimeStr:
                    results['endTime'] = datetime.strptime(endTimeStr.group('timedate'),'%Y-%m-%d %H:%M:%S')

                # calculate overall statistics.
                results['tcleanTime'] = results['endTime']-results['startTime']
                results['ncycle'] = cycle
                results['specmode'] = specmode


                # if iter1 image, save results and clear
                # variables. Include the stopreason variable
                # to avoid the iter1 case where only the common beam is
                # being applied and no cleaning is done.
                if ( re.search('iter1',imagename) and ('stopreason' in results) ):
                    allresults[imagename] = results

                # clear for next clean run
                results = {} 
                cycleresults = {}
                imagename = ''
                cycle = '0'
                specmode=''

    filein.close()

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
                'negativeThresholdTime': [],
                'smooth1Time':[],
                'smooth2Time': [],
                'startRestoreTime':[],
                'endRestoreTime': [],
                'restoreTime':[],
                'noiseTime': [],
                'endNoiseTime':[],
                'modelFlux':[],
                'mpi':[]}
    
    durationKeys = ['negativeThresholdTime','cycleTime','prune2Time','growTime','thresholdTime','prune1Time','totalMaskTime','smooth1Time','smooth2Time','noiseTime']
    cycleKeys = ['startMinorCycleTime', 'cycleTime', 'startPrune2Time', 'startGrowTime', 'totalMaskTime', 'startPrune1Time', 'endMajorCycleTime', 'prune2Time', 'thresholdTime', 'startMajorCycleTime', 'prune1Time', 'maskStartTime', 'growTime','negativeThresholdTime','smooth1Time','smooth2Time','noiseTime','endNoiseTime','modelFlux']

    for (project,images) in inDict.items():
        for (image,mpis) in images.items():
            for (mpi,data) in mpis.items():
                for cycle in map(str,range(0,int(data['ncycle'])+1)):

                    flatDict['project'].append(project)
                    flatDict['imagename'].append(image)
                    flatDict['mpi'].append(mpi)
                    flatDict['cycle'].append(cycle)

                    # Get the base info from the top level of the data structure
                    flatDict['ncycle'].append(data['ncycle'])
                    flatDict['stopreason'].append(data['stopreason'])
                    flatDict['startTime'].append(data['startTime'])
                    flatDict['specmode'].append(data['specmode'])
                    flatDict['tcleanTime'].append(float(data['tcleanTime'].seconds))
                    flatDict['endTime'].append(data['endTime'])
                    flatDict['startRestoreTime'].append(data['startRestoreTime'])
                    flatDict['endRestoreTime'].append(data['endRestoreTime'])
                    flatDict['restoreTime'].append(data['restoreTime'])

                    # doing something a little bit fancy.
                    for akey in cycleKeys:
                        if akey in data[cycle]:
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

def flattenTimingData_serial(inDict):
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
                'negativeThresholdTime': [],
                'smooth1Time':[],
                'smooth2Time': [],
                'startRestoreTime':[],
                'endRestoreTime': [],
                'restoreTime':[],
                'noiseTime': [],
                'endNoiseTime':[],
                'modelFlux':[]}
    
    durationKeys = ['negativeThresholdTime','cycleTime','prune2Time','growTime','thresholdTime','prune1Time','totalMaskTime','smooth1Time','smooth2Time','noiseTime']
    cycleKeys = ['startMinorCycleTime', 'cycleTime', 'startPrune2Time', 'startGrowTime', 'totalMaskTime', 'startPrune1Time', 'endMajorCycleTime', 'prune2Time', 'thresholdTime', 'startMajorCycleTime', 'prune1Time', 'maskStartTime', 'growTime','negativeThresholdTime','smooth1Time','smooth2Time','noiseTime','endNoiseTime','modelFlux']

    for (project,images) in inDict.items():
        for (image,data) in images.items():
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
                flatDict['startRestoreTime'].append(data['startRestoreTime'])
                flatDict['endRestoreTime'].append(data['endRestoreTime'])
                flatDict['restoreTime'].append(data['restoreTime'])

                # doing something a little bit fancy.
                for akey in cycleKeys:
                    if akey in data[cycle]:
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

def createBatchScript(testDir, casaPath, scriptname=None, mpi=False,n=8, stage=None):
    '''
    generate a pipeline batch script quickly to send jobs to nodes
    '''

    import os
    import glob
    import re

    projectRE = re.compile("(?P<project>\w{4}\.\w\.\d{5}\.\w_\d{4}_\d{2}_\d{2}T\d{2}_\d{2}_\d{2}\.\d{3})")

    if os.path.exists(testDir):
        
        if stage:
            pipename = 'pipelinerun.stage'+str(stage)

        else:
            pipename = 'pipelinerun'

        if mpi:
            pipename = pipename+".parallel"
        else:
            pipename = pipename+".serial"

        f = open(os.path.join(testDir,pipename),'w')
        dataDirs = os.listdir(testDir)
        for mydir in dataDirs:
            if projectRE.match(mydir):
                project = projectRE.match(mydir).group('project')
                projectDir = os.path.join(testDir,mydir)

                basescriptname = os.path.join(projectDir,project)

                if stage and scriptname:
                    script = basescriptname+'_stage'+str(stage)+'_'+scriptname+'.py'
                elif stage and not scriptname:
                    script = basescriptname+'_stage'+str(stage)+'.py'
                elif not stage and scriptname:
                    script = basescriptname+'_'+scriptname+'.py'
                else:
                    script = basescriptname+'.py'

                
                if mpi:
                    outline = "cd " +projectDir + \
                              "; export PATH="+os.path.join(casaPath, "bin") + ":${PATH}" + \
                              "; xvfb-run -d mpicasa -n "+str(n)+" casa --nogui -c " + script +"\n"

                else:

                    outline = "cd " +projectDir + \
                              "; export PATH="+os.path.join(casaPath, "bin") + ":${PATH}" + \
                              "; xvfb-run -d casa --nogui -c " + script +"\n"

                f.write(outline)
        f.close()


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
        print("Couldn't open file: " + casalogfile)


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
        print("Data directory doesn't exist: " + dataDir)
            
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

    # Note: this could probably be modified to use modify parameters.
    
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
        print("Given robust value not found in beam info file: ", beamInfo)
        print("exiting")
    
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
                        print("Not changing script. Input and output robust are the same",robust)
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


def addParameters(inScript,outScript,parameters):
    ''' 
    Add a parameter to an existing tclean script
    '''

    # Purpose: add a parameter to an existing tclean script

    # Input:
    #   inScript

    # Output:
    #  outScript

    # Notes: This is potentially superseded by modifyParameters.

    # Date              Programmer      Description of Changes
    # ------------------------------------------------------------------------
    # 7/27/2018         A.A. Kepley     Original Code
    
    import re
    import os.path
    import pdb
    import math

    
    tcleanCmd = re.compile(r"""
    (?P<cmd>tclean\(   ## tclean command
    .*\) ## end of tclean command
    ) 
    """,re.VERBOSE)

    if os.path.exists(inScript):
        filein = open(inScript,'r')
        fileout = open(outScript,'w')
        
        for line in filein:
            findtclean = tcleanCmd.search(line)

            if findtclean:
                # just add the new parameters onto the end of the tclean command
                outline = line.replace(')',', '+parameters+')')                
                fileout.write(outline)
            else:
                fileout.write(line)

        filein.close()
        fileout.close()
                
#----------------------------------------------------------------------

def modifyParameters(inScript,outScript, parameters, delparams=None, removerestart=False):
    '''

    modify the parameter values

    parameters is a set of tuples giving the parameter name and the
    value to be changed to.

    NO PARAMETER CHECKING IS DONE. IF YOU'RE USING THIS, YOU SHOULD
    KNOW WHAT YOU'RE DOING.

    '''

    # Purpose: modify parameter values in an existing tclean script
    
    # Input:
    #   inScript


    # Output:
    #   outScript

    # Method: I'm giving a list of tuples describing key value pairs
    #           here, so I can change multiple parameters at
    #           once. I've also made it so parameters that don't show
    #           up are added.
    #   

    # Date              Programmer              Description of Changes
    #----------------------------------------------------------------------
    # 7/31/2018         A.A. Kepley             original code

    import re
    import os.path
    import pdb
    import math

    tcleanCmd = re.compile(r"""
    (?P<cmd>tclean\(   ## tclean command
    .*\) ## end of tclean command
    ) 
    """,re.VERBOSE)
    
    #tcleanRestart = re.compile(r"tclean\(*., restoringbeam=\'common\', *., niter=0, *., parallel=False\)")
    #tcleanRestart = re.compile(r"restoringbeam='common', *., niter=0, *., parallel=False")
    tcleanRestart = re.compile(r"specmode='cube', .*, restoringbeam='common', .*, niter=0, .*, parallel=False\)")

    cubeRE = re.compile(r"specmode='(cube|cubesource)'")

    if os.path.exists(inScript):
        filein = open(inScript,'r')
        fileout = open(outScript,'w')

        for line in filein:                    
            if tcleanCmd.search(line):
                # modify the line for each parameter we want
                # change. This may not be the most efficient.

                if removerestart:
                    if tcleanRestart.search(line):
                        print("removing restart")
                        continue

                for (key,value) in parameters:
                    newstr = key+'='+str(value)
                    mymatch = re.search("(?P<mykey>"+key+"=.*?)[,|\)]",line)           
                 
                    #breakpoint()

                    if mymatch:
                        if (((value == "'briggsbwtaper'") or (key == 'perchanweightdensity')) and (not cubeRE.search(line))):
                            print("briggsbwtaper or perchanweightdensity selected and image isn't cube. not modifying command'")
                            line = line
                        else:
                            print("match found! modifying input parameter" )
                            line = line.replace(mymatch.group('mykey'),newstr)
                    else:
                        print("no match found for "+key)
                        print("adding to parameter list")
                        line = line.replace(')',', '+newstr+')')

                for key in delparams:
                    mymatch = re.search("(?P<mykey>"+key+"=.*?)[,|\)]",line)
                    if mymatch:
                        print("deleting parameter")
                        line = line.replace(mymatch[0],'')
    
            fileout.write(line)


        filein.close()
        fileout.close()

    
                
    
#----------------------------------------------------------------------

def setupNewParameterTest(benchmarkDir, testDir, parameters, scriptID,delparams=None, removerestart=False):
    '''
    setup a test where I have added additional parameters to the data set.
    '''

    # Input:
    #   benchmarkDir: directory with benchmarks
    #   testDir: directory to run test in

    #   parameters: list of tuples with (key,value) for each paraemter to change. 
    #     Note NO checks are done on whether the parameters are valid.

    # Output:
    #   scripts and directory structure for test

    # Date      Programmer              Description of Code
    #---------------------------------------------------7-------------------
    # 7/27/2018 A.A. Kepley             Original Code

    import shutil
    import glob
    import os
    import os.path
    import pdb

    # if the benchmark directory exists
    if os.path.exists(benchmarkDir):
        
        # get all benchmarks
        dataDirs = os.listdir(benchmarkDir)
        
        # go to test directory, create a directory structure, and copy scripts
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
                myOutScript = myscript.replace('.py','_'+scriptID+'.py') 
                
                scriptDir = os.path.join(testDir,mydir)
                scriptPath = os.path.join(scriptDir,os.path.basename(myOutScript))

                #if not os.path.isfile(scriptPath):
                modifyParameters(myscript,myOutScript,parameters,delparams=delparams, removerestart=removerestart)
                shutil.copy(myOutScript,scriptDir)

        # switch back to original directory
        os.chdir(currentDir)

#----------------------------------------------------------------------

def split_mpi_logs(log,n=8):

    '''
    split up mpi logs so that they make more sense

    inlog: input log
    n: number of cores, set to 8 by default. number of mpi servers is n-1

    '''

    import re

    # opening up log files
    inlog = open(log,'r')
    outlog_mpi = [open(log.replace('.log','_mpi'+str(i)+'.log'),'w') for i in range(n)]

    # setting up the regular expression
    mpilineRE = re.compile(r"MPIServer-(?P<mpi>\d+)")

    for line in inlog:
        if mpilineRE.search(line):
            mpi = int(mpilineRE.search(line).group('mpi'))
            outlog_mpi[mpi].write(line)
        else:
            for i in range(0,n):
                outlog_mpi[i].write(line)

    # closing everything up
    inlog.close()
    for fh in outlog_mpi:
        fh.close()


# ----------------------------------------------------------------------

def parseLog_newlog_simple(logfile,serial=False):
    '''

    Parse an individual log file and return an object with the data in it

    serial = True get stats from return dictionary.

    '''
    
    import re
    from datetime import datetime
    import copy
    import ast
    import numpy as np
    #import ipdb

    # regex patterns for below.
    tcleanBeginRE = re.compile(r"Begin Task: tclean")

    imagenameRE = re.compile(r'imagename=(\"|\')(?P<imagename>.*?)(\"|\')')
    specmodeRE = re.compile(r'specmode=(\"|\')(?P<specmode>.*?)(\"|\')')

    imsizeRE = re.compile(r'imsize=\[(?P<imsize1>.*?),(?P<imsize2>.*?)\]')
    nchanRE = re.compile(r'nchan=(?P<nchan>.*?),')

    commonbeamRE = re.compile("restoringbeam='common', .*, parallel=False")

    gridderRE = re.compile(r"gridder=\'(?P<gridder>.*?)\'")

    majorCycleRE = re.compile(r"Major Cycle (?P<nmajor>\d*)")

    tcleanEndRE = re.compile(r"End Task: tclean")
    
    dateFmtRE = re.compile(r"(?P<timedate>\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2})")
    antennaRE = re.compile(r"antenna=\[\'(?P<antennalist>.*?)\'")    

    chanchunksRE = re.compile(r"INFO\tSynthesisImagerVi2::appendToMapperList\(ftm\)\+\tSetting chanchunks to (?P<chanchunks>.*?)\n")

    nrowsRE = re.compile(r"SynthesisImagerVi2::selectData 	  NRows selected : (?P<nrows>.*?)\n")

    # Hmm... I'm not sure this makes sense here. The old parallel is
    # reports per node, which makes it harder to parse the
    # output. Maybe only good for serial cross-checks?
 
    resultTcleanRE = re.compile(r"Result tclean: \{.*, 'cyclethreshold': (?P<cyclethreshold>.*?), .*, 'iterdone': (?P<iterdone>.*?), .*, 'nmajordone': (?P<nmajordone>.*?), .*, 'stopcode': (?P<stopcode>.*?),")

    stoppingRE = re.compile(r"Reached global stopping criterion : (?P<stopcode>.*)")

    # open file
    filein = open(logfile,'r')

    #initialize loop status        
    imagename = ''
    allresults = {}
    results = {}
    specmode=''
    commonbeam = False
    nrows = np.array([],dtype='int')

    # go through file
    for line in filein:

        # capture start of tclean
        if tcleanBeginRE.search(line):
            startTimeStr = dateFmtRE.search(line)
            if startTimeStr:                
                results['startTime'] = datetime.strptime(startTimeStr.group('timedate'),'%Y-%m-%d %H:%M:%S')

        # capture image name
        if imagenameRE.search(line):
            imagename = imagenameRE.search(line).group('imagename')

        if commonbeamRE.search(line):
            commonbeam=True

        # capture line vs. continuum
        if specmodeRE.search(line):
            if (re.match(specmodeRE.search(line).group('specmode'),'cube')):
                results['specmode']='cube'
            elif (re.match(specmodeRE.search(line).group('specmode'),'cubesource')):
                results['specmode']='cubesource'            
            else:
                results['specmode']='cont'

        # capture imsize
        if imsizeRE.search(line):
            results['imsize'] = [int(imsizeRE.search(line).group('imsize1')),int(imsizeRE.search(line).group('imsize2'))] 

        # capture nchan
        if nchanRE.search(line):
            results['nchan'] = int(nchanRE.search(line).group('nchan'))

        if gridderRE.search(line):
            results['gridder'] = gridderRE.search(line).group('gridder')

        if antennaRE.search(line):
            antennalist = ast.literal_eval(antennaRE.search(line).group('antennalist').replace('&',''))
            # crude and not necessarily true for early data, but
            # should be okay now.
            if len(antennalist) > 16:
                results['array'] = '12m'
            else:
                results['array'] = '7m'

        if chanchunksRE.search(line):
            results['chanchunks'] = int(chanchunksRE.search(line).group('chanchunks'))

        if serial and resultTcleanRE.search(line):
            results['cyclethreshold'] = float(resultTcleanRE.search(line).group('cyclethreshold'))
            results['iterdone'] = int(resultTcleanRE.search(line).group('iterdone'))
            results['nmajordone'] = int(resultTcleanRE.search(line).group('nmajordone'))
            results['stopcode'] = int(resultTcleanRE.search(line).group('stopcode'))

        if nrowsRE.search(line):
            nrows = np.append(nrows,int(nrowsRE.search(line).group('nrows')))

        if majorCycleRE.search(line):
            nmajor = majorCycleRE.search(line).group('nmajor')

        if stoppingRE.search(line):
            results['stopcode'] = stoppingRE.search(line).group('stopcode')


        # capture the end of the clean
        if tcleanEndRE.search(line):
            endTimeStr = dateFmtRE.search(line)
            if endTimeStr:
                results['endTime'] = datetime.strptime(endTimeStr.group('timedate'),'%Y-%m-%d %H:%M:%S')

            # calculate overall statistics.
            results['tcleanTime'] = results['endTime']-results['startTime']


            if results['specmode'] == 'cube':
                results['totalSize'] = results['imsize'][0] * results['imsize'][1] * results['nchan']
            else:
                results['totalSize'] = results['imsize'][0] * results['imsize'][1] 
                
            results['aspectRatio'] = float(max(results['imsize']))/float(min(results['imsize']))

            ## deal with restart to get common beam.            
            if commonbeam and (results['specmode'] == 'cube'):
                if re.search('iter0',imagename):
                    imagename = imagename.replace('iter0','iter2')
                elif re.search('iter1',imagename):
                    imagename = imagename.replace('iter1','iter2')
                results['iter'] = 'iter2'
            else:
                if re.search('iter0',imagename):
                    results['iter'] = 'iter0'
                elif re.search('iter1',imagename):
                    results['iter'] = 'iter1'

            if 'chanchunks' not in results.keys():
                results['chanchunks'] = 0

            if results['iter'] is not 'iter2':
                results['nrows'] = np.sum(nrows)
            else:
                results['nrows'] = np.sum(nrows[0:int(len(nrows)/2.0)])

            results['nmajor'] = nmajor

            allresults[imagename] = results

            results = {}
            specmode=''
            imagename=''
            commonbeam=False
            nmajor = ''
            nrows = np.array([],dtype='int')

    filein.close()

    return allresults

#----------------------------------------------------------------------

def tCleanTime_newlogs_simple(testDir,serial=False):
    '''
    Time how long tclean takes
    '''

    # Purpose: mine logs for information about timing.
    
    # Input: 

    #   testDir: I'm assuming that the testDirectory contains one
    #   casalog file, but may want to add the option to specify a log

    # Output:

    #    a structure with all the timing information, plus vital stats
    #    on the data set. 
    #
    #  Date             Programmer              Description of Changes
    #----------------------------------------------------------------------
    # 2/24/2020         A.A. Kepley             Original Code based on tCleanTime_newlogs

    import os
    import os.path
    import glob
    import re
    #import ipdb


    if os.path.exists(testDir):
    
        # initialize lists
        allresults = {}

        # get the file name 
        logfilelist = glob.glob(os.path.join(testDir,"casa-????????-??????.log"))

        # go through logs and extract info.
        for logfile in logfilelist:

            allresults = parseLog_newlog_simple(logfile,serial=serial)
    else:
        print("no path found")
        allresults = {}
            
    return allresults

#----------------------------------------------------------------------


def flattenTimingData_simple(inDict):
    '''
    flatten the simple timing data
    '''

    import numpy as np

    flatDict = {'project': [],
                'imagename': [],
                'specmode':[],
                'imsize':[],
                'aspectRatio': [],
                'nchan':[],
                'gridder':[],
                'array':[],
                'tcleanTime':[],
                'totalSize':[],
                'chanchunks':[]}

    for (project,images) in inDict.items():
        for (image,data) in images.items():

            for key in flatDict.keys():
                if key is 'tcleanTime':
                    flatDict[key].append(float(data[key].seconds))
                elif key is 'project':
                    flatDict['project'].append(project)
                elif key is 'imagename':
                     flatDict['imagename'].append(image)
                else:
                    flatDict[key].append(data[key])


    return flatDict

#----------------------------------------------------------------------
            
def makeAstropyTimingTable(inDict1,inDict2,label1='casa610',label2='build84',serial=False):
    '''
    make an astropy table of the timing data for (hopefully) easier plotting
    '''
    
    from astropy.table import Table
    import numpy as np
    import numpy.ma as ma
    import re

    projectArr = np.array([])
    imagenameArr = np.array([])
    specmodeArr = np.array([])
    imsize1Arr = np.array([])
    imsize2Arr = np.array([])
    nchanArr = np.array([])
    gridderArr = np.array([])
    arrayArr = np.array([])
    tcleanTime1Arr = np.array([])
    tcleanTime2Arr = ma.array([])
    tcleanTime2MaskArr = np.array([])
    totalSizeArr = np.array([])
    iterArr =  np.array([])
    pdiffArr = ma.array([])
    pdiffMaskArr = np.array([])
    aspectRatioArr = np.array([])
    chanchunksArr = np.array([])
    nrowsArr = np.array([])

    if serial:
        cyclethreshold1Arr = ma.array([])
        cyclethreshold1MaskArr  = np.array([])
        iterdone1Arr = ma.array([])
        iterdone1MaskArr = np.array([])
        nmajordone1Arr = ma.array([])
        nmajordone1MaskArr = np.array([])
        stopcode1Arr = ma.array([])
        stopcode1MaskArr = np.array([])
        
        cyclethreshold2Arr = ma.array([])
        cyclethreshold2MaskArr  = np.array([])
        iterdone2Arr = ma.array([])
        iterdone2MaskArr = np.array([])
        nmajordone2Arr = ma.array([])
        nmajordone2MaskArr = np.array([])
        stopcode2Arr = ma.array([])
        stopcode2MaskArr = np.array([])

    for project in inDict1.keys():
        if project in inDict2.keys():
            for image in inDict1[project].keys():
                    projectArr = np.append(project,projectArr)
                    imagenameArr = np.append(image,imagenameArr)

                    iterArr = np.append(inDict1[project][image]['iter'],iterArr)

                    specmodeArr = np.append(inDict1[project][image]['specmode'],specmodeArr)
                    imsize1Arr = np.append(inDict1[project][image]['imsize'][0],imsize1Arr)
                    imsize2Arr = np.append(inDict1[project][image]['imsize'][1],imsize2Arr)
                    aspectRatioArr =np.append(inDict1[project][image]['aspectRatio'],aspectRatioArr)
                    nchanArr = np.append(inDict1[project][image]['nchan'],nchanArr)
                    gridderArr = np.append(inDict1[project][image]['gridder'],gridderArr)
                    arrayArr = np.append(inDict1[project][image]['array'],arrayArr)
                    totalSizeArr = np.append(inDict1[project][image]['totalSize'],totalSizeArr)

                    tcleanTime1 = float(inDict1[project][image]['tcleanTime'].total_seconds())
                    tcleanTime1Arr = np.append(tcleanTime1,tcleanTime1Arr)

                    chanchunksArr = np.append(inDict1[project][image]['chanchunks'],chanchunksArr)

                    nrowsArr = np.append(inDict1[project][image]['nrows'],nrowsArr)

                    if image in inDict2[project]:

                        tcleanTime2 = float(inDict2[project][image]['tcleanTime'].total_seconds())
                        pdiff = 100.0* (tcleanTime2 - tcleanTime1) / tcleanTime1

                        tcleanTime2Arr = ma.append(tcleanTime2,tcleanTime2Arr)
                        pdiffArr = ma.append(pdiff, pdiffArr)

                        tcleanTime2MaskArr = np.append(False,pdiffMaskArr)
                        pdiffMaskArr = np.append(False,pdiffMaskArr)


                    else: 
                        tcleanTime2Arr = ma.append(99,tcleanTime2Arr)
                        pdiffArr = ma.append(99,pdiffArr)

                        tcleanTime2MaskArr = np.append(True,pdiffMaskArr)
                        pdiffMaskArr = np.append(True,pdiffMaskArr)

                    if serial:
                        if ((image in inDict1[project].keys()) and 
                                ('cyclethreshold' in inDict1[project][image].keys())):
                            cyclethreshold1Arr = np.append(inDict1[project][image]['cyclethreshold'], cyclethreshold1Arr)
                            cyclethreshold1MaskArr = np.append(False, cyclethreshold1MaskArr)

                            iterdone1Arr = np.append(inDict1[project][image]['iterdone'], iterdone1Arr)
                            iterdone1MaskArr = np.append(False, iterdone1MaskArr)

                            nmajordone1Arr = np.append(inDict1[project][image]['nmajordone'], nmajordone1Arr)
                            nmajordone1MaskArr = np.append(False, nmajordone1MaskArr)

                            stopcode1Arr = np.append(inDict1[project][image]['stopcode'], stopcode1Arr)
                            stopcode1MaskArr = np.append(False, stopcode1MaskArr)

                        else:
                            cyclethreshold1Arr = np.append(99, cyclethreshold1Arr)
                            cyclethreshold1MaskArr = np.append(True, cyclethreshold1MaskArr)

                            iterdone1Arr = np.append(99, iterdone1Arr)
                            iterdone1MaskArr = np.append(True, iterdone1MaskArr)

                            nmajordone1Arr = np.append(99, nmajordone1Arr)
                            nmajordone1MaskArr = np.append(True, nmajordone1MaskArr)

                            stopcode1Arr = np.append(99, stopcode1Arr)
                            stopcode1MaskArr = np.append(True, stopcode1MaskArr)


                            # mask
                        if ((image in inDict2[project].keys()) and 
                                ('cyclethreshold' in inDict2[project][image].keys())):
                            cyclethreshold2Arr = np.append(inDict2[project][image]['cyclethreshold'], cyclethreshold2Arr)
                            cyclethreshold2MaskArr = np.append(False, cyclethreshold2MaskArr)

                            iterdone2Arr = np.append(inDict2[project][image]['iterdone'], iterdone2Arr)
                            iterdone2MaskArr = np.append(False, iterdone2MaskArr)

                            nmajordone2Arr = np.append(inDict2[project][image]['nmajordone'], nmajordone2Arr)
                            nmajordone2MaskArr = np.append(False, nmajordone2MaskArr)

                            stopcode2Arr = np.append(inDict2[project][image]['stopcode'], stopcode2Arr)
                            stopcode2MaskArr = np.append(False, stopcode2MaskArr)

                        else:
                            #mask

                            cyclethreshold2Arr = np.append(99, cyclethreshold2Arr)
                            cyclethreshold2MaskArr = np.append(True, cyclethreshold2MaskArr)

                            iterdone2Arr = np.append(99, iterdone2Arr)
                            iterdone2MaskArr = np.append(True, iterdone2MaskArr)

                            nmajordone2Arr = np.append(99, nmajordone2Arr)
                            nmajordone2MaskArr = np.append(True, nmajordone2MaskArr)

                            stopcode2Arr = np.append(99, stopcode2Arr)
                            stopcode2MaskArr = np.append(True, stopcode2MaskArr)


        tcleanTime2Arr.mask = tcleanTime2MaskArr
        pdiffArr.mask = pdiffMaskArr

        if serial:
            cyclethreshold1Arr.mask = cyclethreshold1MaskArr
            iterdone1Arr.mask = iterdone1MaskArr
            nmajordone1Arr.mask = nmajordone1MaskArr
            stopcode1Arr.mask = stopcode1MaskArr
            
            cyclethreshold2Arr.mask = cyclethreshold2MaskArr
            iterdone2Arr.mask = iterdone2MaskArr
            nmajordone2Arr.mask = nmajordone2MaskArr
            stopcode2Arr.mask = stopcode2MaskArr

    t = Table([projectArr,
               imagenameArr,
               iterArr,
               specmodeArr,            
               imsize1Arr,
               imsize2Arr,
               aspectRatioArr,
               nchanArr,
               gridderArr,
               arrayArr,
               totalSizeArr,
               tcleanTime1Arr,
               tcleanTime2Arr,
               pdiffArr,
               chanchunksArr,
               nrowsArr],
              names=('project','imagename','iter','specmode','imsize1','imsize2','aspectRatio','nchan','gridder','array','totalSize','tcleanTime_'+label1, 'tcleanTime_'+label2,'pdiff','chanchunks','nrows'),masked=True)


    if serial:
        t.add_column(cyclethreshold1Arr,name='cyclethreshold_'+label1)
        t.add_column(iterdone1Arr,name='iterdone_'+label1)
        t.add_column(nmajordone1Arr,name='nmajordone_'+label1)
        t.add_column(stopcode1Arr,name='stopcode_'+label1)

        t.add_column(cyclethreshold2Arr,name='cyclethreshold_'+label2)
        t.add_column(iterdone2Arr,name='iterdone_'+label2)
        t.add_column(nmajordone2Arr,name='nmajordone_'+label2)
        t.add_column(stopcode2Arr,name='stopcode_'+label2)
    

    totalTime1 = np.zeros(len(projectArr))
    totalTime2 = np.zeros(len(projectArr))
    totalTime_pdiff = np.zeros(len(projectArr))

    for entry in t:
        if entry['iter'] == 'iter0':
            
            # looking for corresponding images.
            idx_iter0 = ((t['project'] == entry['project'] ) & 
                         (t['imagename'] == entry['imagename']))

            idx_iter1 = ((t['project'] == entry['project']) &
                         (t['imagename'] == entry['imagename'].replace('iter0','iter1')))

            idx_iter2 = ((t['project'] == entry['project']) &
                         (t['imagename'] == entry['imagename'].replace('iter0','iter2')))

            # check if iter2  and iter2 present
            if t[idx_iter2] and t[idx_iter1]:                
                
                totalTime1[idx_iter0] = t[idx_iter2]['tcleanTime_'+label1] + t[idx_iter1]['tcleanTime_'+label1]+t[idx_iter0]['tcleanTime_'+label1]                

                if  t[idx_iter2]['tcleanTime_'+label2].mask:
                    totalTime2[idx_iter0] = t[idx_iter1]['tcleanTime_'+label2]+t[idx_iter0]['tcleanTime_'+label2]                    
                else:
                    totalTime2[idx_iter0] = t[idx_iter2]['tcleanTime_'+label2] + t[idx_iter1]['tcleanTime_'+label2]+t[idx_iter0]['tcleanTime_'+label2]
            # only iter two
            elif t[idx_iter2]:
                totalTime1[idx_iter0] = t[idx_iter2]['tcleanTime_'+label1] +t[idx_iter0]['tcleanTime_'+label1]                

                if  t[idx_iter2]['tcleanTime_'+label2].mask:
                    totalTime2[idx_iter0] = t[idx_iter0]['tcleanTime_'+label2]                    
                else:
                    totalTime2[idx_iter0] = t[idx_iter2]['tcleanTime_'+label2] + t[idx_iter0]['tcleanTime_'+label2]

            # if not fall back to iter1
            elif t[idx_iter1]:

                totalTime1[idx_iter0] = t[idx_iter1]['tcleanTime_'+label1]+t[idx_iter0]['tcleanTime_'+label1]

                totalTime2[idx_iter0] = t[idx_iter1]['tcleanTime_'+label2]+t[idx_iter0]['tcleanTime_'+label2]

            # otherwise no cleaning done, so iter0
            else:

                totalTime1[idx_iter0] = t[idx_iter0]['tcleanTime_'+label1]

                totalTime2[idx_iter0] = t[idx_iter0]['tcleanTime_'+label2]              
            # calculate difference in total time.
            totalTime_pdiff[idx_iter0] = 100.0* ( totalTime2[idx_iter0]-totalTime1[idx_iter0])/totalTime1[idx_iter0]

            # copy info from iter1 to iter0
            if t[idx_iter1]:
                ## NEED TO DO LABEL THEN IDX TO GET CORRECT COPY
                t['nmajordone_'+label2][idx_iter0] = t['nmajordone_'+label2][idx_iter1]
            else:
                t['nmajordone_'+label2][idx_iter0] = 0
            
        else:
            continue

   
    t.add_column(totalTime1,name='totalTime_'+label1)
    t.add_column(totalTime2,name='totalTime_'+label2)
    t.add_column(totalTime_pdiff, name='totalTime_pdiff')


    return t
                    
# ----------------------------------------------------------------------

def generatePipeScript(dataDir, outDir, scriptName='test.py',mfsParameters=None,cubeParameters=None):
    '''
    generate a simple imaging-only pipeline script
    '''
    
    import os

    scriptStr = '''
import glob
import os
    
_rethrow_casa_exceptions = True
    
dataDir = '{0:s}'

context = h_init()
    
try:
    myvis = glob.glob(os.path.join(dataDir,"*target.ms"))
    hifa_importdata(vis=myvis,pipelinemode="automatic",dbservice=False, asimaging=True)
'''.format(dataDir)

    parameterStr = ''

    if mfsParameters:
        
        for param in mfsParameters:         
            for key in param.keys():                
                if parameterStr:
                    parameterStr = parameterStr+','+key+'='+str(param[key])
                else:
                    parameterStr = key+'='+str(param[key])

        mfsImagingStr = '''
    hif_makeimlist(specmode='mfs')
    hif_makeimages(pipelinemode="automatic",{0:s})
    hif_makeimlist(specmode='cont')
    hif_makeimages(pipelinemode="automatic",{0:s})   
'''.format(parameterStr)
    else:
        mfsImagingStr = '''
    hif_makeimlist(specmode='mfs')
    hif_makeimages(pipelinemode="automatic")
    hif_makeimlist(specmode='cont')
    hif_makeimages(pipelinemode="automatic")
'''

    parameterStr=''
        
    if cubeParameters:
        
        for param in cubeParameters:         
            for key in param.keys():                
                if parameterStr:
                    parameterStr = parameterStr+','+key+'='+str(param[key])
                else:
                    parameterStr = key+'='+str(param[key])

        cubeImagingStr = '''
    hif_makeimlist(specmode='cube')
    hif_makeimages(pipelinemode="automatic",{0:s})
    #hif_makeimlist(specmode='repBW')
    #hif_makeimages(pipelinemode="automatic",{0:s})
    hifa_exportdata(pipelinemode="automatic")
finally:
    h_save()
'''.format(parameterStr)
    else:
         cubeImagingStr = '''
    hif_makeimlist(specmode='cube')
    hif_makeimages(pipelinemode="automatic")
    #hif_makeimlist(specmode='repBW')
    #hif_makeimages(pipelinemode="automatic")
    hifa_exportdata(pipelinemode="automatic")
finally:
    h_save()
'''.format(parameterStr)
        

    fout = open(os.path.join(outDir,scriptName),'w')
    fout.write(scriptStr)
    fout.write(mfsImagingStr)
    fout.write(cubeImagingStr)
    fout.close()


#----------------------------------------------------------------------

def setupPipeTest(benchmarkDir, testDir, mfsParameters=None, cubeParameters=None):
    '''
    Automatically populate a test directory with directories for
    individual data sets and copies over the relevant pipeline scripts.
    
    '''

    import shutil
    import glob
    import os
    import os.path
    import re
    
    projectRE = re.compile("(?P<project>\w{4}\.\w\.\d{5}\.\w_\d{4}_\d{2}_\d{2}T\d{2}_\d{2}_\d{2}\.\d{3})")

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
            
             scriptDir = os.path.join(testDir,mydir)
             dataDir = os.path.join(benchmarkDir,mydir)

             project = projectRE.match(mydir).group('project')

             generatePipeScript(dataDir, scriptDir, scriptName=project+'.py',cubeParameters=cubeParameters,mfsParameters=mfsParameters)

             shutil.copy(os.path.join(dataDir,'cont.dat'),
                         os.path.join(scriptDir,'cont.dat'))

        # switch back to original directory
        os.chdir(currentDir)


#----------------------------------------------------------------------

