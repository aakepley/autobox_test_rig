# The goal of routines here is to go through logs and pull out
# information. They are likely one-offs, but I want to the aU's that
# Todd has developed to make my life easier, so I'm doing this in
# CASA.

from taskinit import *
ia = iatool()

import os.path
import numpy as np
import pdb
import re
import glob
import analysisUtils as au  

import test_rig



def divergence_hunt(root,outfile='test.log',stage=35,spw=None):

    '''

    Process a log file from a pipeline run to find divergence
    issues. If the input ends in ".log" we assume a log file is
    given. If it doesn't, we assume it's the root directory of a
    pipeline run and file the file that way.

    You need to specify the stage to look it.

    '''

    # find file
    if root.endswith('.log'):
        logfile = root
    else:
        weblog = au.findWeblog(root)
        logfile = os.path.join(weblog,'html','stage'+str(stage),'casapy.log')
       

    # compile the patterns we're interested in
    tcleanCmd = re.compile(r"""
    (?P<cmd>tclean\(   ## tclean command
        .*                 ## skip junk in command
    imagename='(?P<imagename>.*?)' ## imagename
    .*
    calcpsf=False
    .*\) ## end of tclean command
    ) 
    """,re.VERBOSE)
    niter0RE = re.compile(r"niter=0")

    posDivRE = re.compile(r"Possible divergence")
    peakResidRE = re.compile(r"Peak residual")
    exitCriteriaRE = re.compile (r"Reached global stopping criterion")
    maskWarnRE = re.compile(r"No automatic clean mask was found")

    majorCycleRE = re.compile(r'Major Cycle')
    minorCycleRE = re.compile(r'Run Minor Cycle Iterations')
    cyclethresholdRE = re.compile(r'Run Hogbom minor-cycle')
    
    mpilineRE = re.compile(r"MPIServer-(?P<mpi>\d+)")

    # initialize variables
    imagename=''
    printline=False
    mpimess = {}


    if os.path.exists(logfile):
        filein  = open(logfile,'r')
        for line in filein:
            findtclean = tcleanCmd.search(line)

            # find the tclean command responsible for the actual clean and trigger printing
            if (findtclean and not niter0RE.search(line)): 

                imagename =  findtclean.group('imagename')

                # check to see if this is the spw we want.
                if spw:
                    if re.search('spw'+str(spw),imagename):
                        printline=True
                    else:
                        printline=False
                else:
                    printline=True

                # save data if we want to
                if printline:
                    mpimess[imagename] = {'0':[]}
                    mpimess[imagename]['0'].append(line)

            # print matching output lines
            if printline:

                # if things match what I'm looking for
                if (posDivRE.search(line) or 
                    peakResidRE.search(line) or 
                    exitCriteriaRE.search(line) or 
                    majorCycleRE.search(line) or 
                    minorCycleRE.search(line) or
                    cyclethresholdRE.search(line)):
                    
                    # save to appropriate mpi message log
                    if mpilineRE.search(line):
                        mpinum = mpilineRE.search(line).group('mpi')
                        if not mpimess[imagename].has_key(mpinum):
                            mpimess[imagename][mpinum] = []
                        mpimess[imagename][mpinum].append(line)
                    else:                        
                        mpimess[imagename]['0'].append(line)                        

            if maskWarnRE.search(line):
                mpimess[imagename]['0'].append("\n")
                mpimess[imagename]['0'].append(line)      

            # turn off printing when you hit the final tclean command
            if (findtclean and niter0RE.search(line)):
                printline=False

        # close the input file
        filein.close()
        
        # write out results to outfile
        fileout = open(outfile,'w')
        for image in sorted(mpimess.keys()):

            fileout.write('\n')
            fileout.write('---------------------------------------------------------------------------\n')
            fileout.write('      ' + image + '      \n')
            fileout.write('---------------------------------------------------------------------------\n')
            fileout.write('\n')

            for mpi in sorted(mpimess[image].keys()):

                if int(mpi) > 0:

                    fileout.write('\n')
                    fileout.write('MPIServer-'+mpi+':\n')
                    fileout.write('\n')

                for line in mpimess[image][mpi]:
                    fileout.write(line)

    else: 
        print "logfile does not exist:", logfile
        return

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
