# The purpose of this code is to provide some diagnostic information
# on how the mask worked. Right now all I have here is a mask
# comparison tool, but I want to expand to include some plots.

# The code is designed to be run from within casa because it's easier
# to access the pipeline produces that way, since they are CASA image
# files rather than fits files.

# casa --nologger --log2term

# creating an ia tool to use.
from taskinit import *
ia = iatool()

import os.path
import numpy as np
import pdb
import re
import glob
import analysisUtils as au  

def compareMask(baseMaskPath,testMaskPath,writer,label=None):
    '''
    This chunk of code does the comparison on two different images
    '''

    # Input: 
    #   baseMaskPath: fidicual mask
    #   testMaskPath: test mask
    #   writer: where to write the results to.
    #
    # Output:
    #   diffImage
    #   results to writer
    # 
    # Notes:
    #   Should I assume that the files have alaredy been tested for existance?
    #
    #   Date    Programmer              Description of Changes
    #----------------------------------------------------------------------
    # 7/31/2018 A.A. Kepley             Original Code

    import csv


    mask = os.path.basename(baseMaskPath).replace('.mask','')
    mydir = os.path.basename(os.path.dirname(baseMaskPath))

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
        
        # pdb.set_trace

        if nPixMask > 0:
            fracDiff = nPixDiff/nPixMask
            if label:
                writer.writerow([label,mydir,mask,nPixMask,nPixMaskTest,nPixDiff,nPixDiffBase,nPixDiffTest,fracDiff])
            else:
                 writer.writerow([mydir,mask,nPixMask,nPixMaskTest,nPixDiff,nPixDiffBase,nPixDiffTest,fracDiff])
        else:
            if label:
                writer.writerow([label,mydir,mask,nPixMask,nPixMaskTest,nPixDiff,nPixDiffBase,nPixDiffTest, '--'])
            else:
                writer.writerow([mydir,mask,nPixMask,nPixMaskTest,nPixDiff,nPixDiffBase,nPixDiffTest, '--'])

    else:
         print "no corresponding test image: ", testMaskPath
         #writer.writerow([mydir,mask,nPixMask,'--','--','--','--','--'])
                                
    # clean up image tool
    ia.done()

#----------------------------------------------------------------------

def runMaskComparison(baseDir, testDir, outFile,projects=None):

    '''
    Compare the test masks to the base masks and see if anything has changed.
    '''

    # Input: 
    #       baseDir: base speed directory
    #       testDir: test directory
    #       outFile: text file for results
    #   
    #       projects: list of strings matching the projects you want to check.
    #
    # Output:
    #       text file with results 
    #
    # TO DO:
    #    -- check that this works for nterms>2 images -- I think it does, but should double-check
    #    -- compare the mask per channel.
    #    -- separate out the actual mask comparison from the directory crawl.
    #
    # Date              Programmer              Description of Changes
    #----------------------------------------------------------------------
    # 11/16/2017?       A.A. Kepley             Original Code
    # 12/08/2017        A.A. Kepley             Modified original code to extract more stats
    # 08/01/2018        A.A. Kepley             modified so that it uses compareMasks

    import csv

    projectRE = re.compile("\d{4}\.\w\.\d{5}\.\w_\d{4}_\d{2}_\d{2}T\d{2}_\d{2}_\d{2}\.\d{3}")
 
    if os.path.exists(baseDir):
        dataDirs = os.listdir(baseDir)

        if not projects:
            projects = dataDirs
        
        with open(outFile,'w') as csvfile:
            writer = csv.writer(csvfile,delimiter=',')

            writer.writerow(["# Base: "+baseDir])
            writer.writerow(["# Test: "+testDir])
            writer.writerow(["Project","Mask","nPixMaskBase","nPixMaskTest","nPixDiff","nPixDiffBase","nPixDiffTest","fracDiff"])

            for mydir in dataDirs:
                if (projectRE.match(mydir)) and (mydir in projects):
                    baseProject = os.path.join(baseDir,mydir)
                    baseMaskList = [os.path.basename(mask) for mask in glob.glob(os.path.join(baseProject,"*.mask"))]        
                    testProject = os.path.join(testDir,mydir)
                    
                    if os.path.exists(testProject):            
                        for mask in baseMaskList:
                            baseMaskPath = os.path.join(baseProject,mask)
                            testMaskPath = os.path.join(testProject,mask)                            
                            compareMask(baseMaskPath, testMaskPath, writer)
                    else:
                        print "no corresponding test project: ", mydir
    
    else:
        print "test directory doesn't exist:", mydir


#----------------------------------------------------------------------

def calculateStats(image, mask, pb, pblimit=0.2):
    '''

    calculate the stats for the image outside the clean mask and
    inside the pblimit. Using Chauvenet Algorithm by default for now,
    but I could change.
    
    Date        Programmer              Changes
    ----------------------------------------------------------------------
    5/8/2019    A.A. Kepley             Original Code

    '''

    ia.open(image)
    stats = ia.statistics( mask='"%s"<0.5 && "%s">%g'%(mask,pb,pblimit), axes=[0,1],robust=True,algorithm='chauvenet',maxiter=5)
    ia.close()
    ia.done()

    return stats

def getMaskPixels(mask):
    '''
    calculate the mask statistics

    Date        Programmer              Changes
    ----------------------------------------------------------------------
    5/8/2019    A.A. Kepley             Original Code
    '''

    ia.open(mask)
    maskStats = ia.statistics(axes=[0,1])
    ia.close()
    ia.done()

    nPixMask = maskStats['sum']

    return nPixMask


def getFOVPixels(pb,pblimit=0.2):
    '''
    calculate how many pixels are in the FOV
    
    Date        Programmer              Changes
    ----------------------------------------------------------------------
    5/9/2019    A.A. Kepley             Original Code
    '''
    
    ia.open(pb)
    stats = ia.statistics(mask='"%s">%g'%(pb,pblimit),axes=[0,1])
    ia.close()
    ia.done()

    nPixFOV = stats['npts'][0]

    return nPixFOV



def calcDiffMask(baseMask,testMask,diffImage='test.diff'):
    ''' 
    calculate the mask difference

    Date        Programmer              Changes
    ----------------------------------------------------------------------
    5/8/2019    A.A. Kepley             Original Code
    '''


    
    if not os.path.exists(diffImage):
        divexpr = '\"'+testMask + '\"-\"'+ baseMask+'\"'
        myim = ia.imagecalc(diffImage,divexpr,overwrite=True,imagemd=baseMask)
        myim.done()
    

    ia.open(diffImage)
    diffImagePix = ia.getchunk(dropdeg=True)
    ia.close()
    ia.done()
    
    if np.ndim(diffImagePix) > 1:
        nPixDiff = np.sum(abs(diffImagePix),axis=(0,1))
        nPixDiffBase = np.sum(diffImagePix < 0,axis=(0,1))
        nPixDiffTest = np.sum(diffImagePix > 0,axis=(0,1))
    else:
        # work around for bug with ia.imagecalc -- I'm wondering if I should do this with immath.....
        nPixDiff = np.sum(abs(diffImagePix))
        nPixDiffBase = np.sum(diffImagePix < 0)
        nPixDiffTest = np.sum(diffImagePix > 0)


    return nPixDiff, nPixDiffBase, nPixDiffTest


def plotFracDiff(fracDiff, title='test', outliervalue=1.0, maxval=5.0,countsplit=5,binsize=1.0):
    '''
    plot the fractional difference between the base and test mask, i.e., nPixDiff/nPixMask

    '''
    
    import matplotlib.pyplot as plt
    import numpy as np

    plt.close()

    mymax = round(np.nanmax(fracDiff),-1)

    if mymax < maxval:
        mymax = maxval

    nbins=int(mymax/binsize)
    
    (counts,bins,patches) = plt.hist(fracDiff,bins=nbins,range=(0,mymax),log=True,zorder=2,alpha=0.25,color='gray')
    plt.xlabel(r'Npix(abs(TestMask - BaseMask))/Npix(Smoothed Beam)')
    plt.ylabel('Number of channels')

    chanlist = np.arange(0,len(fracDiff))
    binidx = np.digitize(fracDiff,bins)

    for i in np.arange(len(bins)-1):
        if bins[i] >= outliervalue:
            chanliststr = "\n".join(map(str,chanlist[binidx==i+1]))
            if ((counts[i] > 0) and (counts[i] < countsplit)) :
                plt.text((bins[i]+bins[i+1])/2.0,counts[i]*0.87,chanliststr,horizontalalignment='center',verticalalignment='top',zorder=1,fontweight='bold')
            if counts[i] >= countsplit:
                plt.text((bins[i]+bins[i+1])/2.0,counts[i]*0.87,str(counts[i])+" chan",horizontalalignment='center',verticalalignment='top',zorder=1,fontweight='bold')

    plt.title(title)

    plt.savefig(title+'_fracdiff.png')
    plt.close()

def runCubeMaskComparison(baseDir,testDir,projects=[],exclude=[]):

    '''

    Run cube comparison code for all projects.

    Date        Programmer              Description of Changes
    ----------------------------------------------------------------------
    5/8/2019    A.A. Kepley             Original Code

    '''

    projectRE = re.compile("\d{4}\.\w\.\d{5}\.\w_\d{4}_\d{2}_\d{2}T\d{2}_\d{2}_\d{2}\.\d{3}")
 
    if os.path.exists(baseDir):
        dataDirs = os.listdir(baseDir)

        if not projects:
            projects = dataDirs
            
        for mydir in dataDirs:
            if (projectRE.match(mydir)) and (mydir in projects) and (mydir not in exclude):
                baseProject = os.path.join(baseDir,mydir)
                baseMaskList = [os.path.basename(mask) for mask in glob.glob(os.path.join(baseProject,"*cube*.mask"))]        
                testProject = os.path.join(testDir,mydir)
                    
                if os.path.exists(testProject):            
                    for mask in baseMaskList:
                        baseMaskPath = os.path.join(baseProject,mask)
                        baseImagePath = os.path.join(baseProject,mask.replace('.mask','.image'))
                        basePBPath = os.path.join(baseProject,mask.replace('.mask','.pb'))

                        testMaskPath = os.path.join(testProject,mask)                            

                        nPixMask = getMaskPixels(baseMaskPath)

                        if os.path.exists(testMaskPath):
                            nPixTestMask = getMaskPixels(testMaskPath)
                        else:
                            print testMaskPath+" not found. Skipping comparison."
                            continue

                        #nPixFOV = getFOVPixels(basePBPath)
                        nPixBeam = au.pixelsPerBeam(baseImagePath)

                        diffImage = mask+'.diff'
                        (nPixDiff, nPixDiffBase, nPixDiffTest) = calcDiffMask(baseMaskPath,testMaskPath,diffImage=diffImage)
                            
                        # the factor of 6.0 below accounts for the
                        # increase in size due to the smoothing and
                        # cutting process. I determined this factor
                        # empirically.
                        fracDiff = nPixDiff/(6.0*nPixBeam)

                        plotFracDiff(fracDiff,title=diffImage)


                else:
                    print "no corresponding test project: ", mydir
    
    else:
        print "test directory doesn't exist:", mydir
