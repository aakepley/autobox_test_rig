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

def calculateStats(image, mask=None, pb=None, pblimit=0.2):
    '''

    calculate the stats for the image outside the clean mask and
    inside the pblimit. Using Chauvenet Algorithm by default for now,
    but I could change.
    
    Date        Programmer              Changes
    ----------------------------------------------------------------------
    5/8/2019    A.A. Kepley             Original Code

    '''

    if mask and pb:
        ia.open(image)
        stats = ia.statistics( mask='"%s"<0.5 && "%s">%g'%(mask,pb,pblimit), axes=[0,1],robust=True,algorithm='chauvenet',maxiter=5)
        ia.close()
        ia.done()
    else:
        ia.open(image)
        stats = ia.statistics(axes=[0,1],robust=True,algorithm='chauvenet',maxiter=5)
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

#----------------------------------------------------------------------


def runImageStats(baseDir, projects=[],exclude=[]):
    '''
    Create plots of noise for each data set
    
    Date        Programmer              Changes
    ----------------------------------------------------------------------
    5/13/2019   A.A. Kepley             Original Code
    '''


    projectRE = re.compile("\w{4}\.\w\.\d{5}\.\w_\d{4}_\d{2}_\d{2}T\d{2}_\d{2}_\d{2}\.\d{3}")
 
    if os.path.exists(baseDir):
        dataDirs = os.listdir(baseDir)

        if not projects:
            projects = dataDirs
            
        for mydir in dataDirs:
            if (projectRE.match(mydir)) and (mydir in projects) and (mydir not in exclude):
                baseProject = os.path.join(baseDir,mydir)
                baseImageList = [os.path.basename(image) for image in glob.glob(os.path.join(baseProject,"*cube*.image"))] 

                for image in baseImageList:
                    baseImagePath = os.path.join(baseProject,image)
                    baseMaskPath = os.path.join(baseProject,image.replace('.image','.mask'))
                    baseResidualPath = os.path.join(baseProject,image.replace('.image','.residual'))
                    basePBPath = os.path.join(baseProject,image.replace('.image','.pb'))
                                    
                    au.imageStdPerChannel(img=baseImagePath,plotmax=False,plotfile=os.path.basename(baseImagePath)+'.png',pbimg=basePBPath)
                    au.imageStdPerChannel(img=baseResidualPath,plotmax=False,plotfile=os.path.basename(baseResidualPath)+'.png',pbimg=basePBPath)
                            
                        

#----------------------------------------------------------------------

def plotDiffSpect(image1, image2, thresh=10.0, nspec=10, plotname='test',labels=['linear','nearest'],padchans=0.1,minSN=10.0,deltaSN=20.0,minchan=10):
    '''
    Plot difference spectra to compare linear vs. nearest

    Date        Programmer              Changes
    ----------------------------------------------------------------------
    5/23/2019   A.A. Kepley             Original Code   
    '''

    import matplotlib.pyplot as plt


    MADtoRMS =1.4826

    # open the two images
    ia.open(image1)
    cube1 = ia.getchunk(dropdeg=True)
    stats = ia.statistics(robust=True,algorithm='chauvenet')
    ia.close()


    ia.open(image2)
    cube2 = ia.getchunk(dropdeg=True)
    ia.close()
    ia.done()

    imagename = os.path.basename(image1)
    nchan = np.shape(cube1)[2]

    # use one estimate for whole window. Not great, but removes the issue
    # of emission over the whole region increasing the sigma estimate.
    sigma = MADtoRMS * stats['medabsdevmed']

    # get the positions and values of the peaks in descending order
    (coords, maxVals) = getLocalMaxes(cube1,sigma,minSN=minSN,deltaSN=deltaSN)
    npeaks = len(maxVals)

    # rearrange significant indices

    if (nspec < npeaks) and (npeaks != 0):
        for i in np.arange(nspec):
            
            # get spectra at peak
            spectra1 = cube1[coords[i][0],coords[i][1],:]
            spectra2 = cube2[coords[i][0],coords[i][1],:]
            
            # figure out plot range
            sigIdxSpec = spectra1 > (sigma*thresh)
            chansWithSignal = np.where(sigIdxSpec)

            maxChan = np.argmax(cube1[coords[i][0],coords[i][1],:])
            
            nChanWithSignal = chansWithSignal[0][-1] - chansWithSignal[0][0]

            if nChanWithSignal < minchan:
                nChanWithSignal = minchan
            
            if nChanWithSignal > 0:
                begchan = chansWithSignal[0][0] - np.int(np.floor((padchans+1.0) * nChanWithSignal))
                endchan = chansWithSignal[0][-1] + np.int(np.ceil((padchans+1.0) * nChanWithSignal))
            else:
                begchan = 0
                endchan =  nchan - 1

            if begchan < 0:
                begchan = 0
            if endchan > (nchan - 1):
                endchan = nchan - 1

            #print imagename, maxChan, nChanWithSignal,begchan, endchan

            # plot spectra
            plt.close()
            fig = plt.figure(figsize=(8,8))
            
            f1 = fig.add_subplot(211)
            f1.set_title(imagename + '\n' + '('+str(coords[i][0])+','+str(coords[i][1])+','+str(maxChan)+')')
            f1.plot(spectra1,label=labels[0],drawstyle='steps-mid')
            f1.plot(spectra2,label=labels[1],drawstyle='steps-mid')
            f1.set_xlabel('channel')
            f1.set_ylabel('Jy/beam')
            f1.set_xlim(begchan,endchan)
            f1.axhline(0.0,color='gray',linestyle=':')
            f1.legend()
            
            
            diffSpectra = spectra1 - spectra2
            f2 = fig.add_subplot(212)
            f2.plot(diffSpectra,label=labels[0]+' minus '+labels[1],drawstyle='steps-mid')
            f2.set_xlabel('channel')
            f2.set_ylabel('Jy/beam')
            f2.set_xlim(begchan,endchan)
            f2.axhline(0.0,color='gray',linestyle=':')
            f2.legend(loc='lower left')
            
            
            fig.savefig(plotname+'_'+str(coords[i][0])+'_'+str(coords[i][1])+'_'+str(maxChan)+'.png')

        
        


def getLocalMaxes(cube,sigma,minSN=10.0,deltaSN=10.0):
    '''
    Get local maximum for spectra

    Date        Programmer              Changes
    ----------------------------------------------------------------------
    5/23/2019   A.A. Kepley             Original Code   
    '''

    import matplotlib.pyplot as plt
    import scipy.ndimage as ndi
    



    # get peak intensity image.  The code below can do either a cube
    # or peak image, but the peak image is substantially faster when
    # trying to determine maxima.
    peak = np.max(cube,axis=2)

    # generate the levels. We're going from a minSN to maxSN, then
    # reversing to put highest contour first.
    maxSN = np.max(peak)/sigma 
    levels = np.arange(minSN,maxSN,deltaSN)[::-1]

    # create empty structures for the master list of labels, num_labels, and coords 
    coords = []
    maxVals = []

    # go through the levels
    for i in np.arange(len(levels)):
        thresh = levels[i]*sigma

        labels, num_labels = ndi.label(peak > thresh)
        coords_tmp = ndi.measurements.maximum_position(peak,labels=labels,index=np.arange(1,num_labels+1)) ## these coordinates are flipped to what I expect (y,x) not (x,y)
        maxVals_tmp = ndi.measurements.maximum(peak,labels=labels,index=np.arange(1,num_labels+1))

        if coords:
            new_coords = []
            new_maxVals = []
            for (coord_tmp,maxVal_tmp) in zip(coords_tmp,maxVals_tmp):
                if coord_tmp not in coords:
                    # add peak to new coord list
                    new_coords.append(coord_tmp)
                    new_maxVals.append(maxVal_tmp)

            # add new coords to master coord list. May not be
            # necessary not that I only have one loop above?
            if new_coords:
                coords.extend(new_coords)
                maxVals.extend(new_maxVals)
                new_coords = []

        else:
            coords.extend(coords_tmp)
            maxVals.extend(maxVals_tmp)

    descidx = (np.argsort(maxVals))[::-1]

    return np.array(coords)[descidx],np.array(maxVals)[descidx]

#----------------------------------------------------------------------

def runDiffSpectra(baseDir,testDir, projects=[],exclude=[],labels=['linear','nearest'],**kwargs):
    '''
    Create plots of noise for each data set
    
    Date        Programmer              Changes
    ----------------------------------------------------------------------
    5/13/2019   A.A. Kepley             Original Code
    '''

    import matplotlib.pyplot as plt


    projectRE = re.compile("\w{4}\.\w\.\d{5}\.\w_\d{4}_\d{2}_\d{2}T\d{2}_\d{2}_\d{2}\.\d{3}")
 
    if os.path.exists(baseDir):
        dataDirs = os.listdir(baseDir)

        if not projects:
            projects = dataDirs
            
        for mydir in dataDirs:
            if (projectRE.match(mydir)) and (mydir in projects) and (mydir not in exclude):
                baseProject = os.path.join(baseDir,mydir)
                testProject = os.path.join(testDir,mydir)

                baseImageList = [os.path.basename(image) for image in glob.glob(os.path.join(baseProject,"*cube*.image"))] 

                if os.path.exists(testProject):            
                    for image in baseImageList:
                        baseImagePath = os.path.join(baseProject,image)
                        testImagePath = os.path.join(testProject,image)     
                        
                        if os.path.exists(testImagePath) and os.path.exists(baseImagePath):
                            plotDiffSpect(baseImagePath,testImagePath,plotname=image,**kwargs)
