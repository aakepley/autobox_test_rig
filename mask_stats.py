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
    
def MaskComparison(baseDir, testDir, outFile):

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
    #    -- check that this works for nterms>2 images -- I think it does, but should double-check
    #    -- compare the mask per channel.
    #    -- separate out the actual mask comparison from the directory crawl.
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
                                print "no corresponding test image: ", testMaskPath
                                writer.writerow([mydir,mask,nPixMask,'--','--','--','--','--'])


                    else:
                            print "no corresponding test project: ", mydir
                            writer.writerow([mydir,mask,nPixMask,'--','--','--','--','--'])

            ia.done()

    else:
        print "test directory doesn't exist:", mydir
            


def compareImages(baseMaskPath,testMaskPath,fileout):
    '''
    This chunk of code does the comparison on two different images
    '''

    # Input: 
    #   baseMaskPath: fidicual mask
    #   testMaskPath: test mask
    #   writer: where to write the results to.


    pass




def test_function_passing():
    fh = open("junk.dat","w")
    write_stuff(fh)
    fh.close()

def write_stuff(fh):
    fh.write("test\n")
    
