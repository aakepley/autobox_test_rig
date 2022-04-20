# routines useful for uvcontsub testing

# assumes using in casalith

from casaplotms import plotms
import os

def uvcontsubplot(origvis='',contsubvis='',spwlist=[],srclist=[],plotprefix=''):

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
                   yaxis='real',ydatacolumn='data',
                   field=src,spw=spw,
                   avgtime='1e8',avgscan=True,avgbaseline=True,
                   transform=True,freqframe='LSRK',showgui=False,
                   height=height,
                   width=width)

            plotms(vis=contsubvis, gridcols=2,plotindex=1, clearplots=False,
                   rowindex=0,
                   colindex=0,
                   xaxis='frequency',
                   yaxis='real',ydatacolumn='data',
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
                   yaxis='imag',ydatacolumn='data',
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
                   yaxis='imag',ydatacolumn='data',
                   field=src,spw=spw,
                   avgtime='1e8',avgscan=True,
                   avgbaseline=True,
                   transform=True,freqframe='LSRK',showgui=False,
                   customsymbol=True,symbolcolor='ff00ff',
                   height=height,
                   width=width,
                   plotfile=filename)
