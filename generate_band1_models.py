import numpy as np
import matplotlib.pyplot as plt
import os
import shutil

def calc_ff_spect(snu_ff, nu0, obs_freq, alpha=-0.1):
    '''
    calculate free-free spectrum given ff flux at a given frequency
    '''

    
    ff_spectrum = snu_ff * (nu0/obs_freq)**alpha
    return ff_spectrum


def calc_dust_spect(snu_ff, nu0, dust_ff_ratio, obs_freq, beta=2.0):
    '''
    calculate the dust spectrum based on scaling from a particular frequency
    '''

    dust_nu0 = dust_ff_ratio * snu_ff * (obs_freq/nu0)**(beta+2.0) 

    return dust_nu0


def plot_spectrum(ff_spect, dust_spect, nu0, obs_freq):
    '''
    plot spectra
    '''

    plt.clf()
    plt.loglog(obs_freq, ff_spect, label='Free-Free')
    plt.loglog(obs_freq, dust_spect, label='Dust')

    comb_spect = ff_spect + dust_spect

    plt.plot(obs_freq, comb_spect, label='Combined')

    plt.axvline(nu0,color='gray')
    plt.legend()
    
    plt.xlabel("Frequency (GHz)")
    plt.ylabel("Snu (Jy)")

    plt.show()


def create_model(snu_ff, nu0, dust_ff_ratio, bandwidth=7.5, startfreq=35.0,
                 offset_position=[0.0,0.0],direction="J2000 10h00m00.0s -30d00m00.0s",modelname='skymodel_test'):
    '''

    create an ms with a point source with the given spectrum.

    '''
    
    from casatasks.private import simutil
    u = simutil.simutil()
    
    from casatools import quanta
    from casatools import componentlist 
    from casatools import image
    from casatools import measures

    qa = quanta()
    cl = componentlist()
    me = measures()
    ia = image()

    obs_freq = np.linspace(startfreq, startfreq+bandwidth, 1000)

    ff_spect = calc_ff_spect(snu_ff, nu0, obs_freq)
    dust_spect = calc_dust_spect(snu_ff, nu0, dust_ff_ratio, obs_freq)
    
    comb_spect = ff_spect + dust_spect

    xx = u.direction_splitter(direction)
    qra = xx[1]
    qdec = xx[2]
    
    qra1 = qa.add(qra,str(offset_position[0])+"arcsec")
    qdec1 = qa.add(qdec,str(offset_position[1])+"arcsec")
    
    xx1 = xx[0] + " "+qa.formxxx(qra1, format='hms',prec=3)+" "+qa.formxxx(qdec1,format='dms',prec=4)

    
    cl.done() #close any open component list

    cl.addcomponent(flux=1.0, dir=xx1,shape='point')

    # tabularfreq in Hz
    # tabularflux in Jy
    obs_freq_Hz = obs_freq * 1e9
    cl.setspectrum(which=0, type='tabular', tabularfreqs = obs_freq_Hz, 
                   tabularflux=comb_spect)

    filename = modelname+'.cl'
    if os.path.exists(filename):
        shutil.rmtree(filename)
    cl.rename(filename)


    # make a skymodel from the component list since simobserve 
    # doesn't handle the component lists with frequency dependence.
    # Don't need header info. can set that in simobserve
    ia.done()

    filename = modelname+".image"
    if os.path.exists(filename):
        shutil.rmtree(filename)
        
    ia.fromshape(filename,[300,300,1,len(obs_freq)],overwrite=True)
    cs = ia.coordsys()
    
    cs.setunits(['deg','deg','','GHz'])
    cell_rad = qa.convert(qa.quantity("0.1arcsec"),"deg")['value']
    cs.setincrement([-cell_rad,cell_rad],'direction')
    
    cs.setreferencepixel(0,type='Spectral')
    cs.setreferencevalue(str(obs_freq[0])+"GHz",'Spectral')
    cs.setincrement("%.5fGHz"%np.diff(obs_freq)[0],'spectral')

    tmp = cs.referencevalue(format='q')
    tmp['quantity']['*1'] = xx[1]
    tmp['quantity']['*2'] = xx[2]
    cs.setreferencevalue(value=tmp)

    ia.setcoordsys(cs.torecord())
    ia.setbrightnessunit("Jy/pixel")
    ia.modify(cl.torecord(),subtract=False)

    ia.done()
    cl.done()
