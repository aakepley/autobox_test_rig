%run generate_band1_models.py

### TODO: 
##      -- get direction offsets working
##             -- needed for uvcontsub2021 testing and mtmfs nterms=2 imaging



obs_freq = np.linspace(35, 35+8,1000) #GHz
snu0 = 230e-3 #Jy
nu0=40 #GHz
dust_ff_ratio = 0.5

ff_spect = calc_ff_spect(snu0,nu0, obs_freq) 
dust_spect = calc_dust_spect(snu0,nu0,dust_ff_ratio,obs_freq)  

plot_spectrum(ff_spect,dust_spect,nu0,obs_freq)

direction="J2000 10h00m00.0s -30d00m00.0s"
create_model(snu0, nu0, dust_ff_ratio,direction=direction)


project = "band1_test"
f=open(project+".ptg.txt",'w+')
f.write(direction)
f.close()

# doing simulation without any noise
simobserve(project=project,
           skymodel="skymodel_test.image",
           setpointings=False,
           ptgfile=project+".ptg.txt",
           antennalist='alma.cycle9.1.cfg',
           thermalnoise='',
           totaltime='60s',
           verbose=True)

tclean(vis='band1_test/band1_test.alma.cycle9.1.ms',
       imagename='test_cube',
       cell='2.0arcsec',
       imsize=[90,90],
       field='0',
       specmode='cubedata',
       gridder='standard',
       niter=0,
       weighting='briggs',
       robust=0.5)

# adding some noise       
project = "band1_test_wnoise"
simobserve(project=project,
           skymodel="skymodel_test.image",
           setpointings=False,
           ptgfile="band1_test.ptg.txt",
           antennalist='alma.cycle9.1.cfg',
           thermalnoise='tsys-atm',
           user_pwv = 5.186, # 7th octile, default choice from alma sensitivity calculator
           totaltime='60s',
           verbose=True)

# 2022-04-27 19:23:12     INFO    simobserve::::casa      sm.setnoise(spillefficiency=0.95,correfficiency=0.845,antefficiency=0.6804122026297641,trx=25,tground=269.0,tcmb=2.725,mode='tsys-atm',pground='560mbar',altitude='5000m',waterheight='2km',relhum=20,pwv=5.186mm)

## Note that the noise and noise range here aren't quite right yet for band 1.

tclean(vis='band1_test_wnoise/band1_test_wnoise.alma.cycle9.1.noisy.ms',
       imagename='test_cube_wnoise',
       cell='2.0arcsec',
       imsize=[90,90],
       field='0',
       specmode='cubedata',
       gridder='standard',
       niter=0,
       weighting='briggs',
       robust=0.5)

