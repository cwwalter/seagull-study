import numpy as np
import pandas as pd
import h5py
import glob

filepath = '/global/cfs/cdirs/lsst/shared/xgal/skysim/skysim5000_v1.1.1'

healpix_pixels = [8786, 8787, 8788, 8789, 8790, 8791, 8792, 8793, 8794, 8913, 8914, 8915, 8916, 8917, 8918, 8919, 8920, 8921, 9042, 9043, 9044, 9045, 9046, 9047, 9048, 9049,
                  9050, 9169, 9170, 9171, 9172, 9173, 9174, 9175, 9176, 9177, 9178, 9298, 9299, 9300, 9301, 9302, 9303, 9304, 9305, 9306, 9425, 9426, 9427, 9428, 9429, 9430,
                  9431, 9432, 9433, 9434, 9554, 9555, 9556, 9557, 9558, 9559, 9560, 9561, 9562, 9681, 9682, 9683, 9684, 9685, 9686, 9687, 9688, 9689, 9690, 9810, 9811, 9812,
                  9813, 9814, 9815, 9816, 9817, 9818, 9937, 9938, 9939, 9940, 9941, 9942, 9943, 9944, 9945, 9946, 10066, 10067, 10068, 10069, 10070, 10071, 10072, 10073, 10074, 10193,
                  10194, 10195, 10196, 10197, 10198, 10199, 10200, 10201, 10202, 10321, 10322, 10323, 10324, 10325, 10326, 10327, 10328, 10329, 10444, 10445, 10446, 10447, 10448, 10449, 10450,
                  10451, 10452]


for i, h5f in enumerate(glob.iglob(filepath+'/*.hdf5')):
    with h5py.File(h5f, 'r') as file:

        # print(i, file.filename)
        # if i == 10: break

        # Uncomment (and likely change output directory below) to only use the
        # healpixels that overlap with cosmoDC2
        pixel_overlap = [pixel for pixel in healpix_pixels if str(pixel) in h5f]
        if not pixel_overlap:
            continue

        print(i, file.filename)

        properties = file['galaxyProperties']

        id = np.array(properties['galaxyID'])
        mag_r = np.array(properties['LSST_filters']['magnitude:LSST_r:observed'])
        z = np.array(properties['redshiftHubble'])

        ra = np.array(properties['ra'])
        dec = np.array(properties['dec'])
        ra_true = np.array(properties['ra_true'])
        dec_true = np.array(properties['dec_true'])

        angle = np.array(properties['morphology']['positionAngle'])
        e = np.array(properties['morphology']['totalEllipticity'])
        e1 = e*np.cos(2.0*angle)
        e2 = e*np.sin(2.0*angle)

        magnification = np.array(properties['magnification'])
        kappa = np.array(properties['convergence'])
        g1 = np.array(properties['shear1'])
        g2 = np.array(properties['shear2'])

        array_list = np.column_stack([id, mag_r, z,
                                      ra_true, dec_true, ra, dec,
                                      angle, e, e1, e2,
                                      kappa, g1, g2, magnification])

        df = pd.DataFrame(array_list, columns=['id', 'mag_r', 'z',
                                               'ra_true', 'dec_true', 'ra', 'dec',
                                               'angle', 'e', 'e1', 'e2',
                                               'kappa', 'g1', 'g2', 'magnification'])

        df = df.query('mag_r < 29')
        df.to_parquet(f'/global/cscratch1/sd/cwalter/parquet-skysim-small/shear-cosmoDC2-{i}.parquet')
