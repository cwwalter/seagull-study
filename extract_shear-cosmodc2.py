import numpy as np
import pandas as pd
import h5py
import glob

filepath = '/global/cfs/cdirs/lsst/shared/xgal/cosmoDC2/cosmoDC2_v1.1.4_rs_scatter_query_tree_double/'

for i, h5f in enumerate(glob.iglob(filepath+'/*.hdf5')):
    with h5py.File(h5f, 'r') as file:
        print(i, file.filename)

        # if i == 10: break

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
        df.to_parquet(f'/global/cscratch1/sd/cwalter/parquet-cosmoDC2/shear-cosmoDC2-{i}.parquet')
