import numpy as np
import pandas as pd
import h5py

filepath = '/global/cfs/cdirs/desc-wl/users/zuntz/data/'
filename = 'matched-shear-2.2i_dr6-v1.hdf5'
h5 = h5py.File(filepath+filename, 'r')

dset = h5['shear']

id = np.array(dset['id'])
ra = np.array(dset['ra'])
dec = np.array(dset['dec'])
g1 = np.array(dset['true_g1'])
g2 = np.array(dset['true_g2'])
array_list = np.column_stack([id, ra, dec, g1, g2])

df = pd.DataFrame(array_list, columns=['id', 'ra', 'dec', 'g1', 'g2'])
df.to_parquet('shear-matched.parquet')
