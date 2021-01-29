import glob
import re

import pandas as pd

file_path = '/global/cfs/cdirs/lsst/shared/xgal/skysim/skysim5000_v1.1.1_parquet/'
scratch_path = '/global/cscratch1/sd/cwalter/parquet-with-healpixels/'

selected = ['galaxy_id', 'mag_i', 'redshift_true', 'ra', 'dec', 'convergence', 'magnification',
            'shear_1', 'shear_2']

for i, parquet_file in enumerate(glob.iglob(file_path+'/*.parquet')):

    df = pd.read_parquet(parquet_file, columns=selected)

    number = re.findall(r'skysim5000_v1.1.1_healpix(\d+).parquet', parquet_file)[0]
    df['healpix'] = int(number)
    df.set_index('healpix', inplace=True)

    df.to_parquet(scratch_path + f'/skysim-{int(number)}.parquet')
