from qfin import package_path
import pandas as pd
import numpy as np
from os.path import join as path_join
import os
import csv


# create dataframe from templates
# default when empty=0, give assign default values
# when empty=1, return empty dataframe only
def parse_data_from(file_name, lenth=1, empty=0):

    file_path = path_join(package_path, file_name)
    assert  os.path.exists(file_path), "%s is not in templates folder" % str(file_name)

    rs_df = pd.DataFrame()
    with open(file_path) as tmpl:
        for row in csv.reader(tmpl, delimiter=','):
            if empty==0:
                rs_df[row[0].strip()] = np.repeat(row[2].strip(), lenth, axis=0)
            else:
                rs_df=rs_df.append(pd.DataFrame(columns=[row[0].strip()]))
            rs_df[row[0].strip()] = rs_df[row[0].strip()].astype(row[1].strip())

    return rs_df
