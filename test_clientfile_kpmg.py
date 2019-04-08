#!/usr/bin/python
# -*- coding: utf-8 -*-
import pandas as pd
#import numpy as np
#import re
#import string
from postal_utils import *
from CPCons import *

cpcons=load_cpcons('/Users/armandohm/Projects/cp_cons')
dom = pd.read_csv('Contactos corporativos - Reforma Fiscal 2018.csv',encoding='utf8',dtype=str)
acolumn = 'Contact Address: Street Concatenation'
df = dom
#df = pd.DataFrame()
#df['line'] = dom[acolumn]
df['callenum'] = df.apply(lambda row: get_callenum_from_str(row[acolumn]),axis=1)
df['colonia'] = df.apply(lambda row: get_colonia_from_str(row[acolumn]),axis=1)
#df['cp'] = df['Contact Address: Postal Code']
df['cp'] = df['Contact Address: Postal Code'].fillna(0)
df['cp'] = df['cp'].apply(int)
df['Contact Address: State/Province'] = df['Contact Address: State/Province'].fillna('')
df['Contact Address: District'] = df['Contact Address: District'].fillna('')
df['cp-valido'] = df.apply(lambda row: cp_is_valid(cp_to_int(row['cp']),cpcons),axis=1)
df['cp->edo'] = df.apply(lambda row: cp_edo_corresponde(cp_to_int(row['cp']),row['Contact Address: State/Province'],cpcons),axis=1)
df['cp->cd_mun'] = df.apply(lambda row: cp_ciudad_mun_corresponde(cp_to_int(row['cp']),row['Contact Address: District'],cpcons),axis=1)
#df['cp->asentamiento'] = df.apply(lambda row: cp_asentamiento_corresponde(cp_to_int(row['cp']),row['line'],cpcons),axis=1)
df['cp_ciudad'] = df.apply(lambda row: ','.join(list(get_ciudad_by_cp(cp_to_int(row['cp']),cpcons)['NOMBRE'])),axis=1)
df['cp_municipio'] = df.apply(lambda row: ','.join(list(get_municipio_by_cp(cp_to_int(row['cp']),cpcons)['NOMBRE'])),axis=1)
df['cp_edo'] = df.apply(lambda row: ','.join(list(get_estado_by_cp(cp_to_int(row['cp']),cpcons)['NOMBRE'])),axis=1)
writer = pd.ExcelWriter('salida.xlsx')
df.to_excel(writer,'SALIDA')
writer.save()


