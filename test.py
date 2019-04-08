#!/usr/bin/python
# -*- coding: utf-8 -*-
import pandas as pd
#import numpy as np
#import re
#import string
from postal_utils import *
from CPCons import *

cpcons=load_cpcons('/Users/armandohm/dev/cp_cons')
dom = pd.read_csv('../../Documents/Documentos - MacBook Pro de Armando/Layout Paqueteria Bimbo/DISPERCION_dom.csv',encoding='utf8')
acolumn = 'DOMICILIO ACTUAL DEL CEVE COMPLETO'
df = pd.DataFrame()
df['line'] = dom[acolumn]
df['callenum'] = df.apply(lambda row: get_callenum_from_str(row['line']),axis=1)
df['colonia'] = df.apply(lambda row: get_colonia_from_str(row['line']),axis=1)
df['cp'] = df.apply(lambda row: get_cp_from_str(row['line']),axis=1)  
df['cp-existe'] = df.apply(lambda row: cp_existe(cp_to_int(row['cp']),cpcons),axis=1)
df['cp->edo'] = df.apply(lambda row: cp_edo_corresponde(cp_to_int(row['cp']),row['line'],cpcons),axis=1)
df['cp->cd_mun'] = df.apply(lambda row: cp_ciudad_mun_corresponde(cp_to_int(row['cp']),row['line'],cpcons),axis=1)
df['cp->asentamiento'] = df.apply(lambda row: cp_asentamiento_corresponde(cp_to_int(row['cp']),row['line'],cpcons),axis=1)
df['cp_ciudad'] = df.apply(lambda row: ','.join(list(get_ciudad_by_cp(cp_to_int(row['cp']),cpcons)['NOMBRE'])),axis=1)
df['cp_municipio'] = df.apply(lambda row: ','.join(list(get_municipio_by_cp(cp_to_int(row['cp']),cpcons)['NOMBRE'])),axis=1)
df['cp_edo'] = df.apply(lambda row: ','.join(list(get_estado_by_cp(cp_to_int(row['cp']),cpcons)['NOMBRE'])),axis=1)
writer = pd.ExcelWriter('test.xlsx')
df.to_excel(writer,'TEST')
writer.save()


