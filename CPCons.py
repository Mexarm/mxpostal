#!/usr/bin/python
# -*- coding: utf-8 -*-

import pandas as pd
from simpledbf import Dbf5
from slugify import slugify

def df_load_from_dbf(path_to_dbf,codec):
    return Dbf5(path_to_dbf, codec=codec).to_dataframe()

def load_cpcons(path_to_cpcons, codec='cp850'): # codec = latin_1, utf_8, etc see https://docs.python.org/2/library/codecs.html 7.8.3. Standard Encodings¶
    from os import path
    import pickle
    
    pickle_file = path.join(path_to_cpcons,'cp_cons.pickle') # delete cp_cons.pickle to recreate from files

    if not path.isfile(pickle_file):
        ciudades = df_load_from_dbf(path.join(path_to_cpcons,'CIUDADES.DBF'),codec=codec)
        rep = df_load_from_dbf(path.join(path_to_cpcons,'REP.DBF'),codec=codec)
        municipi = df_load_from_dbf(path.join(path_to_cpcons,'MUNICIPI.DBF'),codec=codec)
        codigo_dbf = path.join(path_to_cpcons,'CODIGO%02d.DBF')
        codigo = pd.concat([ df_load_from_dbf(codigo_dbf % (i,),codec=codec) for i in range(1,33) ])
        cols = ['RANGO%d' %(i,) for i in range(1,5)]
        ciudades[cols] = ciudades[cols].apply(pd.to_numeric)
        cols = ['RANGO%d' %(i,) for i in range(1,3)]
        rep[cols] = rep[cols].apply(pd.to_numeric)
        cols = ['RANGO%d' %(i,) for i in range(1,9)]
        municipi[cols] = municipi[cols].apply(pd.to_numeric)
        codigo[['OFICINA', 'CODIGO', 'COR']] = codigo[['OFICINA', 'CODIGO', 'COR']].apply(pd.to_numeric)
        cpcons = dict(
            ciudades = ciudades,
            rep = rep,
            municipi = municipi,
            codigo = codigo
            ) 
        pickle.dump(cpcons,open(pickle_file,'wb'),pickle.HIGHEST_PROTOCOL)
    else:
        cpcons = pickle.load(open(pickle_file,'rb'))
    return cpcons 

def get_estado_by_cp(codigo_postal, cpcons):
    if not cpcons: return
    rep = cpcons['rep']
    return rep[ (rep['RANGO1'] <= codigo_postal) & (rep['RANGO2'] >= codigo_postal) ]

def get_ciudad_by_cp(codigo_postal, cpcons):
    if not cpcons: return
    cd = cpcons['ciudades']
    c1 = (cd['RANGO1'] <= codigo_postal) & (cd['RANGO2'] >= codigo_postal)
    c2 = (cd['RANGO3'] <= codigo_postal) & (cd['RANGO4'] >= codigo_postal)
    return cd[ c1 | c2 ]

def get_municipio_by_cp(codigo_postal, cpcons):
    if not cpcons: return
    mpio = cpcons['municipi']
    c1 = (mpio['RANGO1'] <= codigo_postal) & (mpio['RANGO2'] >= codigo_postal)
    c2 = (mpio['RANGO3'] <= codigo_postal) & (mpio['RANGO4'] >= codigo_postal)
    c3 = (mpio['RANGO5'] <= codigo_postal) & (mpio['RANGO6'] >= codigo_postal)
    c4 = (mpio['RANGO7'] <= codigo_postal) & (mpio['RANGO8'] >= codigo_postal)
    return mpio[ c1 | c2 | c3 | c4 ]

def get_asentamiento_by_cp(codigo_postal, cpcons):
    if not cpcons: return
    cod = cpcons['codigo']
    c1 = (cod['CODIGO'] == codigo_postal) 
    c2 = (cod['OFICINA'] == codigo_postal) 
    c3 = (cod['COR'] == codigo_postal) 
    return cod[ c1 | c2 | c3 ]

def cp_edo_corresponde(codigo_postal,line, cpcons):
    if not cp_is_valid(codigo_postal,cpcons): return False
    estado = get_estado_by_cp(codigo_postal,cpcons)
    if slugify(list(estado['NOMBRE'])[0]) in slugify(line): return True
    if slugify(list(estado['EDO1'])[0]) in slugify(line): return True
    return False

def cp_ciudad_mun_corresponde(codigo_postal,line,cpcons):
    if not cp_is_valid(codigo_postal,cpcons): return False
    cdmun = list(get_ciudad_by_cp(codigo_postal,cpcons)['NOMBRE']) + \
            list(get_municipio_by_cp(codigo_postal,cpcons)['NOMBRE'])
    for cdm in cdmun:
        if slugify(cdm) in slugify(line):
            return True
    return False

def cp_asentamiento_corresponde(codigo_postal,line, cpcons):
    if not cp_is_valid(codigo_postal,cpcons): return False
    asentamientos = get_asentamiento_by_cp(codigo_postal,cpcons)
    for col in list(asentamientos['USUARIO']):
        if slugify(col) in slugify(line):
            return True
    return False


def cp_is_valid(codigo_postal,cpcons):
    if not codigo_postal: return
    if not isinstance(codigo_postal,int):
        codigo_postal = int(codigo_postal)
    cod = cpcons['codigo']
    return len(cod[cod['CODIGO'] == codigo_postal]) > 0
