#!/usr/bin/python
# -*- coding: utf-8 -*-
import re
RE_FLAGS = re.IGNORECASE|re.MULTILINE|re.DOTALL
REGEX_CP1 = re.compile(r'c\W*p\W*(\d{5}|\d{4})',RE_FLAGS)
REGEX_CP2 = re.compile(r'(\d{5})',RE_FLAGS)
REGEX_COL = r'^.+?((%s)\W\W*[\w\W]+?)(?:,|\n|\r|c[\W]*p|\d{5}|$)'
REGEX_COL1 = re.compile(REGEX_COL%('colonia|col',),RE_FLAGS)
REGEX_COL2 = re.compile(REGEX_COL%('fraccionamiento|fracc',),RE_FLAGS)
REGEX_COL3 = re.compile(REGEX_COL%('barrio|parque|zona',),RE_FLAGS)

def get_cp_from_str(line):
    cp = REGEX_CP1.search(line)
    if cp: return cp.group(1)
    cp = REGEX_CP2.search(line)
    if cp: return cp.group(1)
    return None

def get_colonia_from_str(line):
    col = REGEX_COL1.search(line)
    if col: return col.group(1)
    col = REGEX_COL2.search(line)
    if col: return col.group(1)
    col = REGEX_COL3.search(line)
    if col: return col.group(1)
    return

def get_callenum_from_str(line):
    col = REGEX_COL1.search(line)
    if col: return line[0:col.start(1)]
    col = REGEX_COL2.search(line)
    if col: return line[0:col.start(1)]
    col = REGEX_COL3.search(line)
    if col: return line[0:col.start(1)]
    return

def format_cp(codigo_postal):
    if isinstance(codigo_postal,basestring):
        codigo_postal = cp_to_int(codigo_postal)
    return '%05d' % (codigo_postal,)

def cp_to_int(codigo_postal):
    if not codigo_postal \
        or isinstance(codigo_postal,int) \
        or not isinstance(codigo_postal,basestring):
        return codigo_postal
    try:
        cp = int(codigo_postal)
    except:
        cp = 0
    return cp