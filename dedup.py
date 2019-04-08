#!/usr/bin/python
# -*- coding: utf-8 -*-

import csv
import codecs
import os
from fuzzywuzzy import fuzz

def csv_unireader(f, encoding="utf-8",delimiter=',', quotechar='"'):
    for row in csv.reader(codecs.iterencode(codecs.iterdecode(f, encoding), "utf-8"),delimiter=delimiter,quotechar=quotechar):
        yield [e.decode("utf-8") for e in row]


basefile = os.path.join('el_sardinero_dedup','BASE PARA QP_DEPURADA V2 GR DUP FILTRADOS.csv')
secfile = os.path.join('el_sardinero_dedup','2da.csv')

delimiter = ','
quotechar = '"'
dupscore = 80

basereader = csv_unireader(open(basefile,'rb'),
                            encoding='iso8859-1',
                            delimiter=delimiter,
                            quotechar=quotechar)

secreader = csv_unireader(open(secfile,'rb'),
                            encoding='iso8859-1',
                            delimiter=delimiter,
                            quotechar=quotechar)

secrecords = list(secreader)
baserecords = list(basereader)
duplicates = []

for n,row in enumerate(baserecords):
    max_ = 0
    score = 0
    dupindex=0
    row_str = " ".join(row[1:]) # [1:] ignore first field
    for m,row2 in enumerate(secrecords):
        row2_str =" ".join(row2)
        #if row[4] == row2[-1] and (len(row_str)*1.0)/(len(row2_str))>0.60:
        if (len(row_str)*1.0)/(len(row2_str))>0.60: # work on records with length ratio bigger than 60%
            score = fuzz.token_set_ratio(row_str, row2_str)
            if score>max_:
                candidate = row2
                max_=score
                dupindex=m
    if max_>dupscore: 
        print "{},{},{}".format(n+1,dupindex+1, max_)
        #print row, len(row_str)
        #print candidate, len(' '.join(candidate))
        #duplicates.append((n,dupindex,max_))