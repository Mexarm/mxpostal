# -*- coding: utf-8 -*-

import csv
import codecs
import json
from collections import OrderedDict
from postal_utils import is_bussiness

def csv_unireader(f, encoding="utf-8",delimiter=',', quotechar='"'):
    for row in csv.reader(codecs.iterencode(codecs.iterdecode(f, encoding), "utf-8"),delimiter=delimiter,quotechar=quotechar):
        yield [e.decode("utf-8") for e in row]

def process_csv(filename, output, encoding='utf-8', delimiter=',', quotechar='"'):
    with open(output,'wb') as outhandle:
        outwriter = csv.writer(outhandle, delimiter=',',
                            quotechar='"', quoting=csv.QUOTE_MINIMAL)
        with open(filename,'rb') as handle:
            csv_reader = csv_unireader(handle,encoding=encoding, delimiter=delimiter, quotechar=quotechar)
            col_count = 0
            #header = []
            header = csv_reader.next()
            #print header
            outwriter.writerow(header+['es_empresa'])
            for lineno,line in enumerate(csv_reader):
                #line=[ f.decode(encoding).encode('utf-8') for f in line] #converts to utf-8
                #print header,line
                file_record = OrderedDict(zip(header,line))
                #print file_record
                #print file_record
                es_empresa=0
                if is_bussiness(file_record['Company']):
                    es_empresa=1
                line = [f.encode('utf-8') for f in line]
                outwriter.writerow(line+[es_empresa])

#process_csv('../el_sardinero/filtrar-empresas/2da_registros_comunes.csv','../el_sardinero/filtrar-empresas/2da_registros_comunes_empresas.csv')
process_csv('../el_sardinero/filtrar-empresas/BASE PARA QP_DEPURADA V2 GR DUP FILTRADOS_RESTANTES.csv','../el_sardinero/filtrar-empresas/BASE PARA QP_DEPURADA V2 GR DUP FILTRADOS_RESTANTES_empresas.csv')