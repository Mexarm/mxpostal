#!/usr/bin/env python
# -*- coding: utf-8 -*-
import csv
import CPCons
import postal_utils
import re
from slugify import slugify
import time
import sys
import json
from elasticsearch import Elasticsearch
from elasticsearch.exceptions import ConnectionTimeout
es = Elasticsearch(['http://proton:Proton2018.@192.168.0.81:9200/'], timeout=30)

def get_value(line, field_tuple):
    p,l = field_tuple
    p-=1
    return line[p:p+l]

def to_fixed_width(value,width = 125):
    if len(value) > width:
        raise ValueError('{} invalid field width'.format(len(value)))
    return value + (' ' * (width-len(value)))

def trim_and_reduce(value):
    return " ".join(value.split())

def separa_num_colonia(value):
    res = re.match(r'(.*)((?:col|colonia)[\s\.,]+.*)',value,re.DOTALL|re.IGNORECASE)
    return (res.group(1),res.group(2)) if res else False

def separa_num_matching_colonia(cp,value,cpcons):
    asent = CPCons.cp_asentamiento_corresponde(cp,value,cpcons)
    if asent:
        res = re.match(r'(.*)({}.*)'.format(slugify(asent)),slugify(value),re.DOTALL|re.IGNORECASE)
        return (res.group(1).replace('-',' '), res.group(2).replace('-',' '))
    return False

def tiene_numeros_o_letras_aisladas(value):
    tiene_num = re.search(r'[^\s\-\.\,]\d+',value) #digitos
    tiene_letra_aislada = re.search(r'[\s\-^]\w[\s\-$\.]',value,re.IGNORECASE) #letras aisladas
    return tiene_num or tiene_letra_aislada
    
def get_asentamiento_elasticsearch(cp, value):
    value = value.replace('"',"'")
    qstr = """{
            "query":{
                "bool":{
                    "must":[
                        {
                        "match":{
                            "USUARIO":{
                                "query":"%s",
                                "fuzziness":2,
                                "prefix_length":1
                            }
                        }
                        },
                        {
                        "match":{
                            "CODIGO": %d
                        }
                        }
                    ]
                }
            },
            "sort":{
                "_score":"desc"
            }
            }""" % (value,cp)
    while True:
        try:
            result = es.search(index="cpcons", body = json.loads(qstr))
        except ConnectionTimeout:
            print "elasticsearch connection timeout, retrying..."
            continue
        break

    if result['hits']['total'] and result['hits']['max_score'] > 8.0:
        return result['hits']['hits'][0]
    return False

def search_asentamiento_elasticsearch(estado_str, value):
    value = value.replace('"',"'")
    qstr = """{
       "query": {
         "match": {
           "USUARIO": {
             "query": "%s",
             "fuzziness": 2,
             "prefix_length": 1
           }
         }
       }
    }""" % value
    qstr = """{
            "query":{
                "bool":{
                    "must":[
                        {
                        "match":{
                            "USUARIO":{
                                "query":"%s",
                                "fuzziness":2,
                                "prefix_length":1
                            }
                        }
                        },
                        {
                        "match":{
                            "ESTADO": "%s"
                        }
                        }
                    ]
                }
            },
            "sort":{
                "_score":"desc"
            }
    }""" % (value,estado_str)
    result = es.search(index="cpcons", body = json.loads(qstr))
    if result['hits']['total']: 
        return result
    return False

def main():
    t0 = time.time()
    sep = ','
    quotechar = '"'
    skip_first_line = True
    dom_field_no = 1
    cp_field_no = 2
    asent_field_no = 3

    print ('cargando CPCONS...')
    cpcons = CPCons.load_cpcons('../cp_cons')
    t1 = time.time()
    print ('hecho. (tomo: {} segs)'.format(t1-t0))
    
    # fcp = (25,5)
    # fadd1 = (288,40)
    # fadd2 = (328,40)
    # fadd3 = (368,40)
    # fadd4 = (408,40)
    #1957196 - Actualización Contratos Nov Dic Lending
    fcp = (353,5)
    fadd1 = (113,40)
    fadd2 = (153,40)
    fadd3 = (193,40)
    fadd4 = (233,40)
    fadd5 = (273,40)
    fadd6 = (313,40)
    filename = sys.argv[1]
    output = sys.argv[2]
    out = open(output, 'wb')
    print ('procesando {}...'.format(filename))
    with open(filename,'rb') as handle:
        # csv_reader = csv.reader(handle,delimiter=sep,quotechar=quotechar) #
        # header_list = []
        # if skip_first_line:
        #     header_list = csv_reader.next()
        # for line in csv_reader:
        #     values = line
        #     dom = values[dom_field_no]
        #     cp = values[cp_field_no]
        #     asent = values [asent_field_no]
        #     cpi = postal_utils.cp_to_int(cp)

        #     new_asent = ''
        #     asentamientos = CPCons.get_asentamiento_by_cp(cpi)
        #     if len(asentamientos) == 1
        #         new_asent = asentamientos.iloc[0]['USUARIO']

            #if CPCons.cp_asentamiento_corresponde(cpi,asent)
        seq = 0
        cp_ok = 0
        asent_ok = 0
        for line in handle:
            seq += 1
            vcp = get_value(line,fcp)
            add1 = get_value(line,fadd1)
            add2 = get_value(line,fadd2)
            add3 = get_value(line,fadd3)
            add4 = get_value(line,fadd4)
            vcpi = postal_utils.cp_to_int(vcp)
            valid_cp = CPCons.cp_is_valid(vcpi,cpcons)
            # print add1
            # print add2
            # print add3
            # print vcpi
            dom = ''
            asent = ''
            asent_match = ' '
            new_asent = '' 
            calc_asent = ''
            if valid_cp:
                cp_ok += 1
                asent_add4 = CPCons.cp_asentamiento_corresponde(vcpi,add4,cpcons)
                asent_add3 = CPCons.cp_asentamiento_corresponde(vcpi,add3,cpcons)
                asent_add2 = CPCons.cp_asentamiento_corresponde(vcpi,add2,cpcons)

                if asent_add2 or asent_add3 or asent_add4: 
                    asent_ok += 1
                    new_asent = asent_add2 or asent_add3 or asent_add4
                if asent_add2:
                    asent_match = '2'
                    dom = add1
                    asent = add2
                else:
                    if asent_add3: 
                        asent_match = '3'
                        dom = add1 + ' ' + add2 
                        asent = add3
                    elif asent_add4:
                        asent_match = '4'
                        dom = add1 + ' ' + add2 + ' ' + add3  #revisar que pasa con add2 y add3
                        asent = add4
                    else:
                        calc_asent = CPCons.determina_asentamiento_from_cp(vcpi,cpcons)
                        if calc_asent:
                            dom = add1 + add2 if tiene_numeros_o_letras_aisladas(add2) else add1
                            asent_match = 'C'
                        else:
                            asent_elastic = get_asentamiento_elasticsearch(vcpi,add2.decode('iso8859-1'))
                            if asent_elastic:
                                asent_match = "E"
                                dom = add1
                                asent = add2
                                new_asent = asent_elastic['_source']['USUARIO']
                            else:
                                asent_match = "X"
                                dom = "_"
                                asent = "_"
            else:
                tuple1 = separa_num_colonia(add2)
                if tuple1:
                    asent_match = 'S'
                    dom = add1 + ' ' + tuple1[0]
                    asent = tuple1[1]
                else:
                    tuple2 = separa_num_matching_colonia(vcpi,add2,cpcons)
                    if tuple2:
                        asent_ok +=1
                        asent_match = 'M'
                        dom = add1 + ' ' + tuple2[0]
                        asent = tuple2[1]
                    else:
                        tuple3 = separa_num_colonia(add3)
                        if tuple3:
                            asent_match = 's'
                            dom = add1 + add2
                            asent = add3
                        else:
                            asent_match = 'x'
                            dom = "_"
                            asent = "_"
            
            dom = to_fixed_width(trim_and_reduce(dom))
            asent = to_fixed_width(asent)
            new_asent = to_fixed_width(new_asent)
            calc_asent = to_fixed_width(calc_asent) if calc_asent else to_fixed_width('')
            out.write(line[:-2])
            out.write('.')
            out.write('1' if valid_cp else '0')
            out.write(asent_match)
            out.write('.')
            out.write(dom)
            out.write(asent)
            # out.write(new_asent.encode('UTF-8'))
            out.write(new_asent.encode('iso8859-1'))
            out.write(calc_asent.encode('iso8859-1'))
            out.write('\n')
            sys.stdout.flush()
            if seq % 1000 == 0 :
                print "\r registros analizados: {} cp correcto: {} asentamiento correcto: {} duracion {} mins".format( seq, cp_ok, asent_ok,(time.time()-t1)/60.0)
        t2 = time.time()
        print "\r registros analizados: {} cp correcto: {} asentamiento correcto: {} duracion {} segs".format( seq, cp_ok, asent_ok,t2-t1)
        print "proceso tomo {} mins".format((t2-t1)/60.0)
        print "performance {} record/s".format(seq/(t2-t1))
        out.close()


                    
if __name__ == '__main__':
    main()


