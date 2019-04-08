#!/usr/bin/env python
# -*- coding: utf-8 -*-
#import csv
import codecs
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

# def separa_num_matching_colonia(cp,value,cpcons):
#     asent = CPCons.cp_asentamiento_corresponde(cp,value,cpcons)
#     if asent:
#         res = re.match(r'(.*)({}.*)'.format(slugify(asent)),slugify(value),re.DOTALL|re.IGNORECASE)
#         return (res.group(1).replace('-',' '), res.group(2).replace('-',' '))
#     return False

def tiene_numeros_o_letras_aisladas(value):
    tiene_num = re.search(r'[^\s\-\.\,]\d+',value) #digitos
    tiene_letra_aislada = re.search(r'[\s\-^]\w[\s\-$\.]',value,re.IGNORECASE) #letras aisladas
    return tiene_num or tiene_letra_aislada

def main():
    if len(sys.argv) < 3:
        print "uso: {} <input file> <output file> (adecuar ademas posicion y longitud de campos en el codigo!!!)".format(sys.argv[0])
    filename = sys.argv[1]
    output = sys.argv[2]
    t0 = time.time()
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
    fcp = (353,6)
    fadd1 = (113,40)
    fadd2 = (153,40)
    fadd3 = (193,40)
    fadd4 = (233,40)
    fadd5 = (273,40)
    fadd6 = (313,40)
    
    #out = open(output, 'wb')
    out = codecs.open(output, encoding='iso-8859-1', mode='w')
    print ('procesando {}...'.format(filename))
    with codecs.open(filename, encoding='iso-8859-1') as handle:
    #with open(filename,'r') as handle:
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
        stats = dict(seq = 0,
            cp_ok = 0,
            cyn_calculated = 0,
            asent_corresponde = 0,
            mun_corresponde = 0,
            cd_corresponde = 0,
            edo_corresponde = 0,
            )
        
        result = dict(calleynum = '',
                    col='',
                    cdmun = '',
                    edo ='')
        line_length = -1
        for line in handle:
            stats['seq'] += 1
            if line_length == -1:
                line_length = len(line)
            else:
                if not line_length == len(line):
                    raise ValueError("line len not correct at line {}".format(stats['seq']))
            result = dict(calleynum = '',
                    col='',
                    mun = '',
                    cd = '',
                    edo ='')

            

            cp = get_value(line,fcp)
            add1 = trim_and_reduce(get_value(line,fadd1))
            add2 = trim_and_reduce(get_value(line,fadd2))
            add3 = trim_and_reduce(get_value(line,fadd3))
            add4 = trim_and_reduce(get_value(line,fadd4))
            add5 = trim_and_reduce(get_value(line,fadd5))
            add6 = trim_and_reduce(get_value(line,fadd6))

            add_list = [add1,add2,add3,add4,add5,add6]

            add_list = postal_utils.dedup(add_list)
            add_list_original = add_list[:]
            add_list = postal_utils.delete_empty(add_list)
            
            #print stats['seq'],cp,add_list

            cpi = postal_utils.cp_to_int(cp)

            res =  CPCons.match_backwards_in_list(add_list, cpi, cpcons, CPCons.cp_edo_corresponde)
            if res:
                stats['edo_corresponde'] +=1
                result['edo'] = res[0]
                add_list = res[1] #remaining fields
                
                
            valid_cp = CPCons.cp_is_valid(cpi,cpcons)
            if valid_cp: 
                stats['cp_ok'] +=1
                resc =  CPCons.match_backwards_in_list(add_list, cpi, cpcons, CPCons.cp_ciudad_corresponde)
                if resc:
                    stats['cd_corresponde'] += 1
                    result['cd'] = resc[0]
                    add_list = resc[1]

                resm =  CPCons.match_backwards_in_list(add_list, cpi, cpcons, CPCons.cp_mun_corresponde)
                if resm:
                    stats['mun_corresponde'] += 1
                    result['mun'] = resm[0]
                    add_list = resm[1]

                resa = CPCons.match_backwards_in_list(add_list,cpi,cpcons,CPCons.cp_asentamiento_corresponde)
                if resa:
                    stats['asent_corresponde'] +=1
                    result['col'] = resa[0]
                    add_list = resa[1]
            
            
            if len(add_list) > 1 : 
                ress1 =  separa_num_colonia(add_list[1])
                if ress1:
                    result['calleynum'] = add_list[0] +" " + ress1[0]
                else:
                    ress2 = tiene_numeros_o_letras_aisladas(add_list[1])
                    if ress2:
                        result['calleynum'] = add_list[0] + " " + add_list[1]
            elif len(add_list) == 1:
                result['calleynum'] = add_list[0]
                stats['cyn_calculated'] += 1
            #print result
            domicilio_estandarizado = valid_cp and \
                                        len(result['calleynum'])>0 and \
                                        len(result['col'])>0 and \
                                        len(result['mun'])>0 and \
                                        len(result['cd'])>0 and \
                                        len(result['edo'])>0
            out.write(line[:-2])
            out.write('|')
            out.write('cp' + ('1' if valid_cp else '0'))
            out.write('OK' + ('1' if domicilio_estandarizado else '0'))
            out.write(' cyn' + ('1' if result['calleynum'] else '0' ))
            out.write(' col' + ('1' if result['col'] else '0' ))
            out.write(' mun' + ('1' if result['mun'] else '0' ))
            out.write(' cd' + ('1' if result['cd'] else '0' ))
            out.write(' edo' + ('1' if result['edo'] else '0' ))
            out.write('|')

            if domicilio_estandarizado:
                if cpi >=1000 and cpi<=19999:
                    if result['cd'] == result['edo']:
                        result['cd'] = ''  #ciudad de mexico se repite en cd y edo
            
            if not domicilio_estandarizado:
                
                result['calleynum'] = add_list_original[0] if len(add_list_original)>0 else '' 
                result['col'] = add_list_original[1] if len(add_list_original)>1 else '' 
                result['mun'] =  add_list_original[2] if len(add_list_original)>2 else '' 
                result['cd'] = add_list_original[3] if len(add_list_original)>3 else '' 
                result['edo'] = trim_and_reduce(" ".join(add_list_original[4:])) if len(add_list_original)>4 else ''
                
            for k in ['calleynum', 'col', 'mun','cd', 'edo']:
                #print k
                out.write(to_fixed_width(result[k]))
            out.write('\n')
            if stats['seq'] % 1000 == 0:
                print stats
        out.close()
        t2 = time.time()
        print "terminado"
        print "proceso tomo {} mins".format((t2-t1)/60.0)
        print "performance {} record/s".format(1.0*:stats['seq']/(t2-t1))
            
if __name__ == '__main__':
    main()



            

