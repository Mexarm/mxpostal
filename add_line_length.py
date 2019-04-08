#!/usr/bin/env python
# -*- coding: utf-8 -*-
#import csv
import codecs
import sys
import time

def main():
    if len(sys.argv) < 3:
        print "agrega line length uso: {} <input file> <output file> ".format(sys.argv[0])
    filename = sys.argv[1]
    output = sys.argv[2]
    t0 = time.time()
    out = codecs.open(output, encoding='iso-8859-1', mode='w')
    print ('procesando {}...'.format(filename))
    with codecs.open(filename, encoding='iso-8859-1') as handle:
        for i,line in enumerate(handle):
            print i
            out.write(line[:-2])
            out.write('|')
            out.write("%04d" % (len(line),))
            out.write('\n')
        out.close()
        t1 = time.time()
        print "terminado"
        print "proceso tomo {} mins".format((t1-t0)/60.0)
        #print "performance {} line/s".format(seq/(t1-t0))
            
if __name__ == '__main__':
    main()



            

