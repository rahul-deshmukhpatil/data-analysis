#!/usr/bin/python

# Global imports
import os
import sys
import numpy as np
import pandas as pd
import getopt

from sys import argv
from os.path import exists
from threading import Lock 

#local imports
sys.path.insert(0, '../common')
from error_print import eprint

# Usage help
###############################################################
def usage():
    eprint(" ****************************************************************************")
    eprint(" %s usage : " %(argv[0]))
    eprint(" %s -f <data file> [-g]" %(argv[0]))
    eprint(" eg %s -f trades.txt")
    eprint(" -f --file  : data file containing quantity and price of trades, with header")
    eprint(" -h --help  : help")
    eprint(" ****************************************************************************")
    
# Main 
###############################################################
def main_iterator():
    #get the data into data frame
    ts = pd.read_csv(tradesFile, engine='c', sep=' ', header=0 )
    rows = ts.shape[0]

    balance = 0.0
    on_book_qty = 0
    # for each time series calculate the data quality
    for i in range(0, rows):
        qty = int(ts[[0]].iloc[i])
        price = float(ts[[1]].iloc[i])
        balance -= price * qty
        on_book_qty += qty
        profit = balance + (on_book_qty *  price)
        print("%d] qty : %d, last_price : %f, balance : %f, on_book_qty : %d. profit : %f" %(i, qty, price, balance, on_book_qty, profit))


# main function: 
# initialize locks and global variables
# get the command line options
# check if input file exists
# start the main iterator function
######################################################################
def main():
    # to synchronize the output
    global print_lock
    print_lock = Lock()
    try:
        opts, args = getopt.getopt(sys.argv[1:], "hf:", ["help", "file="])
    except getopt.GetoptError as err:
        # print help information and exit:
        print str(err)  # will print something like "option -a not recognized"
        usage()
        sys.exit(2)

    global tradesFile 
    tradesFile = ''

    for o, a in opts:
        if o in ("-f", "--file"):
            tradesFile = a
        elif o in ("-h", "--help"):
            usage()
            sys.exit()
        else:
            assert False, "unhandled option"

    #read the data file location
    absPath= os.getcwd()  + '/' + tradesFile

    #see if file exists and then proceed
    if not exists(tradesFile):
        if(tradesFile == ''):
            eprint("Please provide valid trades file as -f argument on command line")
        else:
            eprint("Data file does not exists aborting : %s" %(tradesFile))
            eprint("abs path : %s" %(absPath))
        usage()
        sys.exit()
    else:
        eprint("Processing data file : %s " %(tradesFile))

    main_iterator()

# Ivoke the main
################################################
main()
