#!/usr/bin/python3
# -*- encoding: utf-8 -*-

"""
CREATED    :
AUTHOR     :
DESCRIPTION: Core functions for processing
"""

import os
import json
import datetime as dt


class check():
    """
    Complete list of check functions for DPS.
    These are very simple functions designed to return bool (True/False) for a defined check
    """

    def chk_study(self, study):
        return True if ru.validstudy(study) else False

    def chk_request(self):
        print("runnin check 2")

    def chk_contract(self, din):
        response = "a{din}"
        return True if response else False

    def chk_allgood(self, vin):
        return True if all(value == True for value in vin.values()) else False

    def chk_inlist(vin, vlist):
        return True if vin in vlist else False

    def chk_access(self, sys):
        response = ""
        return True if response  else False



class cleanup():

    def cleanup1(self):
        print("Clean up1")

    def cleanup2(self):
        print("Clean up2")

    def cleanup3(self, path):
        # CLEAN UP A FOLDER BY REMOVING IT        
        os.system(f"rm {path}")        


class request():

    def request_jira(self):
        print("Clean up1")

    def request_cdse(self, din):
        for item in din:
            # DOWNLOAD CDSE COLLECTION
            cdse.downloadcollection(item['cid'])
            print("Clean up2")


    def request_email(self):
        print("Clean up3")        

    def request_advice(self):
        print("Clean up3")        

    def request_other(self):
        print("Clean up3")        

    def requestanon(self, din):
        gmail.sendmail()        
        print("Clean up3")        


class processutil():
    
    def listsort(self, sub_li=None, element=0, rev=False):
        """
        Sort list of lists by the nth element, 

        Args:
            sub_li ([type]): [description]
            element (int, optional): [description]. Defaults to 0.
            rev (bool, optional): [description]. Defaults to False.

        Returns:
            [type]: [description]
        """
        sub_li.sort(key = lambda x: x[element], reverse=rev)
        return sub_li

class reports():
    """Prints out various reports given specific data"""

    def report_cdse( din, po=True, file=None):
        if file:
            with open("output.txt", "a") as f:
                print('Hi', file=f)
                print('Hello from AskPython', file=f)
                print('exit', file=f)
        else:
            pass

report_cdse('')

import logging as lg
import os

def init_logging(outfile, outpath):
    rootlg = lg.getLogger('my_logger')

    os.makedirs(outpath) if not os.path.exists(outpath) else None
        
    fh = lg.FileHandler(f"{outpath}/{outfile}")

    rootlg.addHandler(fh)
    rootlg.setLevel(lg.DEBUG)
    ch = lg.StreamHandler()
    rootlg.addHandler(ch)

    return rootlg


lg = init_logging()

lg.debug('Hi! :)')


