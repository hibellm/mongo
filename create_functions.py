#!/usr/bin/python3
# -*- encoding: utf-8 -*-

"""
CREATED    :
AUTHOR     :
DESCRIPTION:
"""

import pandas as pd
import numpy as np


# NUMERIC CATEGORY FUNCTION FROM DICTIONARY
# ( is > ) is <  , [ is >=, ] is <=

blist = ['(),[]']
comp = {'(,]': '', '(,)': '', '[,)': '', '[,]': ''}
num_dict = {'(0,10]': '<self>', '(10,40]': '>10', '(40,999]': 'too large'}

for entry in num_dict:
    comp_type = ''.join(filter(str.isalpha, entry))
    print(comp_type)

''.join(x for x in s if x.isalpha())












