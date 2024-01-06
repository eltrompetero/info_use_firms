# ====================================================================================== #
# Custom directories where information is to be found.
# Author : Eddie Lee, edlee@csh.ac.at
# ====================================================================================== #
import os

ORIGINAL_FIREHOSE_PATH='/mnt/cache1/corp/starter_packet/firehose'
assert os.path.isdir(ORIGINAL_FIREHOSE_PATH)

FIREHOSE_PATH='../starter_packet/firehose'
if not os.path.isdir(FIREHOSE_PATH):
    FIREHOSE_PATH='./starter_packet/firehose'
assert os.path.isdir(FIREHOSE_PATH)

DATADR = '../starter_packet' 
if not os.path.isdir(DATADR):
    DATADR='./starter_packet'
assert os.path.isdir(DATADR)

TEMP_PATH = './tmp'
assert os.path.isdir(TEMP_PATH)

