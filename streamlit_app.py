import streamlit as st, pandas as pd, numpy as np, json, sys, requests, math, re
from command_lines import *
from MapReduce import *
st.title('EDFS Commands')
commands = {"command":['mkdir', 'ls', 'cat', 'rm', 'put', 'getPartitionLocations', 'readPartition'],
            "usage": ['create a directory', 'list contents of directory', 'display content of a specific file', 'delete a file', 'upload a file', ' locations of partitions of the file', 'the content of partition # of the specified file'],
            "example": ['mkdir /user/john', 'ls /user', 'cat /user/john/hello.txt', 'rm /user/john/hello.txt', 'put(cars.csv, /user/john, number of partitions)', 'getPartitionLocations(/user/john/hello.txt)', 'readPartition(/user/john/hello.txt, partition#)']}

commands_df= pd.DataFrame(data = commands)
commands_df

col1, col2 = st.columns(2)
with col1:
    command = st.text_input('Enter your command', value="", placeholder = 'please enter command, mkdir /user/john')
with col2:
    confirm_input = st.button('click here to confirm command')

def command_output(command):
    if command[:5] == 'mkdir':
        command_make_dir(command[6:])
        return 'successfully make a directory'
    elif command[:2] == 'ls':
        return command_list(command[3:])
    elif command[:3] == 'cat':
        return command_cat(command[4:])
    elif command[:2] == 'rm':
        return command_rm(command[3:])
    elif command[:3] == 'put':
        user_input = command[4:-1]
        user_input = user_input.replace(" ", "")
        user_input_sep = user_input.split(',')
        return command_put(user_input_sep[0], user_input_sep[1], int(user_input_sep[2]))
    elif command[:21] == 'getPartitionLocations':
        return command_getPartitionLocations(command[22:-1])
    elif command[:13] == 'readPartition':
        user_input = command[14:-1]
        user_input = user_input.replace(" ", "")
        user_input_sep = user_input.split(',')
        return pd.DataFrame(command_readPartition(user_input_sep[0], int(user_input_sep[1])))
    else:
        st.error('invalid command, please try again',  icon="🚨")

if confirm_input:
    st.write(command_output(command))
