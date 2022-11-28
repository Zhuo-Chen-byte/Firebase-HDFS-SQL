import streamlit as st, pandas as pd, numpy as np, json, sys, requests, math, re
from command_lines import *
from MapReduce import *
st.title('EDFS Commands (Case Sensitive)')
commands = {"command":['restartDB', 'mkdir', 'ls', 'cat', 'rm', 'put', 'getPartitionLocations', 'readPartition'],
            "usage": ['restart the database <size of a dataNode>, <the number of dataNodes>', 'create a directory', 'list contents of directory', 'display content of a specific file', 'delete a file', 'upload a file', ' locations of partitions of the file', 'the content of partition # of the specified file'],
            "example": ['RestartDB 1000, 10', 'mkdir /user/john', 'ls /user', 'cat /user/john/cars.csv', 'rm /user/john/cars.csv', 'put(cars.csv, /user/john, number of partitions)', 'getPartitionLocations(/user/john/hello.txt)', 'readPartition(/user/john/hello.txt, partition#)']}

commands_df = pd.DataFrame(data = commands)
commands_df

command = st.text_input('Enter your command', value="", placeholder = 'please enter command, mkdir /user/john')

confirm_input = st.button('click here to confirm command')

def command_output(command):
    if command[:9] == 'restartDB':
        try:
            command = command[command.find('w') + 9:].strip()
            size_, num_ = command.split(',')
            startDatabase(int(size_.strip()), int(num_.strip()))
            st.success('successfully restart the whole database')
        except:
            st.error('invalid command, please try again',  icon="🚨")
    elif command[:5] == 'mkdir':
        try:
            command_make_dir(command[6:])
            st.success('successfully make a directory')
        except:
            st.error('invalid command, please try again',  icon="🚨")
    elif command[:2] == 'ls':
        try:
            if command_list(command[3:]) != 'Nothing in this directory':
                return command_list(command[3:])
            else:
                st.info('Nothing in this directory',icon="ℹ️")
        except:
            st.error('invalid command, please try again',  icon="🚨")
    elif command[:3] == 'cat':
        try:
            st.dataframe(command_cat(command[4:]))
            return 'display file {}'.format(command[4:])
        except:
            st.error('invalid command, please try again',  icon="🚨")
    elif command[:2] == 'rm':
        try:
            rm_try =  command_rm(command[3:])
        
            if rm_try == 'finished deleting':
                st.success(rm_try)
        except:
            st.error('invalid command, please try again',  icon="🚨")
    elif command[:3] == 'put':
        try:
            user_input = command[4:-1]
            user_input = user_input.replace(" ", "")
            user_input_sep = user_input.split(',')
            put_try = command_put(user_input_sep[0], user_input_sep[1], int(user_input_sep[2]))
            if put_try == 'successfully put a file':
                st.success(put_try)
        except:
            st.error('invalid command, please try again',  icon="🚨")
    elif command[:21] == 'getPartitionLocations':
        try:
            return command_getPartitionLocations(command[22:-1])
        except:
            st.error('invalid command, please try again',  icon="🚨")
    elif command[:13] == 'readPartition':
        try:
            user_input = command[14:-1]
            user_input = user_input.replace(" ", "")
            user_input_sep = user_input.split(',')
            st.write('you are seeing partition {} of file {}'.format(user_input_sep[1], user_input_sep[0]))
            return command_readPartition(user_input_sep[0], int(user_input_sep[1]))
        except:
            st.error('invalid command, please try again',  icon="🚨")
    else:
        st.error('invalid command, please try again',  icon="🚨")

if confirm_input:
    command_output_ui = command_output(command)
    if command_output_ui:
        st.write(command_output_ui)
