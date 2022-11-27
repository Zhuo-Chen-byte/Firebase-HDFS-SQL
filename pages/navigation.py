import streamlit as st, pandas as pd, numpy as np, json, sys, requests, math, re
from command_lines import *
from MapReduce import *

st.title('Navigation through the EDFS')
# start at root

# return to root directory
back_to_root = st.button('Click here to return to root directory')
if back_to_root:
    st.session_state.curr_path = '/'

# return to parent directory
back_to_parent = st.button('Click here to return to parent directory')
if back_to_parent:
    temp = st.session_state.curr_path
    if temp == '/':
        st.error('no parent directory')
    else:
        temp = temp[::-1]
        temp = temp[temp.find('/', temp.find('/')+1):][::-1]
        st.session_state.curr_path = temp


def change_session_info_callback():
    currentValue = st.session_state.Current_Value
    if currentValue == '':
        # Select 'NULL' do nothing
        pass
    else:
        st.session_state.curr_path += currentValue + '/';

if 'curr_path' not in st.session_state:
    st.session_state.curr_path = '/'

"current path is: " + st.session_state.curr_path

options = command_list(st.session_state.curr_path)
if options == 'Nothing in this directory':
    st.info('Nothing in this directory', icon="ℹ️")
    st.stop()
options = [''] + options

children = pd.DataFrame(options, columns=['Name'])
def assign_type(df):
    type = []
    for c in children['Name']:

        if c[-3:] == 'csv':
            type.append('file')
        else:
            type.append('directory')
    df['Type'] = type
    return df
assign_type(children)
st.dataframe(children.iloc[1:], use_container_width=True)
# cannot select if a file
# remove it from selectbox
options = list(filter(lambda x: x[-3:] != 'csv', options))

option = st.selectbox('go to directory', options,
                      on_change = change_session_info_callback,
                      format_func = lambda x: 'Please select a subdirectory' if x == '' else x,
                      key='Current_Value')

