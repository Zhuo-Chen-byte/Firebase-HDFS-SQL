import streamlit as st, pandas as pd, numpy as np, json, sys, requests, math, re
from command_lines import *
from MapReduce import *

st.title('Navigation EDFS')
# start at root

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
options = [''] + options
st.dataframe(command_list(st.session_state.curr_path))
option = st.selectbox('go to directory', options,
                      on_change=change_session_info_callback,
                      format_func= lambda x: 'Please select a subdirectory or file' if x == '' else x,
                      key='Current_Value')
