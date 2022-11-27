import streamlit as st, pandas as pd, numpy as np, json, sys, requests, math, re
from command_lines import *
from MapReduce import *

queries = {"command":['select', 'from', 'where', 'group by', 'order by', 'offset', 'limit'],
            "usage": ['create a directory', 'list contents of directory', 'display content of a specific file', 'delete a file', 'upload a file', ' locations of partitions of the file', 'the content of partition # of the specified file'],
            "example": ['select quality, max(volatile acidity) from /usr/Males/John/winequality-red-v1.csv group by quality order by quality offset 1 limit 1']}

#queries = pd.DataFrame(data = queries)
#queries_df

st.title('EDFS Search and Analysis')
st.subheader('we support sql-like query')
st.text('supported clause: select, where, group by, order by, offset, limit, \n'
        'max(), min(), count(), sum(), avg()')
st.text('query example: \n select quality, max(volatile acidity) from /usr/Males/John/winequality-red-v1.csv group by quality order by citric acid offset 1 limit 1')
query = st.text_input('Enter your query:', value="", placeholder = 'please enter sql query, like: select xxxx from xxx where xxx')

confirm_input = st.button('click here to confirm query')
see_partition_output = st.button('click here to get results from each partition')

def command_output(query, see_partition = False):
    # try:
    nextKeyWords = queries['command'][1:]

    # select **
    raw_selects, selects = query[query.find('select') + 6:query.find('from')].split(','), []


    for i in range(len(raw_selects)):
        raw_selects[i] = raw_selects[i].strip()

        if not raw_selects[i]:
            continue

        selects.append(raw_selects[i])


    nextKeyWords = nextKeyWords[1:]

    # from ***
    query = query[query.find('from'):]
    source = ''

    for nextKeyWord in nextKeyWords:
        if nextKeyWord in query:
            source = query[4:query.find(nextKeyWord)].strip()
            break

    if not source:
        source = query[4:].strip()
    # if select *
    if '*' in selects:
        selects += list(command_readPartition(source, 1)[0].keys())
        selects = list(set(selects))
        selects.remove('*')

    nextKeyWords = nextKeyWords[1:]

    # where ***
    conditions_query = ''
    conditions_or = []

    if 'where' in query:
        query = query[query.find('where') + 5:]

        for nextKeyWord in nextKeyWords:
            if nextKeyWord in query:
                conditions_query = query[:query.find(nextKeyWord)]
                break

        if not conditions_query:
            conditions_query = query

        conditions_or = conditions_query.split('or')

        for i in range(len(conditions_or)):
            conditions_and = conditions_or[i].split('and')
            conditions = []

            for condition_and in conditions_and:
                if not condition_and.strip():
                    continue

                conditions.append(condition_and.strip())

            conditions_or[i] = conditions

        nextKeyWords = nextKeyWords[1:]

    # group by ***
    groupby_query = ''
    groupbys = []

    if 'group by' in query:
        query = query[query.find('group by') + 8:]

        for nextKeyWord in nextKeyWords:
            if nextKeyWord in query:
                groupby_query = query[:query.find(nextKeyWord)]
                break

        if not groupby_query:
            groupby_query = query.strip()

        groupbys = groupby_query.split(',')

        for i in range(len(groupbys)):
            groupbys[i] = groupbys[i].strip()

        nextKeyWords = nextKeyWords[1:]

    # order by ***
    orderby_query = ''
    orderbys = []

    if 'order by' in query:
        query = query[query.find('order by') + 8:]

        for nextKeyWord in nextKeyWords:
            if nextKeyWord in query:
                orderby_query = query[:query.find(nextKeyWord)]
                break

        if not orderby_query:
            orderby_query = query.strip()

        orderbys = orderby_query.split(',')

        for i in range(len(orderbys)):
            orderbys[i] = orderbys[i].strip()

    nextKeyWords = nextKeyWords[1:]

    # limit ***
    limit_query = ''
    limit = float('inf')

    if 'limit' in query:
        query = query[query.find('limit') + 5:]

        for nextKeyWord in nextKeyWords:
            if nextKeyWord in query:
                limit_query = query[:query.find(nextKeyWord)].strip()
                break

        if not limit_query:
            limit_query = query.strip()

        limit = int(limit_query)
        nextKeyWords = nextKeyWords[1:]

    # offset ***
    offset_query = ''
    offset = 0

    if 'offset' in query:
        query = query[query.find('offset') + 6:]

        for nextKeyWord in nextKeyWords:
            if nextKeyWord in query:
                offset_query = query[:query.find(nextKeyWord)].strip()
                break

        if not offset_query:
            offset_query = query.strip()

        offset = int(offset_query)

    partition_locations = command_getPartitionLocations(source)
    partitions = []

    for i in range(len(partition_locations)):
        partitions.append(command_readPartition(source, i + 1))

    mapPartitions = []

    partitionNum = 1
    for partition in partitions:
        if not conditions_or:
            mapPartitions.append(mapPartition(partition, [], selects, groupbys, orderbys))
            if see_partition == True:
                'output from partition', partitionNum, ':'
                st.dataframe(mapPartition(partition, [], selects, groupbys, orderbys))
        else:
            for conditions in conditions_or:
                mapPartitions.append(mapPartition(partition, conditions, selects, groupbys, orderbys))
                if see_partition == True:
                    'output from partition', partitionNum, ':'
                    st.dataframe(mapPartition(partition, conditions, selects, groupbys, orderbys))
        partitionNum += 1

    reduced_partitions = reducePartition(mapPartitions, groupbys)
    output_ = output(reduced_partitions, selects, groupbys, orderbys, limit, offset, True)
    st.subheader('Final Output:')
    return output_
    # except Exception as e:
    #     print(e.__traceback__)
    #     st.error('Invalid query')
        
if confirm_input:
    st.write(command_output(query, False))

if see_partition_output:
    st.write(command_output(query, True))