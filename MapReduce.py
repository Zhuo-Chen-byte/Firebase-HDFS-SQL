# Import necessary packages
import pandas as pd, numpy as np, json, sys, requests, math, re
from command_lines import *

# Piecewise_ is a dict, while condition_ is a condition to check some attribute (<, >, <=, >=, =, or regex)
def checkCondition(piecewise_, condition_):
    if '<' in condition_ and '<=' not in condition_:
        v1, v2 = condition_.split('<')
        v1, v2 = v1.strip(), v2.strip()
        
        att, val = '', ''

        if v1 in piecewise_:
            try:
                att, val = v1, float(v2)
            except:
                att, val = v1, v2
                
            return piecewise_[att] < val
        else:
            try:
                att, val = v2, float(v1)
            except:
                att, val = v2, v1
            
            return piecewise_[att] > val
    
    elif '>' in condition_ and '>=' not in condition_:
        v1, v2 = condition_.split('>')
        v1, v2 = v1.strip(), v2.strip()
        
        att, val = '', ''
        
        if v1 in piecewise_:
            try:
                att, val = v1, float(v2)
            except:
                att, val = v1, v2
            
            return piecewise_[att] > val
        else:
            try:
                att, val = v2, float(v1)
            except:
                att, val = v2, v1
            
            return piecewise_[att] < val
        
    elif '<=' in condition_:
        v1, v2 = condition_.split('<=')
        v1, v2 = v1.strip(), v2.strip()
        
        att, val = '', ''
        
        if v1 in piecewise_:
            try:
                att, val = v1, float(v2)
            except:
                att, val = v1, v2
            
            return piecewise_[att] >= val
        else:
            try:
                att, val = v2, float(v1)
            except:
                att, val = v2, v1
            
            return piecewise_[att] <= val
        
    elif '>=' in condition_:
        v1, v2 = condition_.split('>=')
        v1, v2 = v1.strip(), v2.strip()
        
        att, val = '', ''
        
        if v1 in piecewise_:
            try:
                att, val = v1, float(v2)
            except:
                att, val = v1, v2
                
            return piecewise_[att] >= val
        else:
            try:
                att, val = v2, float(v1)
            except:
                att, val = v2, v1
            
            return piecewise_[att] <= val
            
    elif '=' in condition_:
        v1, v2 = condition_.split('=')
        v1, v2 = v1.strip(), v2.strip()
        
        att, val = '', ''
        
        if v1 in piecewise_:
            try:
                att, val = v1, float(v2)
            except:
                att, val = v1, v2
        else:
            try:
                att, val = v2, float(v1)
            except:
                att, val = v1, v2
        
        return piecewise_[att] == val
    
    elif 'regex' in condition_:
        att, pattern = condition_.split('regex')
        pattern = val[1:len(val) - 1]
        
        return re.match(pattern, piecewise_[att])

# map function
def mapPartition(partition_, conditions_, selected_attributes, groupbys):
    intermediate_ = []
    
    for piecewise_ in partition_:
        append_ = True
        
        for condition_ in conditions_:
            if not checkCondition(piecewise_, condition_):
                append_ = False
                break
        
        if not append_:
            continue
            
        intermediate_.append(piecewise_)
    
    if not intermediate_:
        return {}
    
    combined = {}
    
    if groupbys:
        combined['count key'] = {}
        reduced = reduce(intermediate_, groupbys) # A dict group by key
        
        for selected_attribute in selected_attributes:
            if selected_attribute not in combined:
                combined[selected_attribute] = {}

            for key in reduced:
                kmp = reduced[key]
                
                for subkey in kmp:
                    combined['count key'][key] = len(kmp[subkey])
                    break
                
                if re.match('count(.*)', selected_attribute):
                    combined[selected_attribute][key] = len(reduced[key][selected_attribute[6:-1]])
                elif re.match('min(.*)', selected_attribute):
                    combined[selected_attribute][key] = min(reduced[key][selected_attribute[4:-1]])
                elif re.match('max(.*)', selected_attribute):
                    combined[selected_attribute][key] = max(reduced[key][selected_attribute[4:-1]])
                elif re.match('sum(.*)', selected_attribute):
                    combined[selected_attribute][key] = sum(reduced[key][selected_attribute[4:-1]])
                elif re.match('avg(.*)', selected_attribute):
                    att_sum = 'sum(' + selected_attribute[4:-1] + ')'
                    
                    if att_sum not in combined:
                        combined[att_sum] = {}
                    
                    combined[att_sum][key] = sum(reduced[key][selected_attribute[4:-1]])
                    combined[selected_attribute][key] = combined[att_sum][key] / combined['count key'][key]
                elif selected_attribute not in groupbys: # a group-by key element
                    raise Exception('Invalid query')
    else:
        count_ = len(intermediate_)
        combined['count'] = count_
        
        for selected_attribute in selected_attributes:
            if re.match('count(.*)', selected_attribute) or re.match('min(.*)', selected_attribute) or re.match('max(.*)', selected_attribute) or re.match('sum(.*)', selected_attribute) or re.match('avg(.*)', selected_attribute):
                att = selected_attribute[selected_attribute.find('(') + 1:selected_attribute.find(')')]
                max_num, min_num, sum_num = -float('inf'), float('inf'), 0
                max_str, min_str = intermediate_[0][att], intermediate_[0][att]
                
                for piecewise_ in intermediate_:
                    try:
                        max_num, min_num, sum_num = max(max_num, piecewise_[att]), max(max_num, piecewise_[att]), sum_num + piecewise_[att]
                    except:
                        max_str, min_str = max(max_str, piecewise_[att]), min(max_num, piecewise_[att])
                
                if re.match('count(.*)', selected_attribute):
                    combined[selected_attribute] = count_
                elif re.match('min(.*)', selected_attribute):
                    combined[selected_attribute] = min_num if min_num < float('inf') else min_str
                elif re.match('max(.*)', selected_attribute):
                    combined[selected_attribute] = max_num if max_num > -float('inf') else max_str
                elif re.match('sum(.*)', selected_attribute):
                    combined[selected_attribute] = sum_num
                elif re.match('avg(.*)', selected_attribute):
                    combined[selected_attribute] = sum_num / count_
            else:
                if selected_attribute not in combined:
                    combined[selected_attribute] = []
                
                for piecewise_ in intermediate_:
                    combined[selected_attribute].append(piecewise_[selected_attribute])
                    
                
    return combined


def reduce(intermediate_, groupbys):
    reduced = {}
    
    for piecewise_ in intermediate_:
        key_ = ()
        
        for groupby in groupbys:
            key_ += (piecewise_[groupby],)
        
        if key_ not in reduced:
            reduced[key_] = {}
        
        for att in piecewise_:
            if att not in reduced[key_]:
                reduced[key_][att] = []
        
            reduced[key_][att].append(piecewise_[att])

    return reduced


def reducePartition(combined_partitions, groupbys):
    reduced_ = {}
    count_key = {}
    count_ = 0
    
    if not groupbys:
        for combined_partition in combined_partitions:
            for att in combined_partition:
                if att not in reduced_:
                    reduced_[att] = combined_partition[att]
                elif re.match('count(.*)', att) or re.match('sum(.*)', att):
                    reduced_[att] += combined_partition[att]
                elif re.match('max(.*)', att):
                    reduced_[att] = max(reduced_[att], combined_partition[att])
                elif re.match('min(.*)', att):
                    reduced_[att] = min(reduced_[att], combined_partition[att])
                else:
                    reduced_[att] += combined_partition[att]
                    
            count_ += combined_partition['count']
        
        # We use sum / len to calculate avg
        for key in reduced_:
            if re.match('avg(.*)', key):
                reduced_[key] = reduced_['sum(' + key[4:-1] + ')'] / count_
    else:
        for combined_partition in combined_partitions:
            for grouped in combined_partition:
                if grouped == 'count key':
                    for key in combined_partition[grouped]:
                        if key not in count_key:
                            count_key[key] = 0
                        
                        count_key[key] += combined_partition['count key'][key]
                    
                    continue
                
                if grouped not in reduced_:
                    reduced_[grouped] = {}
                    
                for key in combined_partition[grouped]:
                    if key not in reduced_[grouped]:
                        reduced_[grouped][key] = combined_partition[grouped][key]
                    elif re.match('count(.*)', grouped) or re.match('sum(.*)', grouped):
                        reduced_[grouped][key] += combined_partition[grouped][key]
                    elif re.match('max(.*)', grouped):
                        reduced_[grouped][key] = max(reduced_[grouped][key], combined_partition[grouped][key])
                    elif re.match('min(.*)', grouped):
                        reduced_[grouped][key] = min(reduced_[grouped][key], combined_partition[grouped][key])
                      
        # We use sum / count to calculate avg
        for grouped in reduced_:
            if re.match('avg(.*)', grouped):
                for key in reduced_[grouped]:
                    sum_grouped = 'sum(' + grouped[4:-1] + ')'
                    reduced_[grouped][key] = round(reduced_[sum_grouped][key] / count_key[key], 3)
                    
    return reduced_

def output(reduced_, selected_attributes, groupbys, orderbys, offset, limit):
    df = pd.DataFrame()
    
    if not groupbys:
        df = pd.DataFrame(columns=selected_attributes)
        
        for selected_attribute in selected_attributes:
            if not isinstance(reduced_[selected_attribute], list):
                df[selected_attribute] = [reduced_[selected_attribute]]
            else:
                df[selected_attribute] = reduced_[selected_attribute]
    else:
        # Query out group by keys
        groupby_keys = list(reduced_['count key'].keys())
        
        for i in range(len(groupby_keys)):
            groupby_keys[i] = list(groupby_keys[i])

        df = pd.DataFrame(columns=groupbys, data=groupby_keys)
    
        
        for key in reduced_:
            if key == 'count' or key == 'count key' or not reduced_[key]:
                continue
            
            dt_points = []
            
            for gpkey in reduced_[key]:
                dt_points.append(reduced_[key][gpkey])
            
            df[key] = dt_points
    
    return df[selected_attributes].sort_values(by=orderbys).iloc[offset:].head(limit).reset_index(drop=True)
    

# command_put('winequality-red-v1.csv', '/usr/Females/Yifan', 5)

# p1 = command_readPartition('/usr/Males/John/winequality-red-v1.csv', 1)
# # p3 = command_readPartition('/usr/Males/John/winequality-red-v1.csv', 3)
# # print(p1)
# # print(command_getPartitionLocations('/usr/Females/Yifan/winequality-red-v1.csv'))
# # cod = ['quality > 6']
# cod = []
# # #att = ['quality', 'max(volatile acidity)', 'min(citric acid)', 'avg(alcohol)']
# att = ['volatile acidity', 'citric acid', 'alcohol']
# #gpbys = ['quality', 'chlorides']
# # gpbys = []
# # odbys = []
# # odbys = ['volatile acidity']
# offset = 0
# limit = 100
# #
# #
# mp1 = mapPartition(p1, cod, att, gpbys)
# # mp3 = mapPartition(p3, cod, att, gpbys)
#
# print(output(mp1, att, gpbys, odbys, offset, limit))
