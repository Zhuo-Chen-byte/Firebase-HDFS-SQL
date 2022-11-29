# Import necessary packages
import pandas as pd, numpy as np, json, sys, requests, math


# Base url and the total number of datanodes created
base_url = 'https://dsci-551-group-project-default-rtdb.firebaseio.com'
existing_files = set()
DataNodeSize = 1000
numDataNodes = 10
numPieces = 0

# Restart the database
def startDatabase(DataNodeSize_, numDataNodes_):
    r = requests.delete(base_url + '/.json')
    
    existing_files = set()
    DataNodeSize = DataNodeSize_
    numDataNodes = numDataNodes_
    numPieces = 0
    
    for i in range(numDataNodes):
        r = requests.put(base_url + '/DataNodes/DataNode' + str(i) + '.json', json.dumps(''))

# Help function to partite a file into a given number of partitions
def partite(df, num_partitions):
    global DataNodeSize, numDataNodes, numPieces
    
    partition_length = math.ceil(len(df) / num_partitions)
        
    partitions = []
    partition = []
        
    parameters = df.columns
                
    for i in range(len(df)):
        piecewise_df_dict = {}
            
        for parameter in parameters:
            piecewise_df_dict[parameter] = df[parameter].iloc[i]
                
            if isinstance(df[parameter].iloc[i], np.int64):
                piecewise_df_dict[parameter] = int(df[parameter].iloc[i])
            
        partition.append(piecewise_df_dict)

        if len(partition) == partition_length:
            partitions.append(partition)
            partition = []
        
    if len(partition):
        partitions.append(partition)
            
    return partitions


# Help function to find the optimal DataNode to insert
def dataNode_to_insert(input_partition):
    global DataNodeSize, numDataNodes, numPieces
    
    dataNodes_url = base_url + '/DataNodes/DataNode'
        
    for i in range(numDataNodes):
        num_piece_info = 0
        partitions = requests.get(dataNodes_url + str(i) + '.json').json()
            
        if not partitions:
            return 'DataNode' + str(i)
            
        for partition in partitions:
            num_piece_info += len(partitions[partition])
            
        if len(input_partition) <= DataNodeSize - num_piece_info:
            return 'DataNode' + str(i)
        
    return False
        
    
def command_put(file_name, dir, num_partitions):
    global DataNodeSize, numDataNodes, numPieces
    
    complete_filepath = dir + '/' + file_name
        
    # If the file already exists, then we overwrite it
    if complete_filepath in existing_files:
        print('Overwrite the file {}'.format(complete_filepath))
        command_rm(complete_filepath)
    
    df = pd.read_csv(file_name)
    
    if numPieces + len(df) > DataNodeSize * numDataNodes:
        print('There is not enough space to store file {}'.format(file_name))
        return False
        
    org_file_name, format = file_name.split('.')
    file_name = file_name.replace('.', '_')
    
    partitions_ = partite(df, num_partitions)
    dataNodes_url = base_url + '/DataNodes/'
    nameNodes_url = base_url + '/root' + dir + '/' + file_name
        
    partition_idx = 1
        
    # Store data
    put_success = True
        
    for i in range(len(partitions_)):
        if len(partitions_[i]) >= DataNodeSize:
            print('Partition size of {} is too large for a DataNode to stores. Please partition the file into more pieces and try again.'.format(org_file_name + '.' + format))
            return False
            
        DataNode_to_insert = dataNode_to_insert(partitions_[i])
            
        if not DataNode_to_insert:
            if i == 0:
                print('Currently, partition size of {} is too large for a DataNode to stores. Please partition the file into more pieces and try again.'.format(org_file_name + '.' + format))
                return False
                
            print('Currently, some partitions of {} are too large to be stored. Please partition the file into smaller pieces'.format(org_file_name + '.' + format))
            put_success = False
            break
            
        partition_name = dir.replace('/', '_')[1:] + '_' + org_file_name + '_p' + str(i + 1)
            
        dataNodePut = requests.put(dataNodes_url + DataNode_to_insert + '/' + partition_name + '.json', json.dumps(partitions_[i]))
        nameNodePut = requests.put(nameNodes_url + '/p' + str(i + 1) + '.json', json.dumps(DataNode_to_insert + '/' + partition_name))

        
    # Remove the inserted file
    if not put_success:
        command_rm(dir + '/' + file_name)
        return False
    
    numPieces += len(df)
    existing_files.add(complete_filepath)
    return 'successfully put a file'
        
def command_make_dir(directory):
    global DataNodeSize, numDataNodes, numPieces
    
    if requests.get(base_url + '/root' + directory + '.json').json():
        print('Directory already exists')
        return
    
    r = requests.put(base_url + '/root' + directory + '.json', json.dumps(''))
        

def command_list(dir):
    global DataNodeSize, numDataNodes, numPieces
    
    url_ = base_url + '/root' + dir + '.json'
    name_item = requests.get(url_).json()
         
    if isinstance(name_item , str) and name_item == '':
        return 'Nothing in this directory'
            
    if name_item:
        curr_dir = []
        for name_ in name_item:
            curr_dir.append(name_)
        return curr_dir


def command_cat(file_path):
    global DataNodeSize, numDataNodes, numPieces
    
    file_path = file_path.replace('.', '_')
    partition_locations = command_getPartitionLocations(file_path)
        
    if not partition_locations:
        print('Input file does not exist')
        return
        
    df = pd.DataFrame()
        
    for i in range(len(partition_locations)):
        df_partition = command_readPartition(file_path, i + 1)
            
        for piecewise in df_partition:
            piecewise_df = pd.DataFrame(piecewise, index=[0])
            df = pd.concat([df, piecewise_df])
        
    df = df.reset_index(drop=True)
    return df

    
def command_rm(filepath):
    global DataNodeSize, numDataNodes, numPieces

    filepath = filepath.replace('.', '_')
    url_ = base_url + '/root' + filepath + '.json'
    filename = filepath.split('/')[-1]
        
    partition_locations = command_getPartitionLocations(filepath)
     
    if not partition_locations:
        print('The file you want to remove does not exist')
        return
        
    r = requests.delete(url_)
        
    # Recover the directory (which is empty and thus deleted) after deleting all files under it
    dir = filepath[:len(filepath) - len(filename) - 1]
        
    if not requests.get(base_url + dir + '.json').json():
        command_make_dir(dir)
        
    for partition_id in partition_locations:
        numPieces -= len(requests.get(base_url + '/DataNodes/' + partition_locations[partition_id] + '.json').json())
        r = requests.delete(base_url + '/DataNodes/' + partition_locations[partition_id] + '.json')
        dataNodeName, _ = partition_locations[partition_id].split('/')
            
        # Recover the dataNode (which is empty and thus deleted) after deleting all partitions under it
        if not requests.get(base_url + '/DataNodes/' + dataNodeName + '.json').json():
            restoreDataNode = requests.put(base_url + '/DataNodes/' + dataNodeName + '.json', json.dumps(''))
        
    return 'finished deleting'

def command_getPartitionLocations(file_path):
    global DataNodeSize, numDataNodes, numPieces
    
    file_path = file_path.replace('.', '_')
    partition_locations = requests.get(base_url + '/root' + file_path + '.json').json()
        
    if not partition_locations:
        return 'No such file is found'

        
    return partition_locations


def command_readPartition(file_path, partition_id):
    global DataNodeSize, numDataNodes, numPieces
    
    partition_locations = command_getPartitionLocations(file_path)
        
    if not partition_locations:
        return 'Such a partition does not exist'

        
    partition_location = partition_locations['p' + str(partition_id)]
    dataNode_url = base_url + '/DataNodes/' + str(partition_location) + '.json'
        
    return requests.get(dataNode_url).json()


#command_make_dir('/usr/Females/Marry')
#command_make_dir('/usr/Males')
#command_make_dir('/usr/Males/John')
#command_make_dir('/usr/Females/Lisa')
#command_make_dir('/usr/Females')
#command_make_dir('/usr/Males/John')

#command_put('winequality-red-v1.csv', '/usr/Males/John', 10)
#command_put('winequality-red-v1.csv', '/usr/Males/John', 10)
# command_put('winequality-red-v1.csv', '/usr/Males/John', 5)
#command_put('winequality-red-v1.csv', '/usr/Females/Marry', 5)
# print(command_getPartitionLocations('/usr/Males/John/winequality-red-v1.csv'))
# print(command_getPartitionLocations('/usr/Females/Marry/winequality-red-v1.csv'))

# p2 = command_readPartition('/usr/Males/John/winequality-red-v1.csv', 2)
# print(len(p2))

#command_list('/usr')
#command_list('/usr/Females')
#command_list('/usr/Females/Marry')
#command_list('/usr/Females/Lisa')
#command_list('/usr/Males')
#command_list('/usr/Males/John')
# command_rm('/user/tom/cars.csv')
# print(command_readPartition('/usr/Females/Yifan/winequality-red-v1.csv', 2))
# print(command_getPartitionLocations('/usr/Females/Yifan/winequality-red-v1.csv'))
