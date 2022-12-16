# Firebase-based emulation database & SQL-like search and analysis web #
1. Download this project

    You can clone this project by running this command
  
    ```shell
    git clone https://github.com/Zhuo-Chen-byte/Firebase-HDFS-SQL.git
    ```
    
    or download the .zip manually
    
2. Create your Firebase Realtime Database
    
    And in *command_lines.py*, at line 6 change *base_url = 'https://dsci-551-group-project-default-rtdb.firebaseio.com'* to the link of yours

3. Generate a web
    You can start up the command lines, navigation, and search & analysis web by running
    
    ```shell
    cd Firebase-HDFS-SQL
    streamlit run EDFS_Commands.py
    ```
    
    and you will be directed to the webpage
