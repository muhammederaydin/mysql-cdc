# mysql-cdc 
This module is very basic CDC for mysql.

For using this module you should activate mysql general log and read general log file.

This module detect changes inside the log file and publish insert, update and delete queries 

For activating general log. Open my.cnf with your favorite editor and set

    general_log_file = /path/to/query.log
    general_log      = 1
    
and save.

You need general_log_file for reading log file. You should give as parameter to module.
