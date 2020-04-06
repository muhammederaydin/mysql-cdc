##########
###MySQL
##########
host = "127.0.0.1"
port = 3306
user = "user"
password = "pwd"
db_name = "test"
general_log_dir = "/var/log/mysql/general.log"

##########
###Kafka
##########
bootstrap_servers = "127.0.0.1:9092"
topic = "MYSQL"
consumer_group_id = "mysql-cdc_01"

