#!/bin/bash

echo "Creating Debezium user in MySQL..."

docker exec mysql_wsl mysql -u root -proot <<-EOSQL
    CREATE USER IF NOT EXISTS 'debezium'@'%' IDENTIFIED BY 'debezium_pass_123';
    GRANT SELECT, RELOAD, SHOW DATABASES, REPLICATION SLAVE, REPLICATION CLIENT ON *.* TO 'debezium'@'%';
    
    CREATE USER IF NOT EXISTS 'replication_writer'@'%' IDENTIFIED BY 'replication_pass_123';
    GRANT ALL PRIVILEGES ON \`client_%\`.* TO 'replication_writer'@'%';
    
    FLUSH PRIVILEGES;
    
    SELECT User, Host FROM mysql.user WHERE User IN ('debezium', 'replication_writer');
EOSQL

echo "✅ Debezium users created!"
