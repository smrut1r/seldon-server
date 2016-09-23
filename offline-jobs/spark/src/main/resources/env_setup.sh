#!/usr/bin/env bash
sudo apt install mysql-server
sudo apt install zookeeperd
sudo apt install memcached

seldon-cli db --action setup --db-name ClientDB --db-user root --db-password mypass --db-jdbc 'jdbc:mysql://10.0.0.133:3306/?characterEncoding=utf8&useServerPrepStmts=true&logger=com.mysql.jdbc.log.StandardLogger&roundRobinLoadBalance=true&transformedBitIsBoolean=true&rewriteBatchedStatements=true'
seldon-cli db --action commit

seldon-cli memcached --action setup --numClients 4 --servers "10.0.0.131:11211"
seldon-cli memcached --action commit