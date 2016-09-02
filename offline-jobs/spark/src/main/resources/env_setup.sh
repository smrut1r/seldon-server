#!/usr/bin/env bash
seldon-cli db --action setup --db-name ClientDB --db-user root --db-password mypass --db-jdbc 'jdbc:mysql://10.0.0.17:3306/?characterEncoding=utf8&useServerPrepStmts=true&logger=com.mysql.jdbc.log.StandardLogger&roundRobinLoadBalance=true&transformedBitIsBoolean=true&rewriteBatchedStatements=true'
seldon-cli db --action commit

seldon-cli memcached --action setup --numClients 4 --servers "10.0.0.80:11211,10.0.0.151:11211"
seldon-cli memcached --action commit