blackbird-mysql
===============

[![Build Status](https://travis-ci.org/Vagrants/blackbird-mysql.png?branch=development)](https://travis-ci.org/Vagrants/blackbird-mysql)

Get mysql information.

MySQL setting
-------------

Please create monitor user like this.

```
mysql> GRANT SELECT, REPLICATION CLIENT, SHOW DATABASES, PROCESS ON *.* TO  'bbd'@'127.0.0.1' IDENTIFIED BY 'bbd';
```

Installation
------------

```
python setup.py install
```

or using rpm package from Blackbird yum repository.

Configuration
-------------

see [mysql.cfg](https://github.com/Vagrants/blackbird-mysql/blob/master/mysql.cfg) in this repository.

Components
----------

This plugin provides some components.  
Each component corresponds to each template.

| component | content | zabbix template |
|-----------|---------|-----------------|
| (default) | get mysql version and check mysql health| _MySQL_5.5_general.xml |
| global_variables | get information by `SHOW GLOBAL VARIABLES`| _MySQL_5.5_variables.xml |
| global_status | get information by `SHOW GLOBAL STATUS` | _MySQL_5.5_status.xml |
| innodb_status | get information by `SHOW ENGINE INNODB STATUS` | _MySQL_5.5_innodb.xml |
| slave_status | get information by `SHOW SLAVE STATUS` | _MySQL_5.5_slave.xml |
| table_count | discovery number of tables in database | _MySQL_5.5_general.xml |

You can select component for mysql monitoring. Set `components = ` in your mysql.cfg.  
