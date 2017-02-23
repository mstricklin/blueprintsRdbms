#!/usr/bin/env bash
# CLASSIFICATION NOTICE: This file is UNCLASSIFIED


java -cp ${HOME}/.m2/repository/org/hsqldb/hsqldb/2.3.3/hsqldb-2.3.3.jar org.hsqldb.util.DatabaseManagerSwing --urlid graph

#   --help                show this message
#   --driver <classname>  jdbc driver class
#   --url <name>          jdbc url
#   --user <name>         username used for connection
#   --password <password> password for this user
#   --urlid <urlid>       use url/user/password/driver in rc file
#   --rcfile <file>       (defaults to 'dbmanager.rc' in home dir)
#   --dir <path>          default directory
#   --script <file>       reads from script file
#   --noexit              do not call system.exit()

# --driver org.hsqldb.jdbc.JDBCDriver
# --url jdbc:hsqldb:file:/Users/boobear/working/blueprints-rdbms01/blueprintsRdbms/graph/graph


