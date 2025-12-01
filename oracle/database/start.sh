#!/bin/bash

V_DB_NAME=${ORACLE_DB_NAME:-"DBNAME"}
V_USER_PASSWORD=${ORACLE_PWD:-"mudar123"}
V_USER_ADM_NAME=${ORACLE_USER_ADM_NAME:-"${V_DB_NAME}_ADM"}
V_USER_UBR_NAME=${ORACLE_USER_UBR_NAME:-"${V_DB_NAME}UBR"}
V_DATA_TABLESPACE_NAME=${ORACLE_DATA_TABLESPACE_NAME:-"TSD${V_DB_NAME}01"}
V_INDEX_TABLESPACE_NAME=${ORACLE_INDEX_TABLESPACE_NAME:-"TSI${V_DB_NAME}01"}

echo "V_DB_NAME: ${V_DB_NAME^^}"
echo "V_USER_PASSWORD: ${V_USER_PASSWORD}"
echo "V_USER_ADM_NAME: ${V_USER_ADM_NAME^^}"
echo "V_USER_UBR_NAME: ${V_USER_UBR_NAME^^}"
echo "V_DATA_TABLESPACE_NAME: ${V_DATA_TABLESPACE_NAME^^}"
echo "V_INDEX_TABLESPACE_NAME: ${V_INDEX_TABLESPACE_NAME^^}"

### Arquivo temporário SQL
echo "define V_DB_NAME=${V_DB_NAME^^}" > temp.sql
echo "define V_USER_PASSWORD=${V_USER_PASSWORD}" >> temp.sql
echo "define V_USER_ADM_NAME=${V_USER_ADM_NAME^^}" >> temp.sql
echo "define V_USER_UBR_NAME=${V_USER_UBR_NAME^^}" >> temp.sql
echo "define V_DATA_TABLESPACE_NAME=${V_DATA_TABLESPACE_NAME^^}" >> temp.sql
echo "define V_INDEX_TABLESPACE_NAME=${V_INDEX_TABLESPACE_NAME^^}" >> temp.sql

# shellcheck disable=SC1044
# shellcheck disable=SC1073
cat <<EOL >> temp.sql
alter system set COMMON_USER_PREFIX = '' SCOPE=SPFILE;
alter session set "_ORACLE_SCRIPT"=true;

-- 1. Tablespaces
create tablespace $V_DATA_TABLESPACE_NAME
  datafile '/opt/oracle/oradata/tsd01.dbf'
  size 10m
  autoextend on next 1m;

create tablespace &V_INDEX_TABLESPACE_NAME
  datafile '/opt/oracle/oradata/tsi01.dbf'
  size 10m
  autoextend on next 1m;

-- 2. Profiles
create role RL_COMPLIANCE_NSA;

-- 3. Usuário ADM
create user &V_USER_ADM_NAME identified by &V_USER_PASSWORD default tablespace &V_DATA_TABLESPACE_NAME;

grant execute on ctxsys.ctx_ddl to &V_USER_ADM_NAME;

grant
  CTXAPP,
  CONNECT,
  RESOURCE,
  CREATE TABLE,
  DROP PUBLIC SYNONYM,
  EXEMPT REDACTION POLICY,
  ALTER SESSION,
  CREATE DATABASE LINK,
  CREATE TYPE,
  CREATE PUBLIC SYNONYM,
  CREATE VIEW,
  SELECT ANY DICTIONARY,
  CREATE SESSION,
  UNLIMITED TABLESPACE,
  CREATE PROCEDURE,
  CREATE SEQUENCE,
  DROP PUBLIC DATABASE LINK,
  CREATE SYNONYM,
  CREATE TRIGGER
  to &V_USER_ADM_NAME;

create user &V_USER_UBR_NAME identified by &V_USER_PASSWORD default tablespace &V_DATA_TABLESPACE_NAME;

grant
  EXEMPT REDACTION POLICY,
  CREATE SESSION,
  UNLIMITED TABLESPACE
  to &V_USER_UBR_NAME;

EOL

## Executa o SQL usando SQL*Plus
"$ORACLE_HOME"/bin/sqlplus -s "/ as sysdba" @temp.sql;

rm -rf temp.sql
