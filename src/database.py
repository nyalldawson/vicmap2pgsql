#!python

import os
import psycopg2


class Database():
    """ Handles interaction with the Postgres database """

    def __init__(self):
        self.port = 5432
        self.host = 'localhost'
        self.database = 'postgis'
        self.user = 'postgis'
        self.password = 'password'
        self.getParamsFromEnv()
        self.c = self.createConnection()

    def __del__(self):
        try:
            self.closeConnection()
        except:
            pass

    def __enter__(self):
        return self

    def __exit__(self, type, value, tb):
        self.closeConnection()

    def getParamsFromEnv(self):
        """Extracts connection parameters from the environment"""
        self.port = os.getenv('PGPORT', self.port)
        self.host = os.getenv('PGHOST', self.host)
        self.database = os.getenv('PGDATABASE', self.database)
        self.user = os.getenv('PGUSER', self.user)
        self.password = os.getenv('PGPASSWORD', self.password)

    def createConnection(self):
        """Initiates a connection to the PostGIS server"""
        conn_string = "host='{}' dbname='{}' user='{}' password='{}' port={}".format(
            self.host, self.database, self.user, self.password, self.port)
        return psycopg2.connect(conn_string)

    def encodeTableName(self, schema, table):
        """Encodes a table name to a safe string to use in a query"""
        return '"{}"."{}"'.format(schema, table)

    def encodeSchemaName(self, schema):
        """Encodes a schema name to a safe string to use in a query"""
        return '"{}"'.format(schema)

    def fetchSqlRecords(self, sql):
        """Executes a SQL query and returns the result rows"""
        cursor = self.c.cursor()
        cursor.execute(sql)
        r = cursor.fetchall()
        cursor.close()
        return r

    def runSql(self, sql):
        """Executes a SQL query"""
        cursor = self.c.cursor()
        cursor.execute(sql)
        self.c.commit()
        cursor.close()
        return True

    def runSqlNoTransaction(self, sql):
        """Executes a SQL query outside of a transaction block"""
        self.c.autocommit = True
        cursor = self.c.cursor()
        cursor.execute(sql)
        self.c.commit()
        cursor.close()
        self.c.autocommit = False
        return True

    def tableExists(self, schema, table):
        """Tests whether the specified table exists in the database"""
        r = self.fetchSqlRecords(
            "SELECT to_regclass('{}.{}')".format(schema, table))
        return r[0][0]

    def tableHasColumn(self, schema, table, column):
        """Tests whether a table has a specified column"""
        res = self.fetchSqlRecords(
            "select count(*) from information_schema.columns c where c.table_schema = '{}' and c.table_name='{}' and c.column_name='{}'".format(schema, table, column))
        return res[0][0] > 0

    def createTable(self, schema, table, cols):
        """Creates a new table in the database, with specified columns.
           param cols is an array of [name, definition, extra defs (eg PRIMARY KEY)]
        """
        col_definition = ','.join(
            ['"{}" {} {}'.format(c[0], c[1], c[2]) for c in cols])
        return self.runSql('CREATE TABLE {} ({})'.format(self.encodeTableName(schema, table), col_definition))

    def dropTable(self, schema, table):
        """ Drops a table from the database """
        return self.runSql('DROP TABLE IF EXISTS {}'.format(self.encodeTableName(schema, table)))

    def truncateTable(self, schema, table):
        """ Truncates a table from the database """
        return self.runSql('TRUNCATE TABLE {}'.format(self.encodeTableName(schema, table)))

    def getTableColumnDefs(self, schema, table):
        """ Gets the column definitions for the specified table """
        src_columns = self.fetchSqlRecords(
            "select c.column_name, data_type, character_maximum_length, numeric_precision, numeric_scale from information_schema.columns c where c.table_schema = '{}' and c.table_name='{}'".format(schema, table))
        return [dict(zip(('name', 'type', 'max_length', 'precision', 'scale'), c)) for c in src_columns]

    def recordCount(self, schema, table):
        """ Returns the number of rows in a table """
        r = self.fetchSqlRecords(
            "SELECT count(*) FROM {}".format(self.encodeTableName(schema, table)))
        return r[0][0]

    def copyData(self, src_schema, src_table, src_columns, dest_schema, dest_table, dest_columns):
        """Copies data from one table to another"""
        sql = 'INSERT INTO {} ( {} ) SELECT {} FROM {}'.format(self.encodeTableName(dest_schema, dest_table), ','.join(dest_columns),
                                                               ','.join(src_columns), self.encodeTableName(src_schema, src_table))
        return self.runSql(sql)

    def schemaExists(self, schema):
        """Tests whether the specified schema exists in the database"""
        r = self.fetchSqlRecords(
            "SELECT count(*) FROM information_schema.schemata WHERE schema_name = '{}'".format(schema))
        return r[0][0] > 0

    def createSchema(self, schema):
        """Creates a schema"""
        return self.runSql('CREATE SCHEMA IF NOT EXISTS {}'.format(self.encodeSchemaName(schema)))

    def closeConnection(self):
        self.c.close()

    def ogrString(self):
        """Returns an OGR format string which can be used to connect to the database"""
        return 'host={} user={} port={} dbname={} password={}'.format(self.host, self.user, self.port, self.database, self.password)
