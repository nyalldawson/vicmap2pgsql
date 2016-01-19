#!python

import subprocess
import json
import os


class Importer():
    """ Handles importing the source tables into the database destination """

    def __init__(self, db):
        self.db = db

        with open('datasets\\column_mappings.json') as mappings:
            self.columnMappings = json.load(mappings)

    def importLayer(self, path, schema, table):
        """ Imports the specified layer """

        self.importLayerUsingOGR(path, 'public', table)

        if not self.db.schemaExists(schema):
            print "Existing schema {} does not exist".format(schema)
            self.db.createSchema(schema)

        if not self.db.tableExists(schema, table):
            print "Existing destination table {}.{} does not exist".format(schema, table)
            if self.createTableDefinition('public', table, schema, table):
                print "Created!"
        else:
            self.db.truncateTable(schema, table)

        assert self.copyData('public', table, schema,
                             table), 'Could not copy data'

        count = self.db.recordCount(schema, table)
        print 'Copied {} records to destination table'.format(count)
        assert count > 0, 'No records exist in destination table!'

        # Drop temporary table
        self.db.dropTable('public', table)

        return True

    def importLayerUsingOGR(self, path, schema, table):
        """ Imports a given layer to PostGIS using OGR2OGR"""

        # Drop table if exists
        self.db.dropTable(schema, table)

        ogr2ogr_args = [os.getenv('OGR2OGR', 'C:\\OSGeo4W64\\bin\\ogr2ogr.exe'),
                        '--config',
                        'PG_USE_COPY YES',  # faster copies
                        '-skipfailures',  # ignore encoding errors
                        '-progress',  # report progress
                        '-f',
                        'PostgreSQL',  # output format PostgreSQL
                        'PG:{}'.format(self.db.ogrString())]  # PG db details

        # Work out if table is a shapefile or just a database table
        # do this by checking for a .shp file
        print 'Importing from {}'.format(path)
        if path[-3:] == 'shp':
            print 'Uploading shapefile to PostGIS...'
        else:
            # dbf file
            print 'Uploading DBF to PostGIS...'
            # no extra arguments required

        ogr2ogr_args.extend([
            path,  # source table
            '-nln',
            '{}.{}'.format(schema, table)]
        )

        subprocess.call(ogr2ogr_args)  # run OGR2OGR import

        return True

    def getMappedColumnDef(self, schema, table, column_name):
        """ Maps a source column definition to a target column definition """

        matched_map = [m for m in self.columnMappings if m[
            'column_name_10'].upper() == column_name.upper()]

        matched_table_overrides = []
        no_table_overrides = []
        for m in matched_map:
            if 'table_names' in m.keys():
                if table in m['table_names']:
                    matched_table_overrides.append(m)
            else:
                no_table_overrides.append(m)

        if matched_table_overrides:
            if len(matched_table_overrides) == 1:
                return matched_table_overrides[0]
            else:
                # Multiple matches
                return None
        else:
            if len(no_table_overrides) == 1:
                return no_table_overrides[0]
            else:
                return None

    def createTableDefinition(self, temp_schema, temp_table, dest_schema, dest_table):
        """ Creates an empty table definition """

        # get definition of temporary table
        dest_columns = []
        ufi_index = -1
        pk_index = -1
        min_pk_priority = 999
        for i, c in enumerate(self.db.getTableColumnDefs(temp_schema, temp_table)):
            extra_defs = ''

            if c['name'] in ('ogc_fid'):
                # skip column
                continue
            if c['name'] == 'ufi':
                ufi_index = i - 1

            matched_map = self.getMappedColumnDef(
                dest_schema, dest_table, c['name'])
            if not matched_map:
                print "could not match: {}".format(c)
                return False

            if 'primary_key_priority' in matched_map:
                current_pk_priority = matched_map['primary_key_priority']
                if current_pk_priority < min_pk_priority:
                    min_pk_priority = current_pk_priority
                    pk_index = i - 1

            dest_columns.append(
                [matched_map['column_name'], matched_map['data_type'], extra_defs])

        assert pk_index > - \
            1, "Could not determine primary key for {}".format(dest_table)
        dest_columns[pk_index][2] += ' PRIMARY KEY'

        if ufi_index > -1:
            # move ufi to start of list
            dest_columns.insert(0, dest_columns.pop(ufi_index))

        return self.db.createTable(dest_schema, dest_table, dest_columns)

    def copyData(self, temp_schema, temp_table, dest_schema, dest_table):
        """ Copies the data from the temporary import table to the destination table, applying transforms as required """

        source_cols = []
        dest_cols = []

        for c in self.db.getTableColumnDefs(temp_schema, temp_table):
            matched_map = self.getMappedColumnDef(
                dest_schema, dest_table, c['name'])
            if not matched_map:
                # discard column
                continue

            # does mapped column exist in destination?
            if not self.db.tableHasColumn(dest_schema, dest_table, matched_map['column_name']):
                # column not in destination table, ignore
                continue

            source_cols.append('"{}"::{}'.format(
                c['name'], matched_map['data_type']))
            dest_cols.append(matched_map['column_name'])

        return self.db.copyData(temp_schema, temp_table, source_cols, dest_schema, dest_table, dest_cols)
