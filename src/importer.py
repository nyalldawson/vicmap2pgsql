#!python

import subprocess
import json
import os


class Importer():
    """ Handles importing the source tables into the database destination """

    def __init__(self, db):
        self.db = db
        self.recreate = False
        self.skip_shape_import = False

        with open(os.path.join('datasets', 'column_mappings.json')) as mappings:
            self.columnMappings = json.load(mappings)

        with open(os.path.join('datasets', 'table_mappings.json')) as mappings:
            self.tableMappings = json.load(mappings)

    def setupDatabase(self):
        """ Sets up a database before starting the import, eg creating types, custom functions, etc """

        with open(os.path.join('sql', 'postgis', '01_create_datatypes.sql'), 'r') as f:
            sql = f.read()

        self.db.runSqlNoTransaction(sql)

    def importLayer(self, path, schema, table):
        """ Imports the specified layer """

        if not self.skip_shape_import:
            self.importLayerUsingOGR(path, 'public', schema, table)

        dest_schema, dest_table = self.destTable(schema, table)

        if not self.db.schemaExists(dest_schema):
            print "Existing schema {} does not exist".format(dest_schema)
            self.db.createSchema(dest_schema)

        if self.recreate:
            # Possibly should drop cascaded, but that's dangerous...
            self.db.dropTable(dest_schema, dest_table)

        append = self.shouldAppendTable(schema, table)
        if not self.db.tableExists(dest_schema, dest_table):
            print "Existing destination table {}.{} does not exist".format(dest_schema, dest_table)
            if self.createTableDefinition('public', table, dest_schema, dest_table):
                print "Created!"
        else:
            if not append:
                self.db.truncateTable(dest_schema, dest_table)
            else:
                print 'Append to existing table {}.{}'.format(dest_schema, dest_table)

        assert self.copyData('public', table, dest_schema,
                             dest_table), 'Could not copy data'

        self.db.vacuum(dest_schema, dest_table)

        count = self.db.recordCount(dest_schema, dest_table)
        print 'Copied {} records to destination table'.format(count)
        assert count > 0, 'No records exist in destination table!'

        # Drop temporary table
        self.db.dropTable('public', table)

        return True

    def destTable(self, schema, table):
        """ Returns destination schema and table for a given input table """
        matched_map = [m for m in self.tableMappings if m['dataset'].upper(
        ) == schema.upper() and m['table'].upper() == table.upper()]
        if not len(matched_map) == 1:
            return schema, table
        dest_schema = schema
        dest_table = table
        if 'dest_table' in matched_map[0].keys():
            dest_table = matched_map[0]['dest_table']
        if 'dest_schema' in matched_map[0].keys():
            dest_schema = matched_map[0]['dest_schema']

        return dest_schema, dest_table

    def shouldAppendTable(self, schema, table):
        """ Returns whether a table should be appended to an existing table """
        matched_map = [m for m in self.tableMappings if m['dataset'].upper(
        ) == schema.upper() and m['table'].upper() == table.upper()]
        if not len(matched_map) == 1:
            return False
        if 'append' in matched_map[0].keys() and matched_map[0]['append']:
            return True
        else:
            return False

    def isMulti(self, schema, table):
        """ Returns whether a table should have MULTI* geometry type """
        matched_map = [m for m in self.tableMappings if m['dataset'].upper(
        ) == schema.upper() and m['table'].upper() == table.upper()]
        if not len(matched_map) == 1:
            return False
        if 'force_multi' in matched_map[0].keys() and matched_map[0]['force_multi']:
            return True
        else:
            return False

    def addSerialId(self, schema, table):
        """ Returns whether a table should have a manually created serial primary key field, if so, returns the name
        of the desired ID column.
        """
        matched_map = [m for m in self.tableMappings if m['dataset'].upper(
        ) == schema.upper() and m['table'].upper() == table.upper()]
        if not len(matched_map) == 1:
            return None
        if 'create_serial_id' in matched_map[0].keys():
            return matched_map[0]['create_serial_id']
        else:
            return None

    def tablePrimaryKey(self, schema, table):
        """ Returns the manually set primary key for a table
        """
        matched_map = [m for m in self.tableMappings if m['dataset'].upper(
        ) == schema.upper() and m['table'].upper() == table.upper()]
        if not len(matched_map) == 1:
            return None
        if 'id' in matched_map[0].keys():
            return matched_map[0]['id']
        else:
            return None

    def importLayerUsingOGR(self, path, temp_schema, schema, table):
        """ Imports a given layer to PostGIS using OGR2OGR"""

        # Drop table if exists
        self.db.dropTable(temp_schema, table)

        ogr2ogr_args = [os.getenv('OGR2OGR', 'ogr2ogr'),
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

            # Determine whether file should be imported as multipolygons/lines
            if self.isMulti(schema, table):
                ogr2ogr_args.extend(['-nlt', 'PROMOTE_TO_MULTI'])

            # calculate CRS transform
            # reproject from GDA94 to Vicgrid
            ogr2ogr_args.extend(['-s_srs', 'epsg:4283', '-t_srs', 'epsg:3111'])

            ogr2ogr_args.extend(['-lco',
                                 'GEOMETRY_NAME=geom',  # geometry column is 'geom', not that rubbish the_geom default
                                 '-lco',
                                 # no spatial index for temporary table, it's
                                 # only temporary and we want fastest copy
                                 # possible
                                 'SPATIAL_INDEX=OFF',
                                 ])
        else:
            # dbf file
            print 'Uploading DBF to PostGIS...'
            # no extra arguments required

        ogr2ogr_args.extend([
            path,  # source table
            '-nln',
            '{}.{}'.format(temp_schema, table)]
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
        geom_col = None
        table_primary_key = self.tablePrimaryKey(dest_schema, dest_table)
        for i, c in enumerate(self.db.getTableColumnDefs(temp_schema, temp_table)):
            extra_defs = ''

            if c['name'] in ('ogc_fid'):
                # skip column
                continue
            if c['name'] == 'ufi':
                ufi_index = i - 1

            if c['name'] == 'geom':
                dest_columns.append(['geom', self.geometryColumnDefinition(
                    temp_schema, temp_table, dest_schema, dest_table), ''])
                geom_col = 'geom'
                continue

            matched_map = self.getMappedColumnDef(
                dest_schema, dest_table, c['name'])
            assert matched_map, "could not match: {}".format(c)

            if table_primary_key and c['name'].upper() == table_primary_key.upper():
                pk_index = i - 1
                min_pk_priority = 0
            elif 'primary_key_priority' in matched_map:
                current_pk_priority = matched_map['primary_key_priority']
                if current_pk_priority < min_pk_priority:
                    min_pk_priority = current_pk_priority
                    pk_index = i - 1

            dest_columns.append(
                [matched_map['column_name'], matched_map['data_type'], extra_defs])

        create_serial_id = self.addSerialId(dest_schema, dest_table)
        if create_serial_id:
            dest_columns.insert(0, [create_serial_id, 'serial', ''])
            pk_index = 0

        assert pk_index > - \
            1, "Could not determine primary key for {}".format(dest_table)
        dest_columns[pk_index][2] += ' PRIMARY KEY'

        if ufi_index > -1:
            # move ufi to start of list
            dest_columns.insert(0, dest_columns.pop(ufi_index))

        assert self.db.createTable(
            dest_schema, dest_table, dest_columns), "Could not create table {}.{}".format(dest_schema, dest_table)

        if geom_col:
            # Add spatial index
            self.db.createSpatialIndex(dest_schema, dest_table, geom_col)

    def geometryColumnDefinition(self, temp_schema, temp_table, dest_schema, dest_table):
        """ Calculates the definition for a layer's geometry column """

        # Get definition of existing geometry column
        return self.db.getGeometryColumnDef(temp_schema, temp_table, 'geom')

    def copyData(self, temp_schema, temp_table, dest_schema, dest_table):
        """ Copies the data from the temporary import table to the destination table, applying transforms as required """

        source_cols = []
        dest_cols = []

        for c in self.db.getTableColumnDefs(temp_schema, temp_table):
            if c['name'] == 'geom':
                source_cols.append('geom')
                dest_cols.append('geom')
                continue

            matched_map = self.getMappedColumnDef(
                dest_schema, dest_table, c['name'])
            if not matched_map:
                # discard column
                continue

            # does mapped column exist in destination?
            if not self.db.tableHasColumn(dest_schema, dest_table, matched_map['column_name']):
                # column not in destination table, ignore
                continue

            if 'transform' in matched_map.keys():
                transform = matched_map['transform']
            else:
                transform = '"{}"::{}'.format(
                    c['name'], matched_map['data_type'])
            source_cols.append(transform)
            dest_cols.append(matched_map['column_name'])

        print 'Copying data to destination table'
        return self.db.copyData(temp_schema, temp_table, source_cols, dest_schema, dest_table, dest_cols)
