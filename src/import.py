#!python

from database import Database
from importer import Importer
import os
import argparse
import glob


# Find directory from command arguments
if __name__ == "__main__":

    parser = argparse.ArgumentParser(
        description='Importer for VicMap datasets to PostGIS')
    parser.add_argument(
        'folder', help='Name of subfolder or VicMap dataset, eg VMADMIN')
    parser.add_argument(
        'dataset', nargs='?', help='Optional specific dataset from subfolder, eg lga_polygon. If omitted whole folder will be imported.')
    parser.add_argument('--recreate', action='store_true', default=False,
                        help='Forces dropping any existing tables and recreating. Be careful!')
    parser.add_argument('--skipshpimport', action='store_true', default=False,
                        help='Skips the initial shp file import to a temporary table in the public schema. For debugging only.')
    args = parser.parse_args()

    folder = args.folder.lower()
    if args.dataset:
        dataset = args.dataset.lower()
    else:
        dataset = None
    recreate = args.recreate
    skip_shape_import = args.skipshpimport
    print "Importing from {}".format(folder.upper())
    if dataset:
        print "  Dataset: {}".format(dataset.upper())
    if recreate:
        print "  Existing table definitions will be removed!"

    datasets = []
    if dataset:
        shape_file = os.path.join(folder, 'layer', '{}.shp'.format(dataset))
        if os.path.isfile(shape_file):
            datasets.append(shape_file)
        else:
            dbf_file = os.path.join(folder, 'table', '{}.dbf'.format(dataset))
            if os.path.isfile(dbf_file):
                datasets.append(dbf_file)
    else:
        for s in glob.glob(os.path.join(folder, 'layer', '*.shp')):
            datasets.append(s)
        for s in glob.glob(os.path.join(folder, 'table', '*.dbf')):
            datasets.append(s)

    print "\nDatasets to be processed are:"
    for d in datasets:
        print d

    i = Importer(Database())
    i.recreate = recreate
    i.skip_shape_import = skip_shape_import

    for idx, d in enumerate(datasets):
        print "\n\nImporting {}/{}: {}\n-------------".format(idx + 1, len(datasets), d)
        path, file = os.path.split(d)
        layer = file[:-4]

        i.importLayer(d, os.path.basename(os.path.normpath(folder)), layer)
