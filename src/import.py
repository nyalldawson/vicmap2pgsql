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

    folder = args.folder  # .lower()
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

    layers = []
    if dataset:
        dataset = os.path.basename(os.path.normpath(folder)).lower()
        shape_file = os.path.join(folder, 'layer', '{}.shp'.format(dataset))
        if os.path.isfile(shape_file):
            layers.append({'dataset': dataset, 'layer': shape_file})
        else:
            dbf_file = os.path.join(folder, 'table', '{}.dbf'.format(dataset))
            if os.path.isfile(dbf_file):
                layers.append({'dataset': dataset, 'layer': dbf_file})
    else:
        dataset = os.path.basename(os.path.normpath(folder)).lower()
        for s in glob.glob(os.path.join(folder, 'layer', '*.shp')):
            layers.append({'dataset': dataset, 'layer': s})
        for s in glob.glob(os.path.join(folder, 'table', '*.dbf')):
            layers.append({'dataset': dataset, 'layer': s})

    if len(layers) == 0:
        for d in [name for name in os.listdir(folder)
                  if os.path.isdir(os.path.join(folder, name))]:
            for s in glob.glob(os.path.join(folder, d, 'layer', '*.shp')):
                layers.append({'dataset': d.lower(), 'layer': s})
            for s in glob.glob(os.path.join(folder, d, 'table', '*.dbf')):
                layers.append({'dataset': d.lower(), 'layer': s})

    print "\nLayers to be processed are:"
    for l in layers:
        print '{}: {}'.format(l['dataset'], l['layer'])

    i = Importer(Database())
    i.recreate = recreate
    i.skip_shape_import = skip_shape_import
    i.setupDatabase()

    for idx, l in enumerate(layers):
        print "\n\nImporting {}/{}: {}\n-------------".format(idx + 1, len(layers), l['layer'])
        path, file = os.path.split(l['layer'])
        layer = file[:-4]

        i.importLayer(l['layer'], l['dataset'], layer)
