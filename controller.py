import argparse
import json
import os
import queue
import shutil
import threading
from osgeo import gdal

q = queue.Queue()
config = None

def loadConfig(configFile):
    config = {}
    with open(configFile) as config_data:
        config = json.load(config_data)
    return config

def peer(rasters, dryrun=False):
    mask = rasters['cropMask']
    mask_ds = gdal.Open(mask)
    mask_band = mask_ds.GetRasterBand(1)
    mask_none = mask_band.GetNoDataValue()
    mask_data = mask_band.ReadAsArray(0,0, mask_ds.RasterXSize, mask_ds.RasterYSize)
    mask_band = None
    mask_ds = None
    
    rasters_data = []
    rasters_none = []
    rasters_layer = list(rasters['dataLayers'].keys())
    peerless = []
    for raster in rasters['dataLayers'].values():
        ds = gdal.Open(raster)
        band = ds.GetRasterBand(1)
        rasters_none.append(band.GetNoDataValue())
        rasters_data.append(band.ReadAsArray(0,0,ds.RasterXSize, ds.RasterYSize))
        band = None
        ds = None

    for ridx, row in enumerate(mask_data):
        for cidx, col in enumerate(row):
            if col != mask_none:
                cell = readLayersByCells(ridx, cidx, rasters_data, rasters_none, rasters_layer)
                if cell:
                    peerless.append(cell)

    if (dryrun):
        for cell in peerless:
            print(cell)

    return peerless
                    
def readLayersByCells(row, col, raster_data, raster_none, raster_layer):
    cell = {'row': row, 'col': col} 
    for i, r in enumerate(raster_data):
        if r[row][col] == raster_none[i]:
            return None
        else:
            cell[raster_layer[i]] = r[row][col]
    return cell

def makeRunDirectory(wd):
    os.makedirs(wd)

def play(cell):
    cellID = "{}_{}".format(cell['row'], cell['col'])
    cellRunDirectory = os.path.join(config['workdir'], cellID)
    makeRunDirectory(cellRunDirectory)
    for soil in config['soils']:
        shutil.copy2(soil, cellRunDirectory)
    shutil.copy2(os.path.join(config['weatherDirectory'], "{}.WTH".format(cell['weatherFile'])), cellRunDirectory)

def child():
    print("Okay!")
    while True:
        toy = q.get()
        if toy is None:
            print("All done!")
            break
        play(toy)
        q.task_done()


def launcher(config, peerless):
    print("Okay children, go play")
    threads = []
    for i in range(8):
        t = threading.Thread(target=child)
        t.start()
        threads.append(t)
    for cell in peerless:
        q.put(cell)
    q.join()
    for i in range(8):
        q.put(None)
    for t in threads:
        t.join()

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description='Do something useful')
    parser.add_argument('config')
    parser.add_argument('--dry-run', action='store_true')
    args = parser.parse_args()
    config = loadConfig(args.config)
    peerless = peer(config['rasters'], args.dry_run)
    if not args.dry_run:
        makeRunDirectory(config['workdir'])
        launcher(config, peerless)
