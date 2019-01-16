import argparse
import datetime
import json
import os
import queue
import shutil
import string
import threading
from jinja2 import Environment, FileSystemLoader
from osgeo import gdal

q = queue.Queue()
config = None
jinjaEnv = None

def loadConfig(configFile):
    config = {}
    with open(configFile) as config_data:
        config = json.load(config_data)
    return config

def peer(rasters, dryrun=False):
    rasters_data = []
    rasters_none = []
    rasters_layer = list(rasters['dataLayers'].keys())
    rows = -1
    cols = -1
    peerless = []
    alignFail = False
    raster_transform = None
    for raster in rasters['dataLayers'].values():
        ds = gdal.Open(raster)
        if rows == -1 and cols == -1:
            rows = ds.RasterYSize
            cols = ds.RasterXSize
        if ds.RasterYSize != rows or ds.RasterXSize != cols:
            print("The raster {} is not aligned.".format(raster))
            alignFail = True
        if alignFail:
            return peerless
        if not raster_transform:
            raster_transform = ds.GetGeoTransform()
        band = ds.GetRasterBand(1)
        rasters_none.append(band.GetNoDataValue())
        rasters_data.append(band.ReadAsArray(0,0,ds.RasterXSize, ds.RasterYSize))
        band = None
        ds = None

    if 'cliDir' in config:
        for k,raster in rasters['cliLayers'].items():
            for month in range(1,13):
                rm = raster.format(month)
                ds = gdal.Open(r)
                if rows == -1 and cols == -1:
                    rows = ds.RasterYSize
                    cols = ds.RasterXSize
                if ds.RasterYSize != rows or ds.RasterXSize != cols:
                    print("The CLI raster {} is not aligned.".format(r))
                    print("Needed: [{},{}]\nFound:  [{},{}]".format(cols,rows,ds.RasterXSize,ds.RasterYSize))
                    alignFail = True
                if alignFail:
                    return peerless
                band = ds.GetRasterBand(1)
                rasters_layer.append("{}_{}".format(k,month))
                rasters_none.append(band.GetNoDataValue())
                rasters_data.append(band.ReadAsArray(0,0,ds.RasterXSize,ds.RasterYSize))
                band = None
                ds = None
        print("Layers: {}".format(rasters_layer))


    for ridx in range(rows):
        for cidx in range(cols):
            cell = readLayersByCells(ridx, cidx, rasters_data, rasters_none, rasters_layer, raster_transform)
            if cell:
                peerless.append(cell)

    if (dryrun):
        for cell in peerless:
            print(cell)
        print("Total possible cells: {}".format(cols*rows))
        print("Number of cells: {}".format(len(peerless)))
    return peerless

def readLayersByCells(row, col, raster_data, raster_none, raster_layer, raster_transform):
    xoff, pixelw, _, yoff, _, pixelh = raster_transform
    lng = col * pixelw + xoff + (pixelw/2)
    lat = row * pixelh + yoff + (pixelh/2)
    cell = {'row': row, 'col': col, 'lat': lat, 'lng': lng}
    for i, r in enumerate(raster_data):
        if r[row][col] == raster_none[i]:
            return None
        else:
            cell[raster_layer[i]] = r[row][col]
    return cell

def makeRunDirectory(wd):
    os.makedirs(wd, exist_ok=True)

def automaticPlantingWindow(cell):
    first = date(config['startYear'], cell['plantingMonth'], config['plantingDayOfMonth'])
    td = datetime.timedelta(days=config['plantingWindow'])
    last = start+td

def findSoilProfile(profile, soilFiles):
    profile = "*{}".format(profile)
    for sf in soilFiles:
        with open(sf) as f:
            for line in f:
                if line.startswith(profile):
                    return sf
    return None

def transpose(listOfLists):
    return list(map(list, zip(*listOfLists)))

def formatSoilData(header, current_data):
    transposed = transpose(current_data)
    return {k: v for k,v in zip(header, transposed)}

def readSoilLayers(profile, soilFile):
    profile = "*{}".format(profile)
    profilelines = []
    found = False
    with open(soilFile) as f:
        for line in f:
            line = line.strip()
            if line.startswith(profile):
                found = True
            if found and line == "":
                found = False
            if found:
                profilelines.append(line)
    in_data = False
    current_data = []
    header = []
    data = {}
    for line in profilelines:
        if line.startswith('@') and in_data:
            data.update(formatSoilData(header, current_data))
            header = []
            current_data = []
            in_data = False
        if line.startswith('@'):
            header = line[1:].split()
            if header[0] == 'SLB':
                in_data = True
            else:
                in_data = False
        else:
            if in_data:
                current_data.append(line.split())
    data.update(formatSoilData(header, current_data))
    return data

def calculateSoilThickness(slb):
    thick = []
    for i,v in enumerate(slb):
        if i == 0:
            thick.append(v)
        else:
            thick.append(v - slb[i-1])
    return thick

def calculateSoilMidpoint(slb):
    midpoint = []
    for i,v in enumerate(slb):
        if v < 40:
            midpoint.append(0.0)
        else:
            if i == 0:
                midpoint.append(0.0)
            elif slb[i-1] > 100:
                midpoint.append(0.0)
            else:
                midpoint.append((min(100,v)+max(40,slb[i-1]))/2)
    print(midpoint)
    return midpoint

def calculateTopFrac(slb, thickness):
    tf = []
    c = 0.0
    for i,v in enumerate(slb):
        if v < 40:
            c = 1.0
        else:
            c = 1-((v-40)/thickness[i])
        tf.append(max(0.0, c))
    return tf

def calculateBotFrac(slb, thickness):
    bf = []
    c = 0.0
    for i,v in enumerate(slb):
        if i != 0:
            if slb[i-1] > 100:
                c = 1.0
            else:
                c = (v-100)/(thickness[i])
        bf.append(max(0.0, c))
    return bf

def calculateMidFrac(tf, bf):
    return [1 - bf[i] - tf[i] for i in range(len(tf))]

def calculateDepthFactor(mp, tf, mf):
    maths = [tf[i]+(mf[i]*(1-(mp[i]-40)/60)) for i in range(len(mp))]
    return [max(0.05, m) for m in maths]

def calculateWeightingFactor(slbdm, thickness, df):
    return [slbdm[i] * thickness[i] * df[i] for i in range(len(slbdm))]

def calculateICNTOT(wf, n, twf):
    return [f*n/twf for f in wf]

def calculateNDist(icn, sbdm, thickness):
    return [icn[i]/sbdm[i]/thickness[i] for i in range(len(icn))]

def calculateH2O(fractionalAW, slll, sdul):
    h2o = []
    for i,ll in enumerate(slll):
        h2o.append((fractionalAW*(sdul[i]-ll))+ll)
    return h2o

def calculateICLayerData(soilData, n):
    slb = [int(v) for v in soilData['SLB']]
    sbdm = [float(v) for v in soilData['SBDM']]
    slll = [float(v) for v in soilData['SLLL']]
    sdul = [float(v) for v in soilData['SDUL']]

    thickness = calculateSoilThickness(slb)
    mp = calculateSoilMidpoint(slb)
    tf = calculateTopFrac(slb, thickness)
    bf = calculateBotFrac(slb, thickness)
    mf = calculateMidFrac(tf,bf)
    df = calculateDepthFactor(mp, tf, mf)
    wf = calculateWeightingFactor(sbdm, thickness, df)

    tsbdm = sum([thickness[i]*sbdm[i] for i in range(len(thickness))])
    twf = sum(wf)
    ictot = calculateICNTOT(wf, n, twf)
    icndist = calculateNDist(ictot, sbdm, thickness)

    return transpose([
            soilData['SLB'],
            calculateH2O(config['fractionalAW'], slll, sdul),
            [icnd * 10 * 0.1 for icnd in icndist],
            [icnd * 10 * 0.9 for icnd in icndist]
    ])

def genICBlock(cell, profile):
    soilFile = findSoilProfile(profile, config['soils'])
    layers = readSoilLayers(profile, soilFile)
    calculatedLayers = calculateICLayerData(layers, 5) #cell['initialNitrogen'])
    iclayers = ""
    layerBlock = """ 1  {}  {}  {}  {}
"""
    for l in calculatedLayers:
        lb = layerBlock.format("{:>4}".format(l[0]),
                "{:>4.1g}".format(l[1]),
                "{:>4.1g}".format(l[2]),
                "{:>4.1g}".format(l[3]))
        iclayers = iclayers + lb
    return """*INITIAL CONDITIONS
@C   PCR ICDAT  ICRT  ICND  ICRN  ICRE  ICWD ICRES ICREN ICREP ICRIP ICRID ICNAME
 1   {} {}  {}  {}  {}  {}  {} {} {} {} {} {}  {}
@C  ICBL  SH2O  SNH4  SNO3
""".format("{:>3}".format(-99),  #PCR
           "{:>5}".format(84001),    #ICDAT
           "{:>4}".format(5),        #ICRT
           "{:>4}".format(-99),      #ICND
           "{:>4}".format(-99),      #ICRN
           "{:>4}".format(-99),      #ICRE
           "{:>4}".format(-99),      #ICWD
           "{:>5}".format(10),       #ICRES
           "{:>5}".format(0.8),      #ICREN
           "{:>5}".format(-99),      #ICREP
           "{:>5}".format(-99),      #ICRIP
           "{:>5}".format(-99),      #ICRID
           "{:>5}".format(-99)) + iclayers  #ICNAME

def listCLIVar(cell, v):
    c = []
    for i in range(1,13):
        c.append(cell["{}_{}".format(v,i)])
    return c

def genCLIBlock(cell):
    tmin = listCLIVar(cell, 'tempMin')
    tmax = listCLIVar(cell, 'tempMax')
    srad = listCLIVar(cell, "solarRadiation")
    prec = listCLIVar(cell, 'rainAmount')
    rdays = listCLIVar(cell, 'rainyDays')

    avg_temp = sum([tmin[i] + tmax[i] for i in range(12)])/24
    max_temp = max(tmax)
    min_temp = min(tmin)
    total_prec = sum(prec)
    avg_tmin = sum(tmin)/12
    avg_tmax = sum(tmax)/12
    avg_srad = sum(srad)/12
    amp_temp = max_temp-min_temp

    monthly = "@MONTH  SAMN  XAMN  NAMN  RTOT  RNUM\n"
    monthlyBlock = "{}  {}  {}  {} {}  {}\n"
    for i in range(1,13):
        mb = monthlyBlock.format("{:>6d}".format(i),
                                 "{:>4.1f}".format(srad[i-1]),
                                 "{:>4.1f}".format(tmax[i-1]),
                                 "{:>4.1f}".format(tmin[i-1]),
                                 "{:>5.1f}".format(prec[i-1]),
                                 "{:>4.1f}".format(rdays[i-1]))
        monthly = monthly + mb

    return {
            'tav'  : "{:>4.1f}".format(avg_temp),
            'tamp' : "{:>4.1f}".format(amp_temp),
            'avgsr': "{:>4.1f}".format(avg_srad),
            'maxt' : "{:>4.1f}".format(max_temp),
            'mint' : "{:>4.1f}".format(min_temp),
            'tprec': "{:>4.1f}".format(total_prec),
            'monthly_averages' : monthly
            }

def genDates(cell):
    pdate_base = datetime.date(config['startYear'], cell['plantingMonth'], config['plantingDayOfMonth'])
    start_delta = datetime.timedelta(-30)
    pdlast_delta = datetime.timedelta(30)
    return {
            'pdate'   : "{:>5}".format(-99),
            'sdate'   : "{}{:>03}".format(str(config['startYear'])[2:], (pdate_base + start_delta).timetuple().tm_yday),
            'pdfirst' : "{}{:>03}".format(str(config['startYear'])[2:], pdate_base.timetuple().tm_yday),
            'pdlast'  : "{}{:>03}".format(str(config['startYear'])[2:], (pdate_base + pdlast_delta).timetuple().tm_yday)
           }

def to_julian_date(d):
    return d.strftime("%y%j")

def from_julian_date(s):
    return datetime.datetime.strptime(s, "%y%j").date()

def generate_INSI(prefix, c):
    for c1 in c:
        for c2 in c:
            yield prefix+c1+c2

def capture_header(f):
    header = []
    for idx, h in enumerate(open(f)):
        header.append(h)
        if idx == 4:
            break
    header[0] = header[0][:-1]+' {}'+'\n'
    header[3] = '  {}'+header[3][6:]
    return ''.join(header)

def capture_data(j_start_date, j_end_date, f):
    data = []
    started = False
    for d in open(f):
        if d[:5] == j_start_date:
            started = True
        if started:
            data.append(d)
            if d[:5] == j_end_date:
                break
    return data

def generate_base(start_year, cutoff_date, f):
    base = []
    started = False
    start_date = to_julian_date(datetime.date(start_year, 1, 1))
    cutoff_date = to_julian_date(cutoff_date)
    return ''.join(capture_data(start_date, cutoff_date, f))

def generate_forecast_data(target_year, cutoff_date, f):
    start_day = cutoff_date + datetime.timedelta(days=1)
    start_forecast = int(to_julian_date(start_day))
    start_date = to_julian_date(datetime.date(target_year,
                                     start_day.month,
                                     start_day.day))
    end_date = to_julian_date(datetime.date(target_year, 12, 31))
    data = capture_data(start_date, end_date, f)
    for idx, d in enumerate(data):
        data[idx] = '{:>05}'.format(start_forecast+idx) + d[5:]
    return ''.join(data)

def generate_forecast_file(header, base, cutoff_date, forecast_year, insi, f):
    h = header.format('{}F{}'.format(to_julian_date(cutoff_date),
                                     forecast_year),
                      insi)
    forecast = generate_forecast_data(forecast_year, cutoff_date, f)
    return h+base+forecast
def play(cell):
    template = jinjaEnv.get_template(config['xFileTemplate'])
    cellID = "{}_{}".format(cell['row'], cell['col'])
    cellRunDirectory = os.path.join(config['workDir'], cellID)
    makeRunDirectory(cellRunDirectory)
    for soil in config['soils']:
        shutil.copy2(soil, cellRunDirectory)
    if 'weatherDir' in config:
        if 'forecasting' in config:
            cutoff_dt = datetime.datetime.strptime(config['forecasting']['real_cutoff_date'], '%Y-%m-%d')
            cutoff_date = cutoff_dt.date()
            insi = generate_INSI('SS', string.ascii_uppercase)
            wth = os.path.join(config['weatherDir'], "{}.WTH".format(cell['weatherFile']))
            header = capture_header(wth)
            wsts = []
            base = generate_base(config['startYear'], cutoff_date, wth)
            for y in range(config['forecasting']['forecast_start_year'], cutoff_date.year):
                wst = next(insi)
                wsts.append(wst)
                with open(os.path.join(cellRunDirectory, "{}{}01.WTH".format(wst, str(cutoff_date.year)[2:])), 'w') as f:
                    f.write(generate_forecast_file(header, base, cutoff_date, y, wst, wth))
        else:
            shutil.copy2(os.path.join(config['weatherDir'], "{}.WTH".format(cell['weatherFile'])), os.path.join(cellRunDirectory, "SSUD.WTH"))
    soilProfile = "HC_GEN{:0>4}".format(cell['soilProfile'])
    xFileValues = dict(
        soil_id = soilProfile,
        ic_block = genICBlock(cell, soilProfile),
        run_years = "{:>5}".format(config['runYears'])
    )
    xFileValues.update(genDates(cell))
    xfile = template.render(xFileValues)
    with open(os.path.join(cellRunDirectory, config['xFileTemplate']), 'w') as f:
        f.write(xfile)
    if 'cliTemplate' in config:
        cliValues = genCLIBlock(cell)
        cliValues.update({
            'lat': "{:> 5.2f}".format(cell['lat']),
            'lng': "{:> 5.2f}".format(cell['lng']),
            'elev': "{:>4.1f}".format(cell['elevation'])
            })
        cliTemplate = jinjaEnv.get_template(config['cliTemplate'])
        cliFile = cliTemplate.render(cliValues)
        with open(os.path.join(cellRunDirectory,  'SSUD.CLI'), 'w') as f:
            f.write(cliFile)

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
    parser.add_argument('--test-new', action='store_true')
    args = parser.parse_args()
    config = loadConfig(args.config)
    if args.test_new:
        peerless = peer(config['rasters'], True)
        genCLIBlock(peerless[len(peerless)-1])
        pass
    else:
        peerless = peer(config['rasters'], args.dry_run)
        if not args.dry_run:
            jinjaEnv = Environment(loader=FileSystemLoader(config['templateDir']))
            makeRunDirectory(config['workDir'])
            launcher(config, peerless)
