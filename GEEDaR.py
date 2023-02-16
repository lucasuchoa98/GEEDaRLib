# -*- coding: utf-8 -*-
"""
-=|| Google Earth Engine Data Retriever (GEEDaR) ||=-
This script is intended to retrieve data from Google Earth Engine.
Based on the provided list of sites and dates and on the chosen processing algorithms, it retrieves the corresponding satellite data.
Supported products include those from MODIS, VIIRS, Landsat, Sentinel-2 and Sentinel-3.

Created on Python 3.7
@author: Dhalton.Ventura
"""
#%% Modules

import sys
import os
import math
from time import sleep
import sqlite3
import pandas as pd
from shutil import copyfile
from fastkml import kml
import ee
ee.Initialize()

#%% Definitions

myVersion = "0.64.0"
print("\n.* Google Earth Engine Data Retriever (GEEDaR) - version " + myVersion + " *.\n")

# Default parameters for running the script:
running_modes = [
    "1 (specific dates)", 
    "2 (date ranges)", 
    "3 (database update)", 
    "4 (database overwrite)", 
    "5 (estimation overwrite)"
    ]

running_mode = ""

"""
processing_codes, product_ids, img_proc_algos, estimation_algos, reducers = (
    [10110001,10210001,30109001,30209001,30309001,20109001], 
    [101,102,301,302,303,201], 
    [10,10,9,9,9,9], 
    [0]*6, [1]*6
    )
"""

processing_codes = [10110001,10210001,30109001,30209001,30309001,20109001]
product_ids = [101,102,301,302,303,201]
img_proc_algos = [10,10,9,9,9,9]
estimation_algos = [0]*6
reducers = [1]*6

nProcCodes = len(processing_codes)
aoi_modes = ["radius", "kml"]
aoi_mode = aoi_modes[0]
aoi_radius = 250
time_window = 0
append_mode = False
input_path = ""
output_path = ""
max_n_proc_pixels = 25000
run_par = {
    # Help
    "h": "",
    # Full path for the input CSV (single-use mode) or JSON file (monitoring mode).
    "i": input_path,
    # Full path for the output JSON or CSV file.
    "o": output_path,
    # Running mode.
    "m": running_mode,
    # Processing code.
    "c": str(processing_codes),
    # Define the area of interest (AOI) by reading KML files in the same folder from the input CSV.
    # If -k is included among the arguments, aoi_mode is set to "kml", unless -r is also included.
    "k": "",
    # AOI radius (the radius, in meters, around the given coordinates, defining the region of interest (AOI)).
    # If it is provided, aoi_mode is assumed to be "radius", even if -k is included in the command line arguments. 
    "r": "",
    # Time window (number of days, before and after each specified date, to include in the data retrieval).
    "t": str(time_window),
    # If in append_mode, the results are concatenated by replicating the input data frame vertically.
    "a": ""
}

# Global objects used among functions.
image_collection = ee.ImageCollection(ee.Image())
aoi = None
ee_reducer = ee.Reducer.median()
bands = {}
input_df = pd.DataFrame()
user_df = pd.DataFrame()
export_vars = []
export_bands = []
log_file = "GEEDaR_log.txt"
anyError = False

from product_specs import product_specs

# List of GEEDar products' IDs:
available_products = [*product_specs]

# Image processing (atmospheric correction and unwanted pixels' exclusion) algorithms:
img_proc_algo_specs = {
    0: {
        "name": "None",
        "description": "This algorithm makes no change to the image data.",
        "ref": "",
        "nSimImgs": 500,
        "applicableTo": available_products
    },
    1: {
        "name": "StdCloudMask",
        "description": "This algorithm removes pixels with cloud, cloud shadow or high aerosol, based on the product's pixel quality layer. It works better for Modis and Landsat.",
        "ref": "",
        "nSimImgs": 500, # confirm it!
        "applicableTo": [101,102,105,106,107,111,112,115,116,117,151,152,201,202,301,302,303,311,312,313,314,315]
    },
    2: {
        "name": "MOD3R",
        "description": "This algorithm replicates, to the possible extent, the MOD3R algorithm, developed by researchers from the IRD French institute.",
        "ref": "Espinoza-Villar, R. 2013. Suivi de la dynamique spatiale et temporelle des flux se´dimentaires dans le bassin de l’Amazone a` partir d’images satellite. PhD thesis, Université Toulouse III - Paul Sabatier, Toulouse, France.",
        "nSimImgs": 40,
        "applicableTo": [101,102,105,106,107,111,112,115,116,117,151,152]
    },
    3: {
        "name": "MOD3R_minNDVI",
        "description": "It is a modification of the MOD3R algorithm, defining as the water-representative cluster the one with the lowest NDVI.",
        "ref": "VENTURA, D.L.T. 2020. Unpublished.",
        "nSimImgs": 60,
        "applicableTo": [101,102,105,106,107,111,112,115,116,117,151,152]
    },
    4: {
        "name": "MOD3R_minIR",
        "description": "It is a modification of the MOD3R algorithm, defining as the water-representative cluster the one with the lowest reflectance in the near infrared.",
        "ref": "VENTURA, D.L.T. 2020. Unpublished.",
        "nSimImgs": 60,
        "applicableTo": [101,102,105,106,107,111,112,115,116,117,151,152]
    },
    5: {
        "name": "Ventura2018",
        "description": "It is simply a threshold (400) in the near infrared.",
        "ref": "VENTURA, D.L.T. 2018. Water quality and temporal dynamics of the phytoplankton biomass in man-made lakes of the Brazilian semiarid region: an optical approach. Thesis. University of Brasilia.",
        "nSimImgs": 500, # test it!
        "applicableTo": [*range(100, 120)] + [151,152]
    },
    6: {
        "name": "S2WP_v6",
        "description": "Selects, on a Sentinel-2 L2A image, the water pixels not affected by cloud, cirrus, shadow, sunglint and adjacency effects. It selects both 'bright' and 'dark' water pixels. The latter may incorrectly include shaded water pixels.",
        "ref": "VENTURA, D.L.T. 2020. Unpublished.",
        "nSimImgs": 150,
        "applicableTo": [201]
    },
    7: {
        "name": "S2WP_Bright_v6",
        "description": "Selects, on a Sentinel-2 L2A image, the 'bright' water pixels (which includes most types of water) not affected by cloud, cirrus, shadow, sunglint and adjacency effects. 'Dark' water pixels, which may me mixed with shaded water pixels, are excluded.",
        "ref": "VENTURA, D.L.T. 2020. Unpublished.",
        "nSimImgs": 150,
        "applicableTo": [201]
    },
    8: {
        "name": "S2WP_Dark_v6",
        "description": "Selects, on a Sentinel-2 L2A image, the 'dark' water pixels (such as waters rich in dissolved organic compounds) not affected by cloud, cirrus, sunglint and adjacency effects. 'Dark' water pixels may me mixed with shaded water pixels.",
        "ref": "VENTURA, D.L.T. 2020. Unpublished.",
        "nSimImgs": 150,
        "applicableTo": [201]
    },
    9: {
        "name": "S2WP_v7",
        "description": "Selects, on an atmospherically corrected Sentinel-2 or Landsat image, the water pixels not affected by cloud, cirrus and sunglint, as well as pixels not strongly affected by shadow, aerosol and adjacency effects.",
        "ref": "VENTURA, D.L.T. 2020. Unpublished.",
        "nSimImgs": 120,
        "applicableTo": [201,301,302,303,311,312,313,314,315,101,102,105,106,107,111,112,115,116,117,151,152]
    },
    10: {
        "name": "S2WP_v7_MODIS",
        "description": "Selects, on an atmospherically corrected Modis image, the water pixels not affected by cloud, cirrus and sunglint, as well as pixels not strongly affected by shadow, aerosol and adjacency effects.",
        "ref": "VENTURA, D.L.T. 2020. Unpublished.",
        "nSimImgs": 150,
        "applicableTo": [201,301,302,303,311,312,313,314,315,101,102,105,106,107,111,112,115,116,117,151,152]
    },
    11: {
        "name": "RICO",
        "description": "For products with only the red and NIR bands, selects water pixels. Not appropriate for eutrophic conditions or for extreme inorganic turbidity.",
        "ref": "VENTURA, D.L.T. 2021. Unpublished.",
        "nSimImgs": 30,
        "applicableTo": [101,102,103,104,105,106,107,111,112,113,114,115,116,117,151,152,201,202,301,302,303,311,312,313,314,315]
    },
    12: {
        "name": "S2WP_v8",
        "description": "Selects, on an atmospherically corrected image, the water pixels unaffected by cloud, cirrus, sunglint, aerosol, shadow and adjacency effects.",
        "ref": "VENTURA, D.L.T. 2021. Unpublished.",
        "nSimImgs": 120,
        "applicableTo": [201,301,302,303,311,312,313,314,315,101,102,105,106,107,111,112,115,116,117,151,152]
    },
    13: {
        "name": "minNDVI + Wang2016",
        "description": "Selects the pixel cluster with the lowest NDVI and reduces reflectance noise by subtracting the minimum value in the NIR-SWIR range from all bands, excluding pixels with high NIR or SWIR.",
        "ref": "WANG, S. et al. 2016. A simple correction method for the MODIS surface reflectance product over typical inland waters in China. Int. J. Remote Sens. 37 (24), 6076–6096.",
        "nSimImgs": 30,
        "applicableTo": [101,102,105,106,107,111,112,115,116,117,151,152]
    },
    14: {
        "name": "GPM daily precipitation",
        "description": "Average the calibrated precipitation in 24 hours inside the area of interest.",
        "ref": "VENTURA, D.L.T. 2021. Unpublished.",
        "nSimImgs": 48,
        "applicableTo": [901]
    }
}
img_proc_algo_list = [*img_proc_algo_specs]

# Algorithms for parameter estimation from spectral data:
estimation_algo_specs = {
    0: {
        "name": "None",
        "description": "This algorithm makes no calculations and no changes to the images.",
        "model": "",
        "ref": "",
        "paramName": [""],
        "requiredBands": []
    },
    1: {
        "name": "Former HidroSat chla",
        "description": "Estima a concentração de clorofila (ug/L) em açudes do Semiárido.",
        "model": "4.3957 + 0.213*(R - R^2/G) + 0.0004*(R - R^2/G)^2",
        "ref": "",
        "paramName": ["chla_surf"],
        "requiredBands": ["red", "green"]
    },
    2: {
        "name": "SSS Solimões",
        "description": "Estimates the surface suspended solids concentration in the Solimões River.",
        "model": "759.12*(NIR/red)^1.9189",
        "ref": "Villar, R.E.; Martinez, J.M; Armijos, E.; Espinoza, J.C.; Filizola, N.; Dos Santos, A.; Willems, B.; Fraizy, P.; Santini, W.; Vauchel, P. Spatio-temporal monitoring of suspended sediments in the Solimoes River (2000-2014). Comptes Rendus Geoscience, v. 350, n. 1-2, p. 4-12, 2018.",
        "paramName": ["SS_surf"],
        "requiredBands": ["red", "NIR"]
    },
    3: {
        "name": "SSS Madeira",
        "description": "Estimates the surface suspended solids concentration in the Madeira River.",
        "model": "1020*(NIR/red)^2.94",
        "ref": "Villar, R.E.; Martinez, J.M.; Le Texier, M.; Guyot, J.L.; Fraizy, P.; Meneses, P.R.; Oliveira, E. A study of sediment transport in the Madeira River, Brazil, using MODIS remote-sensing images. Journal of South American Earth Sciences, v. 44, p. 45-54, 2013.",
        "paramName": ["SS_surf"],
        "requiredBands": ["red", "NIR"]
    },
    4: {
        "name": "SSS Óbidos",
        "description": "Estimates the surface suspended solids concentration in the Amazon River, near Óbidos.",
        "model": "0.2019*NIR - 14.222",
        "ref": "Martinez, J. M.; Guyot, J.L.; Filizola, N.; Sondag, F. Increase in suspended sediment discharge of the Amazon River assessed by monitoring network and satellite data. Catena, v. 79, n. 3, p. 257-264, 2009.",
        "paramName": ["SS_surf"],
        "requiredBands": ["NIR"]
    },
    5: {
        "name": "Turb Paranapanema",
        "description": "Estimates the surface turbidity in reservoirs along the Paranapnema river.",
        "model": "2.45*EXP(0.00223*red)",
        "ref": "Condé, R.C.; Martinez, J.M.; Pessotto, M.A.; Villar, R.; Cochonneau, G.; Henry, R.; Lopes, W.; Nogueira, M. Indirect Assessment of Sedimentation in Hydropower Dams Using MODIS Remote Sensing Images. Remote Sensing, v.11, n. 3, 2019.",
        "paramName": ["Turb_surf"],
        "requiredBands": ["red"]
    },
    10: {
        "name": "Brumadinho_2020simp",
        "description": "Estimates the surface suspended solids concentration in the Paraopeba River, accounting for the presence of mining waste after the 2019 disaster.",
        "model": "more than one",
        "ref": "VENTURA, 2020 (Unpublished).",
        "paramName": ["SS_surf"],
        "requiredBands": ["red", "green", "NIR"]
    },
    11: {
        "name": "Açudes SSS-ISS-OSS-Chla",
        "description": "Estimates four parameters for the waters of Brazilian semiarid reservoirs: surface suspended solids, its organic and inorganic fractions, and chlorophyll-a.",
        "model": "more than one",
        "ref": "VENTURA, 2020 (Unpublished).",
        "paramName": ["SS_surf","ISS_surf","OSS_surf","chla_surf","biomass_surf"],
        "requiredBands": ["blue", "green", "red", "NIR"]
    },
    12: {
        "name": "Açudes Chla 2022",
        "description": "Estimates chlorophyll-a in Brazilian semiarid reservoirs.",
        "model": "-4.227 + 0.1396*G + -0.1006*R",
        "ref": "VENTURA, 2022 (Unpublished).",
        "paramName": ["chla_surf"],
        "requiredBands": ["green", "red"]
    },
    99: {
        "name": "Test",
        "description": "This algorithm is for test only. It adds a band 'turb_surf' with a constant value of 1234.",
        "ref": "",
        "model": "",
        "paramName": ["turb_surf"],
        "requiredBands": ["red", "NIR"]
    }
}
estimation_algo_list = [*estimation_algo_specs]

# Reducers calculate a statistical parameter to the selected pixels in order to "reduce" them to a single representative value:
reduction_specs = {
    0: {
        "description": "none",
        "sufix": [""]
    },
    1: {
        "description": "median",
        "sufix": ["median"]
    },
    2: {
        "description": "mean",
        "sufix": ["mean"]
    },
    3: {
        "description": "mean & stdDev",
        "sufix": ["mean", "stdDev"]
    },
    4: {
        "description": "min & max",
        "sufix": ["min", "max"]
    },
    5: {
        "description": "count",
        "sufix": ["count"]
    },
    6: {
        "description": "sum",
        "sufix": ["sum"]
    },
    7: {
        "description": "median, mean, stdDev, min & max",
        "sufix": ["median", "mean", "stdDev", "min", "max"]
    }
}
reducerList = [(str(k) + " (" + reduction_specs[k]["description"] + ")") for k in range(len(reduction_specs))]    


#%% Functions

# An R-like 'which' function for Pandas series.
# Credits to Alex Miller <https://alex.miller.im/posts/python-pandas-which-function-indices-similar-to-R/>
def which(self):
    try:
        self = list(iter(self))
    except TypeError as e:
        raise Exception("""'which' method can only be applied to iterables.
        {}""".format(str(e)))
    indices = [i for i, x in enumerate(self) if bool(x) == True]
    return(indices)

# Write lines to log file.
def writeToLogFile(lines, entryType, identifier):
    if not isinstance(lines, list):
        lines = [lines]
    try:
        dateAndTime = pd.Timestamp.now().strftime("%Y-%m-%d %H:%M")
        f = open(log_file, "a")
        for line in lines:
            f.write(dateAndTime + "," + str(entryType) + "," + str(identifier) + "," + line + "\n")
        f.close()
    except:
        print("(!)")
        print("The message(s) below could not be written to the log file (" + log_file + "):")
        for line in lines:
            print(line)
        print("(.)")

# Extract polygon coordinates from a kml file, which must be a simple kml, containing only the polygon.
def polygonFromKML(kmlFile):
    try:
        # Read the file as a string.
        with open(kmlFile, 'rt', encoding="utf-8") as file:
            doc = file.read()   
        # Create the KML object to store the parsed result.
        k = kml.KML()
        # Read the KML string.
        k.from_string(doc)
        structDict = {0: list(k.features())}
    except:
        return []
    
    # Search for polygons.
    polygons = []
    idList = [0]
    curID = 0
    lastID = 0
    try:
        while curID <= lastID:
            curFeatures = structDict[curID]
            for curFeature in curFeatures:
                if "_features" in [*vars(curFeature)]:
                    lastID = idList[-1] + 1
                    idList.append(lastID)
                    structDict[lastID] = list(curFeature.features())
                elif "_geometry" in [*vars(curFeature)]:
                    geom = curFeature.geometry
                    if geom.geom_type == "Polygon":
                        coords = [list(point[0:2]) for point in geom.exterior.coords]
                        if coords == []:
                            coords = [list(point[0:2]) for point in geom.interiors.coords]
                        if coords != []:
                            polygons.append([coords])
            curID = curID + 1
    except:
        pass
    
    return polygons

# Get the GEEDaR product list.
def listAvailableProducts():
    return available_products

# Get the list of image processing algorithms.
def listProcessingAlgos():
    return img_proc_algo_list

# Get the list of estimation (inversion) algorithms.
def listEstimationAlgos():
    return estimation_algo_list

# Get the list of GEE image collection IDs related to a given GEEDaR product.
def getCollection(productID):
    return product_specs[productID]["collection"].set("product_id", productID)

# Given a product ID, get a dictionary with the band names corresponding to spectral regions (blue, green, red, ...).
def getSpectralBands(productID):
    commonBandsDict = {k: product_specs[productID]["bandList"][v] for k, v in product_specs[productID]["commonBands"].items() if v >= 0}
    spectralBandsList = [product_specs[productID]["bandList"][v] for v in product_specs[productID]["spectralBandInds"]]
    spectralBandsDict = {k: k for k in spectralBandsList}
    return {**commonBandsDict, **spectralBandsDict}

# Unfold the processing code into the IDs of the product and of the pixel selection and inversion algorithms.
def unfoldProcessingCode(fullCode, silent = False):
    failValues = (None, None, None, None, None)
    fullCode = str(fullCode)
    
    if len(fullCode) < 8:
        if not silent:
            raise Exception("Unrecognized processing code: '" + fullCode + "'. It must be a list of integers in the form PPPSSRRA (PPP is one of the product IDs listed by '-h:products'; SS is the code of the pixel selection algorithm; RR, the code of the processing algorithm; and A, the code of the reducer.).")
        else:
            return failValues
    
    if fullCode[0] == "[" and fullCode[-1] == "]":
        fullCode = fullCode[1:-1]
        
    strCodes = fullCode.replace(" ", "").split(",")
    
    processingCodes = []
    productIDs = []
    imgProcAlgos = []
    estimationAlgos = []
    reducers = []
    
    for strCode in strCodes:
        try:
            code = int(strCode)
        except:
            if not silent:
                print("(!)")
                raise Exception("Unrecognized processing code: '" + strCode + "'. It should be an integer in the form PPPSSRRA (PPP is one of the product IDs listed by '-h:products'; SS is the code of the pixel selection algorithm; RR, the code of the processing algorithm; and A, the code of the reducer.).")
            else:
                return failValues
        if code < 10000000:
            if not silent:
                print("(!)")
                raise Exception("Unrecognized processing code: '" + strCode + "'.")
            else:
                return failValues
        
        processingCodes.append(code)
        
        productID = int(strCode[0:3])
        if not productID in available_products:
            if not silent:
                print("(!)")
                raise Exception("The product ID '" + str(productID) + "' derived from the processing code '" + strCode + "' was not recognized.")
            else:
                return failValues
        productIDs.append(productID)
        
        imgProcAlgo = int(strCode[3:5])
        if not imgProcAlgo in img_proc_algo_list:
            if not silent:
                print("(!)")
                raise Exception("The image processing algorithm ID '" + str(imgProcAlgo) + "' derived from the processing code '" + strCode + "' was not recognized.")
            else:
                return failValues
        imgProcAlgos.append(imgProcAlgo)
        
        estimationAlgo = int(strCode[5:7])
        if not estimationAlgo in estimation_algo_list:
            if not silent:
                print("(!)")
                raise Exception("The estimation algorithm ID '" + str(estimationAlgo) + "' derived from the processing code '" + strCode + "' was not recognized.")
            else:
                return failValues
        estimationAlgos.append(estimationAlgo)
        
        reducer = int(strCode[-1])
        if not reducer in range(len(reducerList)):
            if not silent:
                print("(!)")
                raise Exception("The reducer code '" + str(reducer) + "' in the processing code '" + strCode + "' was not recognized. The reducer code must correspond to an index of the reducer list: " + str(reducerList) + ".")
            else:
                return failValues
        reducers.append(reducer)
    
    return processingCodes, productIDs, imgProcAlgos, estimationAlgos, reducers

# Mask bad pixels based on the respective "pixel quality assurance" layer.
def qaMask_collection(productID, imageCollection, addBand = False):
    qaLayerName = product_specs[productID]["qaLayer"]
    if qaLayerName == "" or qaLayerName == []:
        if addBand:
            return ee.ImageCollection(imageCollection).map(lambda image: image.addBands(ee.Image(1).rename("qa_mask")))
        else:
            return ee.ImageCollection(imageCollection)
    
    # MODIS bands 1-7 (Terra and Aqua)
    if productID in range(100, 120):
        qaLayer = [qaLayerName[0], qaLayerName[0], qaLayerName[0]]
        startBit = [0, 6, 8]
        endBit = [2, 7, 9]
        testExpression = ["b(0) == 0", "b(0) < 2", "b(0) == 0"]
    # Sentinel-2 L2A
    elif productID == 201:
        qaLayer = [qaLayerName[0]]
        startBit = [0]
        endBit = [7]
        testExpression = ["b(0) >= 4 && b(0) <= 7"]
    # Sentinel-2 L1C
    elif productID == 202:
        qaLayer = [qaLayerName[0]]
        startBit = [10]
        endBit = [11]
        testExpression = ["b(0) == 0"]
    # Landsat 5 and 7 SR Collection 1
    elif productID in [301,302]:
        qaLayer = [qaLayerName[0]]
        startBit = [3]
        endBit = [5]
        testExpression = ["b(0) == 0"]
    # Landsat 8 SR Collection 1
    elif productID in [303]:
        qaLayer = [qaLayerName[0],qaLayerName[1]]
        startBit = [3,6]
        endBit = [5,7]
        testExpression = ["b(0) == 0", "b(0) <= 1"]
    # Landsat 4, 5 and 7 Level 2 Collection 2
    elif productID in [311,312,313]:
        qaLayer = [qaLayerName[0]]
        startBit = [1]
        endBit = [5]
        testExpression = ["b(0) == 0"]
    # Landsat 8 and 9 Level 2 Collection 2
    elif productID in [314,315]:
        qaLayer = [qaLayerName[0],qaLayerName[1]]
        startBit = [1,6]
        endBit = [5,7]
        testExpression = ["b(0) == 0", "b(0) <= 1"]
    # VIIRS
    elif productID in [151,152]:
        qaLayer = [qaLayerName[0],qaLayerName[1]]
        startBit = [2,3]
        endBit = [4,7]
        testExpression = ["b(0) == 0", "b(0) == 0"]
    else:
        if addBand:
            return ee.ImageCollection(imageCollection).map(lambda image: image.addBands(ee.Image(1).rename("qa_mask")))
        else:
            return ee.ImageCollection(imageCollection)
    
    maskVals = []
    for i in range(len(startBit)):
        bitToInt = 0
        for j in range(startBit[i], endBit[i] + 1):
            bitToInt = bitToInt + int(math.pow(2, j))
        maskVals.append(bitToInt)
    
    def qaMask(image):
      mask = ee.Image(1)
      for i in range(len(maskVals)):
        mask = mask.And(image.select(qaLayer[i]).int().bitwiseAnd(maskVals[i]).rightShift(startBit[i]).expression(testExpression[i]));
      if addBand:
        image = image.addBands(mask.rename("qa_mask"))
      return image.updateMask(mask);

    return ee.ImageCollection(imageCollection).map(qaMask)
    
# Get the dates of the images in the collection which match AOI and user dates.
def getAvailableDates(productID, dateList):
    dateMin = dateList[0]
    dateMax = (pd.Timestamp(dateList[-1]) + pd.Timedelta(1, "day")).strftime("%Y-%m-%d")
    imageCollection = ee.ImageCollection(getCollection(productID)) \
        .filterBounds(aoi) \
        .filterDate(dateMin, dateMax) \
        .map(lambda image: image.set("img_date", ee.Image(image).date().format("YYYY-MM-dd"))) \
        .filter(ee.Filter.inList("img_date", dateList))
    return imageCollection.aggregate_array("img_date").getInfo()
    
# Apply an image processing algorithm to the image collection to get spectral data.
def imageProcessing(algo, productID, dateList, clip = True):
    global image_collection
    global bands
    global export_vars, export_bands

    # Band dictio/lists:
    bands = getSpectralBands(productID)
    irBands = [bands[band] for band in ["wl740", "wl780", "wl800", "wl900", "wl1200", "wl1500", "wl2000"] if band in bands]
    spectralBands = [product_specs[productID]["bandList"][i] for i in product_specs[productID]["spectralBandInds"]]

    # Reference band:
    refBand = product_specs[productID]["scaleRefBand"]

    # Lists of bands and variables which will be calculated and must be exported to the result data frame.
    export_vars = ["img_time"]
    export_bands = []

    # Filter and prepare the image collection.
    dateMin = dateList[0]
    dateMax = (pd.Timestamp(dateList[-1]) + pd.Timedelta(1, "day")).strftime("%Y-%m-%d")
    image_collection = ee.ImageCollection(getCollection(productID)).filterBounds(aoi).filterDate(dateMin, dateMax)
    # Set image date and time (manually set time for Modis products).
    if productID in [101,103,105,111,113,115]:
        image_collection = image_collection.map(lambda image: image.set("img_date", ee.Image(image).date().format("YYYY-MM-dd"), "img_time", "10:30"))
    elif productID in [102,104,106,112,114,116]:
        image_collection = image_collection.map(lambda image: image.set("img_date", ee.Image(image).date().format("YYYY-MM-dd"), "img_time", "13:30"))
    elif productID in [107,117]:
        image_collection = image_collection.map(lambda image: image.set("img_date", ee.Image(image).date().format("YYYY-MM-dd"), "img_time", "12:00"))    
    else:
        image_collection = image_collection.map(lambda image: image.set("img_date", ee.Image(image).date().format("YYYY-MM-dd"), "img_time", ee.Image(image).date().format("HH:mm")))
    image_collection = image_collection.filter(ee.Filter.inList("img_date", dateList))
    sortedCollection = image_collection.sort("img_date")
    imageCollection_list = sortedCollection.toList(5000)
    imgDates = ee.List(sortedCollection.aggregate_array("img_date"))
    distinctDates = imgDates.distinct()
    dateFreq = distinctDates.map(lambda d: imgDates.frequency(d))
    # Function to be mapped to the image list and mosaic same-date images.
    def oneImgPerDate(freq, imgList):
        freq = ee.Number(freq)
        localImgList = ee.List(imgList).slice(0, freq)
        firstImg = ee.Image(localImgList.get(0))
        properties = firstImg.toDictionary(firstImg.propertyNames()).remove(["system:footprint"], True)
        proj = firstImg.select(refBand).projection()
        #mosaic = ee.Image(qaMask_collection(productID, ee.ImageCollection(localImgList), True).qualityMosaic("qa_mask").setMulti(properties)).setDefaultProjection(proj).select(firstImg.bandNames())
        mosaic = ee.Image(ee.ImageCollection(localImgList).reduce(ee.Reducer.mean()).setMulti(properties)).setDefaultProjection(proj).rename(firstImg.bandNames())
        singleImg = ee.Image(ee.Algorithms.If(freq.gt(1), mosaic, firstImg))        
        return ee.List(imgList).splice(0, freq).add(singleImg)
    mosaicImgList = ee.List(dateFreq.iterate(oneImgPerDate, imageCollection_list))
    mosaicCollection = ee.ImageCollection(mosaicImgList).copyProperties(image_collection)
    image_collection = ee.ImageCollection(ee.Algorithms.If(imgDates.length().gt(distinctDates.length()), mosaicCollection, image_collection))
    # Clip the images.
    if clip:
        image_collection = image_collection.map(lambda image: ee.Image(image).clip(aoi))
    # Rescale the spectral bands.
    def rescaleSpectralBands(image):
        finalImage = image.multiply(product_specs[productID]["scalingFactor"]).add(product_specs[productID]["offset"]).copyProperties(image)
        return finalImage        
    if product_specs[productID]["scalingFactor"] and product_specs[productID]["offset"]:
        image_collection = image_collection.map(rescaleSpectralBands)

    # Reusable functions:
    
    # Set the number of unmasked pixels as an image property.
    def nSelecPixels(image):
        scale = image.select(refBand).projection().nominalScale()
        nSelecPixels = image.select(refBand).reduceRegion(ee.Reducer.count(), aoi).values().getNumber(0)
        return image.set("n_selected_pixels", nSelecPixels)

    # minNDVI clustering: select the cluster with the lowest NDVI.
    def minNDVI(image):
        nClusters = 20
        targetBands = [bands["red"], bands["NIR"]]
        redNIRimage = ee.Image(image).select(targetBands)
        ndviImage = redNIRimage.normalizedDifference([bands["NIR"], bands["red"]])
        
        # Make the training dataset for the clusterer.
        trainingData = redNIRimage.sample()
        clusterer = ee.Clusterer.wekaCascadeKMeans(2, nClusters).train(trainingData)
        resultImage = redNIRimage.cluster(clusterer)
    
        # Update the clusters (classes).
        maxID = resultImage.reduceRegion(ee.Reducer.max(), aoi).values().getNumber(0)
        clusterIDs = ee.List.sequence(0, maxID)
                   
        # Pick the class with the smallest NDVI.
        ndviList = clusterIDs.map(lambda id: ndviImage.updateMask(resultImage.eq(ee.Image(ee.Number(id)))).reduceRegion(ee.Reducer.mean(), aoi).values().getNumber(0))
        minNDVI = ndviList.sort().getNumber(0)
        waterClusterID = ndviList.indexOf(minNDVI)

        return image.updateMask(resultImage.eq(waterClusterID))

    # RICO algorithm.
    def rico(image):
        firstCut = image.updateMask(image.select(bands["NIR"]).lt(2000).And(image.select(bands["NIR"]).gte(0)).And(image.select(bands["red"]).gte(0)))
        newRed = firstCut.select(bands["red"]).subtract(firstCut.select(bands["NIR"])).unitScale(-500,500).rename("R")
        newGreen = firstCut.select(bands["NIR"]).unitScale(0,2000).rename("G")
        newBlue = firstCut.select(bands["NIR"]).subtract(500).unitScale(0,1500).rename("B")
        redwaterImg = newRed.addBands(newGreen).addBands(newBlue)
        hsvImg = redwaterImg.rgbToHsv()
        waterMask = hsvImg.select("hue").lt(0.08).selfMask()
        value = hsvImg.select("value").updateMask(waterMask)
        valueRef = value.reduceRegion(reducer = ee.Reducer.median(), geometry = aoi, bestEffort = True).values().getNumber(0)
        statMask = ee.Image(ee.Algorithms.If(valueRef, value.gte(valueRef.multiply(0.95)).And(value.lte(valueRef.multiply(1.05))), waterMask))
        waterMask = waterMask.updateMask(statMask)
        hsvImg = hsvImg.updateMask(statMask)
        hue = hsvImg.select("hue")
        saturation = hsvImg.select("saturation")
        trustIndex = saturation.subtract(hue)
        trustIndexRefs = trustIndex.reduceRegion(reducer = ee.Reducer.percentile([40,95]), geometry = aoi, bestEffort = True).values()
        trustIndexRef1 = trustIndexRefs.getNumber(0)
        trustIndexRef2 = trustIndexRefs.getNumber(1)
        sunglintMask = ee.Image(ee.Algorithms.If(trustIndexRef1, trustIndex.gte(trustIndexRef1).And(trustIndex.gte(trustIndexRef2.multiply(0.8))), waterMask))
        waterMask = waterMask.updateMask(sunglintMask)
        return image.updateMask(waterMask)
    
    # Statistic filter to remove mixed (outlier) pixels.
    def mod3rStatFilter(image):
        redNIRRatio = image.select(bands["red"]).divide(image.select(bands["NIR"]).add(1))
        redNIRRatioRef = redNIRRatio.reduceRegion(reducer = ee.Reducer.median(), geometry = aoi).values().getNumber(0)
        statMask = ee.Image(ee.Algorithms.If(redNIRRatioRef, redNIRRatio.gte(redNIRRatioRef.multiply(0.95)), image.mask()))
        return image.updateMask(statMask)

    # Function to calculate a quality flag for Modis images.
    def mod3rQualFlag(image):
        tmpImage = image
        tmpImage.set("qual_flag", 0)
        nSelecPixels = ee.Number(image.get("n_selected_pixels"))
        nValidPixels = ee.Number(image.get("n_valid_pixels"))
        nTotalPixels = ee.Number(image.get("n_total_pixels"))
        scale = image.select(refBand).projection().nominalScale()
        meanVals = image.select([bands["red"], bands["NIR"]]).reduceRegion(ee.Reducer.mean(), aoi).values()
        redMean = meanVals.getNumber(0)
        nirMean = meanVals.getNumber(1)
        convrad = ee.Number(math.pi / 180)
        if productID < 110 or productID in [151,152]:
            vzen = image.select("SensorZenith").reduceRegion(reducer = ee.Reducer.mean(), geometry = aoi, scale = scale).getNumber("SensorZenith").divide(100).multiply(convrad)
            szen = image.select("SolarZenith").reduceRegion(reducer = ee.Reducer.mean(), geometry = aoi, scale = scale).getNumber("SolarZenith").divide(100).multiply(convrad)
            solaz = image.select("SolarAzimuth").reduceRegion(reducer = ee.Reducer.mean(), geometry = aoi, scale = scale).getNumber("SolarAzimuth").divide(100).multiply(convrad)
            senaz = image.select("SensorAzimuth").reduceRegion(reducer = ee.Reducer.mean(), geometry = aoi, scale = scale).getNumber("SensorAzimuth").divide(100).multiply(convrad)
            delta = solaz.subtract(senaz)
            delta = ee.Number(ee.Algorithms.If(delta.gte(360), delta.subtract(360), delta))
            delta = ee.Number(ee.Algorithms.If(delta.lt(0), delta.add(360), delta))
            raz = delta.subtract(180).abs()
        elif productID in range(111,120):
            vzen = image.select("ViewZenith").reduceRegion(reducer = ee.Reducer.mean(), geometry = aoi, scale = scale).getNumber("ViewZenith").divide(100).multiply(convrad)
            szen = image.select("SolarZenith").reduceRegion(reducer = ee.Reducer.mean(), geometry = aoi, scale = scale).getNumber("SolarZenith").divide(100).multiply(convrad)
            raz = image.select("RelativeAzimuth").reduceRegion(reducer = ee.Reducer.mean(), geometry = aoi, scale = scale).getNumber("RelativeAzimuth").divide(100).multiply(convrad)
        sunglint = vzen.cos().multiply(szen.cos()).subtract(vzen.sin().multiply(szen.sin()).multiply(raz.cos())).acos().divide(convrad)
        sunglint = sunglint.min(ee.Number(180).subtract(sunglint))
        qual = ee.Number(1).add( \
            nValidPixels.divide(nTotalPixels).lt(0.05) \
            .Or(nSelecPixels.divide(nValidPixels).lt(0.1)) \
            .Or(nSelecPixels.lt(10)) \
        ).add( \
            vzen.divide(convrad).gte(45) \
            .Or(sunglint.lte(25)) \
        ).add( \
            nirMean.gte(1000) \
            .Or(nirMean.subtract(redMean).gte(300)) \
            .add(nirMean.gte(2000).multiply(2)) \
        )
        image = image.set("vzen", vzen.divide(convrad), "sunglint", sunglint, "qual_flag", qual.min(3))
        
        image = ee.Image(ee.Algorithms.If(nSelecPixels.gt(0), image, tmpImage))
        return image;
    
    # Calculates a quality flag for algorithms that distinguish the numbers of total, valid, water and selected pixels, such as the S2WP algorithms.
    def s2wpQualFlag(image):        
        nSelecPixels = ee.Number(image.get("n_selected_pixels"))
        nWaterPixels = ee.Number(image.get("n_water_pixels"))
        nValidPixels = ee.Number(image.get("n_valid_pixels"))
        nTotalPixels = ee.Number(image.get("n_total_pixels"))
        qualFlag = ee.Number(1).add( \
            nValidPixels.divide(nTotalPixels).lt(0.2) \
        ).add( \
            nSelecPixels.divide(nWaterPixels).lt(0.2) \
        ).add( \
            nSelecPixels.divide(nWaterPixels).lt(0.01) \
        ).min(3).multiply(nSelecPixels.min(1))
        return image.set("qual_flag", qualFlag)

    # Calculates a generic quality flag.
    def genericQualFlag(image):        
        nSelecPixels = ee.Number(image.get("n_selected_pixels"))
        nValidPixels = ee.Number(image.get("n_valid_pixels"))
        nTotalPixels = ee.Number(image.get("n_total_pixels"))
        qualFlag = ee.Number(1).add( \
            nValidPixels.divide(nTotalPixels).lt(0.2) \
        ).add( \
            nSelecPixels.divide(nValidPixels).lt(0.1) \
        ).add( \
            nSelecPixels.divide(nValidPixels).lt(0.01) \
        ).min(3).multiply(nSelecPixels.min(1))
        return image.set("qual_flag", qualFlag)
       
    # Algorithms:
    
    # 00 is the most simple one. It does nothing to the images.
    if algo == 0:
        pass
    # Simply removes pixels with cloud, cloud shadow or high aerosol.
    if algo == 1:
        image_collection = qaMask_collection(productID, image_collection)
    # MOD3R and its variations
    elif algo in [2, 3, 4]:
        export_vars = list(set(export_vars).union({"n_selected_pixels", "n_valid_pixels", "n_total_pixels", "vzen", "sunglint", "qual_flag"}))
        
        # Set the number of total pixels and remove unlinkely water pixels.
        image_collection = image_collection.map( \
            lambda image: ee.Image(image) \
                .set("n_total_pixels", ee.Image(image).select(bands["red"]).reduceRegion(ee.Reducer.count(), aoi).values().getNumber(0)) \
                .updateMask( \
                    ee.Image(image).select(bands["red"]).gte(0) \
                    .And(ee.Image(image).select(bands["red"]).lt(3000)) \
                    .And(ee.Image(image).select(bands["NIR"]).gte(0)) \
                ) \
        )
        # Remove bad pixels (cloud, cloud shadow, high aerosol and acquisition/processing issues)
        image_collection = qaMask_collection(productID, image_collection)
        # Filter out images with too few valid pixels.
        image_collection = image_collection.map(
            lambda image: ee.Image(image) \
                .set("n_valid_pixels", ee.Image(image).select(bands["red"]).reduceRegion(ee.Reducer.count(), aoi).values().getNumber(0))
        )
        image_collection_out = ee.ImageCollection(image_collection.filterMetadata("n_valid_pixels", "less_than", 10).map(lambda image: ee.Image(image).set("n_selected_pixels", 0, "qual_flag", 0).updateMask(ee.Image(0))))
        image_collection_in = ee.ImageCollection(image_collection.filterMetadata("n_valid_pixels", "greater_than", 9))

        if algo == 2:
            # MOD3R clusterer/cassifier.
            ## Run k-means with up to 5 clusters and choose the cluster which most likley represents water.
            ## For such choice, first define the cluster which probably represents soil or vegetation.
            ## Such cluster is the one with the largest difference between red and NIR.
            ## Then test every other cluster as a possible water endmember, choosing the one which yields the smaller error.
            def mod3r(image):
                nClusters = 20
                targetBands = [bands["red"], bands["NIR"]]
                redNIRimage = ee.Image(image).select(targetBands)
                
                # Make the training dataset for the clusterer.
                trainingData = redNIRimage.sample()
                clusterer = ee.Clusterer.wekaCascadeKMeans(2, nClusters).train(trainingData)
                resultImage = redNIRimage.cluster(clusterer)
            
                # Update the clusters (classes).
                maxID = ee.Image(resultImage).reduceRegion(ee.Reducer.max(), aoi).values().get(0)
                clusterIDs = ee.List.sequence(0, ee.Number(maxID))
                
                # Get the mean band values for each cluster.
                clusterBandVals = clusterIDs.map(lambda id: redNIRimage.updateMask(resultImage.eq(ee.Image(ee.Number(id)))).reduceRegion(ee.Reducer.mean(), aoi))
            
                # Get a red-NIR difference list.
                redNIRDiffList = clusterBandVals.map(lambda vals: ee.Number(ee.Dictionary(vals).get(bands["NIR"])).subtract(ee.Number(ee.Dictionary(vals).get(bands["red"]))))
            
                # Pick the class with the greatest difference to be the land endmember.
                greatestDiff = redNIRDiffList.sort().reverse().get(0)
                landClusterID = redNIRDiffList.indexOf(greatestDiff)
                # The other clusters are candidates for water endmembers.
                waterCandidateIDs = clusterIDs.splice(landClusterID, 1)
            
                # Apply, for every water candidate cluster, an unmix procedure with non-negative-values constraints.
                # Then choose as water representative the one which yielded the smaller prediction error.
                landEndmember = ee.Dictionary(clusterBandVals.get(landClusterID)).values(targetBands)
                landEndmember_red = ee.Number(landEndmember.get(0))
                landEndmember_nir = ee.Number(landEndmember.get(1))
                landImage = ee.Image(landEndmember_red).addBands(ee.Image(landEndmember_nir)).rename(targetBands)
                minError = ee.Dictionary().set("id", ee.Number(waterCandidateIDs.get(0))).set("val", ee.Number(2147483647))
                
                # Function for getting the best water candidate.
                def pickWaterCluster(id, errorDict):
                    candidateWaterEndmember = ee.Dictionary(clusterBandVals.get(ee.Number(id))).values(targetBands)
                    candidateWaterEndmember_red = ee.Number(candidateWaterEndmember.get(0))
                    candidateWaterEndmember_nir = ee.Number(candidateWaterEndmember.get(1))
                    candidateWaterImage = ee.Image(candidateWaterEndmember_red).addBands(ee.Image(candidateWaterEndmember_nir)).rename(targetBands)
                    otherCandidatesIDs = waterCandidateIDs.splice(ee.Number(id), 1)
                    def testCluster(otherID, accum):
                        maskedImage = redNIRimage.updateMask(resultImage.eq(ee.Number(otherID)))
                        fractions = maskedImage.unmix([landEndmember, candidateWaterEndmember], True, True)
                        predicted = landImage.multiply(fractions.select("band_0")).add(candidateWaterImage.multiply(fractions.select("band_1")))
                        return ee.Number(maskedImage.subtract(predicted).pow(2).reduce(ee.Reducer.sum()) \
                            .reduceRegion(ee.Reducer.mean(), aoi).values().get(0)).add(ee.Number(accum))
                    errorSum = otherCandidatesIDs.iterate(testCluster, 0)
                    errorDict = ee.Dictionary(errorDict)
                    prevError = ee.Number(errorDict.get("val"))
                    prevID = ee.Number(errorDict.get("id"))
                    newError = ee.Algorithms.If(ee.Number(errorSum).lt(prevError), errorSum, prevError)
                    newID = ee.Algorithms.If(ee.Number(errorSum).lt(prevError), ee.Number(id), prevID)    
                    return errorDict.set("id", newID).set("val", newError)
                
                waterClusterID = ee.Number(ee.Dictionary(waterCandidateIDs.iterate(pickWaterCluster, minError)).get("id"))
                
                # Return the image with non-water clusters masked, with the clustering result as a band and with the water cluster ID as a property.
                return image.updateMask(resultImage.eq(ee.Image(waterClusterID)))
    
        elif algo == 3:
            # minNDVI: a MOD3R modification. Get the lowest-NDVI cluster.
            mod3r = minNDVI
   
        elif algo == 4:
            # minNIR: a MOD3R modification. Get the lowest-NIR cluster.
            def mod3r(image):
                nClusters = 20
                targetBands = [bands["red"], bands["NIR"]]
                redNIRimage = ee.Image(image).select(targetBands)
                
                # Make the training dataset for the clusterer.
                trainingData = redNIRimage.sample()
                clusterer = ee.Clusterer.wekaCascadeKMeans(2, nClusters).train(trainingData)
                resultImage = redNIRimage.cluster(clusterer)
            
                # Update the clusters (classes).
                maxID = resultImage.reduceRegion(ee.Reducer.max(), aoi).values().getNumber(0)
                clusterIDs = ee.List.sequence(0, maxID)
                           
                # Pick the class with the smallest NDVI.
                nirList = clusterIDs.map(lambda id: redNIRimage.select(bands["NIR"]).updateMask(resultImage.eq(ee.Image(ee.Number(id)))).reduceRegion(ee.Reducer.mean(), aoi).values().getNumber(0))
                minNIR = nirList.sort().getNumber(0)
                waterClusterID = nirList.indexOf(minNIR)

                return ee.Image(image).updateMask(resultImage.eq(waterClusterID))

        # Run the modified MOD3R algorithm and set the quality flag.
        image_collection_in = image_collection_in.map(mod3r).map(mod3rStatFilter).map(nSelecPixels).map(mod3rQualFlag)
        
        # Reinsert the unprocessed images.
        image_collection = ee.ImageCollection(image_collection_in.merge(image_collection_out)).copyProperties(image_collection)
    
    # Ventura 2018 (Açudes)
    elif algo == 5:
        # Remove bad pixels (cloud, cloud shadow, high aerosol and acquisition/processing issues)
        image_collection = qaMask_collection(productID, image_collection)
        # Remove pixels with NIR > 400.
        image_collection = ee.ImageCollection(image_collection).map( \
            lambda image: ee.Image(image).updateMask(ee.Image(image).select(bands["NIR"]).lte(400).And(ee.Image(image).select(bands["NIR"]).gte(0)))
        )
    
    # Sentinel-2 Water Processing (S2WP) algorithm version 6.
    elif algo in [6, 7, 8]:
        export_vars = list(set(export_vars).union({"n_selected_pixels"}))
        def s2wp6(image):
            blue = image.select(bands["blue"])
            green = image.select(bands["green"])
            nir = image.select(bands["NIR"])
            swir2 = image.select(bands["wl2000"])
            minSWIR = image.select([bands["wl1500"],bands["wl2000"]]).reduce(ee.Reducer.min())
            maxGR = image.select([bands["green"], bands["red"]]).reduce(ee.Reducer.max())
            blueNIRratio = blue.divide(nir)
            b1pred = blue.multiply(1.1470590).add(green.multiply(-0.24835489)).add(38.96482)
            vis = image.select([bands["blue"], bands["green"], bands["red"]])
            maxV = vis.reduce(ee.Reducer.max())
            minV = vis.reduce(ee.Reducer.min())
            maxDiffV = maxV.subtract(minV)
            ndwi = image.normalizedDifference([bands["green"], bands["NIR"]])
            ci = image.normalizedDifference([bands["red"], bands["green"]])
            rg = image.select(bands["red"]).divide(image.select(bands["green"]))
            ndwihvt2 = maxGR.addBands(minSWIR).normalizedDifference()
            aeib2 = maxDiffV.subtract(blue)
            aeib1 = maxDiffV.subtract(b1pred)
            nirMaxVratio = nir.divide(maxV)
            predNIRmaxVratioHighR = rg.multiply(1.45589130421).exp().multiply(0.0636397716305)
            nirMaxVratioDevHighR = nirMaxVratio.subtract(predNIRmaxVratioHighR)
            
            image = image.updateMask(ndwihvt2.gte(0).And(minSWIR.lt(420)))
            darkAndInterW = maxDiffV.lt(250) \
              .And(nir.lt(300)) \
              .And(aeib2.gte(-450)) \
              .And(maxDiffV.gte(120).And(swir2.lt(125)).Or(ci.lt(0.08).And(swir2.lt(60)))) \
              .And(ndwihvt2.gte(0.78).Or(aeib2.subtract(ci.polynomial([-151.17, -359.17])).abs().lt(80)))
            darkW = darkAndInterW.And(maxDiffV.lt(120))
            interW = darkAndInterW.And(maxDiffV.gte(120).And(maxDiffV.lt(250)))
            brightW = interW.Or(maxDiffV.gte(220) \
              .And(aeib2.gte(-350)) \
              .And(ndwihvt2.gte(0.4)) \
              .And(nirMaxVratioDevHighR.lt(0.5)) \
              .And(ndwi.gte(-0.1).Or(maxDiffV.gte(420).And(ci.gte(0.3).Or(maxDiffV.gte(715))))) \
              .And(ci.lt(0.23).And(aeib2.gte(ci.polynomial([-881.33, 5266.7]))).Or(ci.gte(0.23).And(aeib2.gte(ci.polynomial([519.41, -823.53]))))) 
              .And( \
                ci.lt(-0.35).And(ndwihvt2.gte(0.78).Or(aeib1.gte(-5)).Or(blueNIRratio.gte(4))) \
                .Or(ci.gte(-0.35).And(ci.lt(-0.2)).And(ndwihvt2.gte(0.78).Or(aeib1.gte(-15)).Or(blueNIRratio.gte(5)))) \
                .Or(ci.gte(-0.2).And(ci.lt(0.3)).And(ndwihvt2.gte(0.78).Or(aeib1.gte(0)))) \
                .Or(ci.gte(0.3).And(aeib2.gte(220))) \
              ) \
            )
            # Bright + dark waters:
            if algo == 6:
                image = image.updateMask(darkW.Or(brightW))
            # Only bright waters:
            elif algo == 7:
                image = image.updateMask(brightW)
            # Only dark waters:
            elif algo == 8:
                image = image.updateMask(darkW)
            return image.set("n_selected_pixels", image.select(bands["red"]).reduceRegion(ee.Reducer.count(), aoi).values().get(0))
        image_collection = image_collection.map(s2wp6)

    # Sentinel-2 Water Processing (S2WP) algorithm versions 7 and 8.
    elif algo in [9,10,12]:
        export_vars = list(set(export_vars).union({"n_selected_pixels", "n_valid_pixels", "n_total_pixels", "n_water_pixels", "qual_flag"}))
        
        # Set the total number of pixels in the aoi as an image property:
        def totalPixels(image):
            scale = image.select(refBand).projection().nominalScale()       
            return image.set("n_total_pixels", image.select(refBand).reduceRegion(ee.Reducer.count(), aoi, scale).values().getNumber(0))
        image_collection = image_collection.map(totalPixels)
        
        # Mask clouds and set the number of valid (non-cloudy) pixels.
        def validPixels(image):        
            vis = image.select([bands["blue"], bands["green"], bands["red"]])
            maxV = vis.reduce(ee.Reducer.max())
            minV = vis.reduce(ee.Reducer.min())
            maxDiffV = maxV.subtract(minV)
            atmIndex = maxDiffV.subtract(minV)            
            # Exclude cloud pixels (it will inadvertedly pick very bright pixels):
            validPixels = atmIndex.gte(-1150)
            image = image.updateMask(validPixels)
            scale = image.select(refBand).projection().nominalScale()       
            nValidPixels = image.select(refBand).reduceRegion(ee.Reducer.count(), aoi, scale).values().getNumber(0)
            return image.set("n_valid_pixels", nValidPixels)
        image_collection = image_collection.map(validPixels)       

        # Select potential water pixels.
        def waterPixels(image):
            swir1 = image.select(bands["wl1500"])
            swir2 = image.select(bands["wl2000"])
            ndwihvt = image.select(bands["green"]).max(image.select(bands["red"])).addBands(swir2).normalizedDifference()
            waterMask = ndwihvt.gte(0).And(swir1.lt(680))
            scale = image.select(refBand).projection().nominalScale()       
            nWaterPixels = waterMask.reduceRegion(ee.Reducer.count(), aoi, scale).values().getNumber(0)
            return image.updateMask(waterMask).set("n_water_pixels", nWaterPixels)            
        image_collection = image_collection.map(waterPixels)

        # Remove border (spectrally mixed) pixels (only work for Sentinel-2).
        if productID in [201,151,152]: #"B8" in bands.values() and "B8A" in bands.values():
            if productID == 201:
                def maskBorder(image):
                    smi = image.normalizedDifference(["B8","B8A"])
                    return image.updateMask(smi.abs().lt(0.2))
            else:
                def maskBorder(image):
                    smi = image.select("I3").divide(image.select("M10"))
                    return image.updateMask(smi.lte(1))
            image_collection = image_collection.map(maskBorder)
        
        if algo == 9:
            # More appropriate for Sentinel-2 and Landsat:
            nir_thr = 2000
            blue_thr = 2000
            ndwihvt_thr_bright = 0.2
            ndwi_thr_dark = -0.15
            maxOffset = 30
        elif algo == 10:
            # More appropriate for MODIS:
            nir_thr = 1500
            blue_thr = 800
            ndwihvt_thr_bright = 0.4
            ndwi_thr_dark = 0
            maxOffset = 0
        # Algorithm - version 7.
        def s2wp7(image):
            swir2 = image.select(bands["wl2000"])
            vnir = image.select([bands["blue"], bands["green"], bands["red"], bands["NIR"]])
            offset = vnir.reduce(ee.Reducer.min()).min(0).abs()
            blue = image.select(bands["blue"]).add(offset)
            green = image.select(bands["green"]).add(offset)
            red = image.select(bands["red"]).add(offset)
            nir = image.select(bands["NIR"]).add(offset)
            vnir_offset = blue.addBands(green).addBands(red).addBands(nir);
            vis = vnir_offset.select([bands["blue"], bands["green"], bands["red"]])
            minV = vis.reduce(ee.Reducer.min())
            maxV = vis.reduce(ee.Reducer.max())
            maxDiffV = maxV.subtract(minV)
            ci = vnir_offset.normalizedDifference([bands["red"], bands["green"]])
            ndwi = vnir_offset.normalizedDifference([bands["green"], bands["NIR"]])
            ngbdi = vnir_offset.normalizedDifference([bands["green"], bands["blue"]])
            ndwihvt = green.max(red).addBands(swir2).normalizedDifference()
            # An index helpful to detect clouds (+ bright pixels), cirrus and aerosol:
            saturationIndex = maxDiffV.subtract(minV)            
            # CI-Saturation Index curves.
            curveCI_SI1 = ci.polynomial([-370, -800])
            curveCI_SI2 = -290
            curveCI_SI3 = ci.polynomial([-378.57, 1771.4])
            # A visible-spectrum-based filter which removes pixels strongly affected by aerosol, sungling and cirrus.
            saturationFilter = saturationIndex.gte(curveCI_SI1).And(saturationIndex.gte(curveCI_SI2)).And(saturationIndex.gte(curveCI_SI3))
            # CI-NDWI curves to detect sunglint and cirrus:
            curveHighR1a = ci.polynomial([0.745, 0.575])
            curveHighR1b = ci.polynomial([0.3115, -1.5926])
            curveHighR1c = ci.polynomial([0.4158, -3.0833])
            curveLowR1 = ci.polynomial([-0.3875, -2.9688])            
            # A visible & infrared filter for sunglint, cirrus and dark land pixels.
            # The filter is applied separately to low and high reflectance pixels.
            multiFilter = maxV.gte(200).And( \
                            ndwihvt.gte(ndwihvt_thr_bright) \
                            .And(ndwi.gte(0.6).Or(ndwi.gte(curveHighR1a)).Or(ndwi.gte(curveHighR1b)).Or(ndwi.gte(curveHighR1c)) \
                              # Exception for eutrophic (low-NDWI) waters:
                              .Or(ngbdi.gte(0.25).And(ndwihvt.gte(0.7))) \
                            )).Or(maxV.lt(250).And( \
                            ndwi.gte(ndwi_thr_dark).And(ndwi.gte(curveLowR1)).Or(ndwi.gte(0.6)) \
                          ))
            # "Good" water pixels:
            waterMask = saturationFilter.And(multiFilter).And(nir.lt(nir_thr)).And(blue.lt(blue_thr)).And(offset.lte(maxOffset)).selfMask()
            # Filter shadow by comparing each pixel to the median of the area of interest.
            # It must be applied to a small water surface area so to avoid shadow misclassification due to heterogeneity.
            shadowFilter = waterMask
            indicator = maxV.updateMask(waterMask)
            indicator_ref = indicator.reduceRegion(reducer = ee.Reducer.median(), geometry = aoi, bestEffort = True).values().getNumber(0)
            proportionToRef = indicator.divide(indicator_ref);
            shadowFilter = ee.Image(ee.Algorithms.If(indicator_ref, proportionToRef.gte(0.8), shadowFilter))
            waterMask = waterMask.updateMask(shadowFilter)
            return image.updateMask(waterMask)
        # Algorithm - version 8.2
        def s2wp8(image):
            # Bands and indices:
            blue = image.select(bands["blue"])
            green = image.select(bands["green"])
            red = image.select(bands["red"])
            nir = image.select(bands["NIR"])
            swir2 = image.select(bands["wl2000"])
            vis = image.select([bands["blue"], bands["green"], bands["red"]])
            minV = vis.reduce(ee.Reducer.min())
            maxV = vis.reduce(ee.Reducer.max())
            maxDiffV = maxV.subtract(minV)
            ci = image.normalizedDifference([bands["red"],bands["green"]])
            ndwihvt = green.max(red).addBands(swir2).normalizedDifference()
            # Remove negative-reflectance pixels.
            ndwihvt = ndwihvt.updateMask(minV.gte(0).And(nir.gte(0)))
            # Atmospheric Index (for detection of cloud, cirrus and aerosol).
            atmIndex2 = green.subtract(blue.multiply(2))
            # Filter pixels affected by glint, cirrus or aerosol.
            atm2ndwihvtMask = atmIndex2.gte(ndwihvt.multiply(-500).add(100))
            # "Good" water pixels:
            waterMask = atm2ndwihvtMask.And(ndwihvt.gte(0.6)).selfMask()
            # Filter shaded pixels statistically. For it to work properly, the water must be homogeneous.
            shadowFilter = waterMask
            indicator = maxV.updateMask(waterMask)
            indicator_ref = indicator.reduceRegion(reducer = ee.Reducer.median(), geometry = aoi, bestEffort = True).values().getNumber(0)
            proportionToRef = indicator.divide(indicator_ref)
            shadowFilter = ee.Image(ee.Algorithms.If(indicator_ref, proportionToRef.gte(0.5), shadowFilter))
            # Final mask:
            waterMask = waterMask.updateMask(shadowFilter)
            return image.updateMask(waterMask)
        if algo in [9,10]:
            image_collection = image_collection.map(s2wp7)
        elif algo == 12:
            image_collection = image_collection.map(s2wp8)
        
        # Set the final number of pixels as an image property:
        def selecPixels(image):
            scale = image.select(refBand).projection().nominalScale()
            return image.set("n_selected_pixels", image.select(refBand).reduceRegion(ee.Reducer.count(), aoi, scale).values().getNumber(0))
        image_collection = image_collection.map(selecPixels)

        # Quality flag.
        image_collection = image_collection.map(s2wpQualFlag)
    
    # RICO (Red In Cyan Out)
    elif algo == 11:
        if(productID < 200 and not productID in [103,104,113,114]):
            export_vars = list(set(export_vars).union({"n_selected_pixels", "n_valid_pixels", "n_total_pixels", "vzen", "sunglint", "qual_flag"}))
        else:
            export_vars = list(set(export_vars).union({"n_selected_pixels", "n_valid_pixels", "n_total_pixels", "qual_flag"}))
        # Set the number of total pixels.
        image_collection = image_collection.map(lambda image: image.set("n_total_pixels", image.select(refBand).reduceRegion(ee.Reducer.count(), aoi).values().getNumber(0)))
        # Mask bad pixels.
        image_collection = qaMask_collection(productID, image_collection)
        # Set the number of valid (remainging) pixels.
        image_collection = image_collection.map(lambda image: image.set("n_valid_pixels", image.select(refBand).reduceRegion(ee.Reducer.count(), aoi).values().getNumber(0)))
        # Apply the algorithm.
        image_collection = image_collection.map(rico).map(nSelecPixels)
        # Filter images with no good pixels.
        image_collection_out = ee.ImageCollection(image_collection.filterMetadata("n_selected_pixels", "less_than", 1).map(lambda image: ee.Image(image).set("n_selected_pixels", 0, "qual_flag", 0).updateMask(ee.Image(0))))
        image_collection_in = ee.ImageCollection(image_collection.filterMetadata("n_selected_pixels", "greater_than", 0))
        # Quality flag:
        if(productID < 200 and not productID in [103,104,113,114]):
            image_collection_in = image_collection_in.map(mod3rQualFlag)
        else:
            image_collection_in = image_collection_in.map(genericQualFlag)
        # Reinsert the unprocessed images.
        image_collection = ee.ImageCollection(image_collection_in.merge(image_collection_out)).copyProperties(image_collection)

    # minNDVI + Wang et al. 2016
    elif algo == 13:
        if(productID < 200 and not productID in [103,104,113,114]):
            export_vars = list(set(export_vars).union({"n_selected_pixels", "n_valid_pixels", "n_total_pixels", "vzen", "sunglint", "qual_flag"}))
        else:
            export_vars = list(set(export_vars).union({"n_selected_pixels", "n_valid_pixels", "n_total_pixels", "qual_flag"}))        

        # Set the number of total pixels and remove unlinkely water pixels.
        image_collection = image_collection.map( \
            lambda image: ee.Image(image) \
                .set("n_total_pixels", ee.Image(image).select(refBand).reduceRegion(ee.Reducer.count(), aoi).values().getNumber(0)) \
                .updateMask( \
                    ee.Image(image).select(bands["red"]).gte(0) \
                    .And(ee.Image(image).select(bands["red"]).lt(3000)) \
                    .And(ee.Image(image).select(bands["NIR"]).gte(0)) \
                ) \
        )
        # Remove bad pixels (cloud, cloud shadow, high aerosol and acquisition/processing issues)
        image_collection = qaMask_collection(productID, image_collection)
        # Filter out images with too few valid pixels.
        image_collection = image_collection.map(
            lambda image: ee.Image(image) \
                .set("n_valid_pixels", ee.Image(image).select(refBand).reduceRegion(ee.Reducer.count(), aoi).values().getNumber(0))
        )
        image_collection_out = ee.ImageCollection(image_collection.filterMetadata("n_valid_pixels", "less_than", 10).map(lambda image: ee.Image(image).set("n_selected_pixels", 0, "qual_flag", 0).updateMask(ee.Image(0))))
        image_collection_in = ee.ImageCollection(image_collection.filterMetadata("n_valid_pixels", "greater_than", 9))

        # Clustering.
        image_collection_in = image_collection_in.map(minNDVI)

        def wang2016(image):
            allBands = image.bandNames()
            noCorrBands = allBands.removeAll(ee.List(spectralBands))
            image = image.updateMask(image.select(spectralBands).reduce(ee.Reducer.min()).gte(0)) # Mask negative pixels
            minIR = image.select(irBands).reduce(ee.Reducer.min())
            vis = image.select([bands["blue"], bands["green"], bands["red"]])
            minV = vis.reduce(ee.Reducer.min())
            maxV = vis.reduce(ee.Reducer.max())
            image = image.updateMask(minV.gte(minIR))
            corrImage = image.select(spectralBands).subtract(minIR).rename(spectralBands)
            finalImage = ee.Image(corrImage.addBands(image.select(noCorrBands)).copyProperties(image))
            return finalImage
        image_collection_in = image_collection_in.map(wang2016).map(nSelecPixels)
        
        # Update the separate collections:
        image_collection_out = ee.ImageCollection(image_collection_out.merge(ee.ImageCollection(image_collection_in.filterMetadata("n_selected_pixels", "less_than", 1)).map(lambda image: ee.Image(image).set("n_selected_pixels", 0, "qual_flag", 0).updateMask(ee.Image(0))).copyProperties(image_collection)))
        image_collection_in = ee.ImageCollection(image_collection_in.filterMetadata("n_selected_pixels", "greater_than", 0))     
        
        # Quality flag:
        if(productID < 200 and not productID in [103,104,113,114]):
            image_collection_in = image_collection_in.map(mod3rQualFlag)
        else:
            image_collection_in = image_collection_in.map(genericQualFlag)
        # Reinsert the unprocessed images.
        image_collection = ee.ImageCollection(image_collection_in.merge(image_collection_out)).copyProperties(image_collection)
        
    # GPM daily precipitation
    elif algo == 14:
        export_vars = list(set(export_vars).union({"n_selected_pixels", "area"}))
        area = aoi.area()
        image_collection = image_collection.map(nSelecPixels).map(lambda image: ee.Image(image).set("area", ee.Number(area)))
    
    #---
        
    # If not already added, add the final number of pixels selected by the algorithm as an image propoerty.
    if not "n_selected_pixels" in export_vars:
        export_vars.append("n_selected_pixels")
        image_collection = image_collection.map(lambda image: image.set("n_selected_pixels", image.select(refBand).reduceRegion(ee.Reducer.count(), aoi, image.select(refBand).projection().nominalScale()).values().getNumber(0)))

# Apply a estimation (inversion) algorithm to the image collection to estimate a parameter (e.g. water turbidity).
def estimation(algos, productID, demandIDs = [-1]):
    global image_collection
    global anyError

    if not isinstance(algos, list):
        algos = [algos]
    
    productBands = list(set(list(bands.values())))
    image_collection = ee.ImageCollection(image_collection).select(productBands + export_bands)
    
    for algo_i in range(len(algos)):
        algo = algos[algo_i]

        # Check if the required bands for running the estimation algorithm are prensent in the product.
        requiredBands = estimation_algo_specs[algo]["requiredBands"]
        if not all(band in list(bands.keys()) for band in requiredBands):
            msg = "(!) The product #" + str(productID) + " does not contain all the bands required to run the estimation algorithm #" + str(algo) + ": " + str(requiredBands) + "."
            if running_mode < 3:
                print(msg)
            elif running_mode >= 3:
                anyError = True
                print("[DEMANDID " + str(demandIDs[algo_i]) + "] " + msg)
                writeToLogFile(msg, "Error", "DEMANDID " + str(demandIDs[algo_i]))
            continue
    
        # Add the estimated variable to the list of reduction.
        varName = estimation_algo_specs[algo]["paramName"]
        if not isinstance(varName, list):
            varName = [varName]
        if not varName == [""]:
            export_bands.extend(varName)
        
        # 00 is the most simple one. It does nothing with the images.
        if algo == 0:
            pass
        # Conc. de clorofila-a em açudes do Nordeste.
        elif algo == 1:
            def estim(image):
                red = image.select(bands["red"])
                green = image.select(bands["green"])
                ind = red.subtract(red.pow(2).divide(green))
                return image.addBands(ind.pow(2).multiply(0.0004).add(ind.multiply(0.213)).add(4.3957).rename(varName[0]))
            image_collection = image_collection.map(estim)
        # Sedimentos em Suspensão na Superfície no Solimões.
        elif algo == 2:
            image_collection = image_collection.map(lambda image: image.addBands(image.select(bands["NIR"]).divide(image.select(bands["red"])).pow(1.9189).multiply(759.12).rename(varName[0])))
        # Sedimentos em Suspensão na Superfície do Rio Madeira.
        elif algo == 3:
            def estim(image):
                nir = image.select(bands["NIR"])
                red = image.select(bands["red"])
                nirRedRatio = nir.divide(red)
                filter = nirRedRatio.pow(2).multiply(421.63).add(nirRedRatio.multiply(1027.6)).subtract(nir).abs()
                sss = nirRedRatio.updateMask(filter.lt(200)).pow(2.94).multiply(1020).rename(varName[0])
                return image.addBands(sss)
            image_collection = image_collection.map(estim)                                
        # Sedimentos em Suspensão na Superfície em Óbidos, no rio Amazonas.
        elif algo == 4:
            image_collection = image_collection.map(lambda image: image.addBands(image.select(bands["NIR"]).multiply(0.2019).add(-14.222).rename(varName[0])))
        # Turbidez nos reservatórios do Paranapanema.
        elif algo == 5:
            image_collection = image_collection.map(lambda image: image.addBands(image.select(bands["red"]).multiply(0.00223).exp().multiply(2.45).rename(varName[0])))
        # SSS no Paraopeba.
        elif algo == 10:
            def estim(image):
                nir = image.select(bands["NIR"])
                red = image.select(bands["red"])
                green = image.select(bands["green"])
                rejeito = green.divide(math.pi * 10000).pow(-1).subtract(red.divide(math.pi * 10000).pow(-1))
                ind1 = nir.divide(math.pi * 10000).multiply(red.divide(green))
                ind2 = nir.divide(red)
                normalCase = ind1.pow(2).multiply(18381).add(ind1.multiply(3874.8))
                specialCase = ind2.pow(2).multiply(9205.5).add(ind2.multiply(-9253.8))
                sss = normalCase.where(ind2.gte(0.9).And(rejeito), specialCase).rename(varName[0])
                return image.addBands(sss)
            image_collection = image_collection.map(estim)                
        # SSS, ISS, OSS and chla in Brazilian semiarid reservoirs.
        elif algo == 11:
            def estim(image):
                nir = image.select(bands["NIR"])
                red = image.select(bands["red"])
                green = image.select(bands["green"])
                blue = image.select(bands["blue"])
                iss = red.subtract(nir).multiply(0.059).add(green.subtract(nir).multiply(-0.0245)).add(0.74)
                iss = iss.where(iss.lt(0), 0).rename(varName[1])
                sss = red.subtract(blue).multiply(0.06318).add(green.multiply(0.009793)).add(1.363)
                sss = sss.where(iss.gt(sss), iss).rename(varName[0])
                oss = sss.subtract(iss).rename(varName[2])
                chla = green.multiply(0.0937).add(iss.multiply(-3.752)).add(-10.92)
                chla = chla.where(chla.lt(0), 0).rename(varName[3])
                biomass = chla.multiply(0.02386).exp().multiply(1.55465).rename(varName[4])
                return image.addBands(sss).addBands(iss).addBands(oss).addBands(chla).addBands(biomass)
            image_collection = image_collection.map(estim)                                
        # Chla in Brazilian semiarid reservoirs.
        elif algo == 12:
            def estim(image):
                chla = image.select(bands["green"]).multiply(0.1396).add(image.select(bands["red"]).multiply(-0.1006)).add(-4.227).rename(varName[0])
                return image.addBands(chla)
            image_collection = image_collection.map(estim)                                
        # 99 is for tests only.
        elif algo == 99:
            image_collection = image_collection.map(lambda image: image.addBands(ee.Image(1234).rename(varName[0])))
    
# Function for reducing the values of each image (previously masked) in a collection applying the predefined reducer (mean, median, ...)
def reduction(reducer, productID):
    global image_collection
    global ee_reducer
    
    # Parameters to include in the result data frame:
    paramList = ee.List(export_vars)    
    def getParamVals(image, result):
        return ee.Dictionary(result).set(ee.Image(image).get("img_date"), ee.Dictionary.fromLists(paramList, paramList.map(lambda paramName: ee.Image(image).get(ee.String(paramName)))))   
    first = ee.Dictionary()
    paramDict = ee.Dictionary(ee.ImageCollection(image_collection).iterate(getParamVals, first))
  
    if reducer == 0:
        return paramDict
    else:
        if reducer == 1:
            ee_reducer = ee.Reducer.median()
        elif reducer == 2:
            ee_reducer = ee.Reducer.mean()
        elif reducer == 3:
            ee_reducer = ee.Reducer.mean().combine(reducer2 = ee.Reducer.stdDev(), sharedInputs = True)
        elif reducer == 4:
            ee_reducer = ee.Reducer.minMax()
        elif reducer == 5:
            ee_reducer = ee.Reducer.count()
        elif reducer == 6:
            ee_reducer = ee.Reducer.sum()
        elif reducer == 7:
            ee_reducer = ee.Reducer.median() \
                .combine(reducer2 = ee.Reducer.mean(), sharedInputs = True) \
                .combine(reducer2 = ee.Reducer.stdDev(), sharedInputs = True) \
                .combine(reducer2 = ee.Reducer.minMax(), sharedInputs = True)
       
    band = product_specs[productID]["scaleRefBand"]
       
    # Combine the dictionaries of parameters and of band values.
    def combDicts(key, subDict):
        return ee.Dictionary(subDict).combine(ee.Dictionary(paramDict).get(key))
    
    successful = False
    timeoutcounts = 0
    tileScale = 1
    for c in range(3):
        
        def reduce(image, result):
            scale = image.select(band).projection().nominalScale()
            return ee.Dictionary(result).set(ee.Image(image).get("img_date"), ee.Image(image).reduceRegion(reducer = ee.Reducer(ee_reducer), geometry = aoi, scale = scale, bestEffort = True, tileScale = tileScale))
        #first = ee.Dictionary()
        bandDict = ee.Dictionary(ee.ImageCollection(image_collection).iterate(reduce, first))
        
        try:
            result = bandDict.map(combDicts).getInfo()
            successful = True
            #if c > 0:
            #print("Successful retrieval.")
            break
        except Exception as e:
            print("(!)")
            print(e)
            if str(e) == "Computation timed out.":
                if c < 2:
                    print("Trying again...")                    
                timeoutcounts = timeoutcounts + 1
                if(timeoutcounts >= 2):
                    # On the second failure for computation timeout, process images one by one:
                    localDateList = image_collection.aggregate_array("img_date").getInfo()
                    if len(localDateList) > 1:
                        print("This time processing images one by one:")                    
                        result = ee.Dictionary()
                        for localDate in localDateList:
                            localImageCollection = image_collection.filterDate(localDate, (pd.Timestamp(localDate) + pd.Timedelta(1, "day")).strftime("%Y-%m-%d"))
                            #first = ee.Dictionary()
                            paramDict = ee.Dictionary(ee.ImageCollection(localImageCollection).iterate(getParamVals, first))
                            bandDict = ee.Dictionary(ee.ImageCollection(localImageCollection).iterate(reduce, first))
                            localResult = bandDict.map(combDicts)
                            print(localDate + ": ", end = '')
                            try:
                                result = ee.Dictionary(result).combine(localResult).getInfo()
                                print("successful retrieval.")
                                successful = True
                            except:
                                print("Failed.")
                        break
            elif str(e)[:40] == "Output of image computation is too large":
                if c < 2:
                    print("Trying with a different tileScale parameter: " + str(tileScale) + "...")
                    tileScale = tileScale * 2
                else:
                    print("Failed.")
            else:
                if c < 2:
                    print("Trying again in 30 seconds...")
                    sleep(30)
                else:
                    print("Failed.")
                    
    if not successful:
        return
        
    reducedBands = list({*bands.values()}) + export_bands
    sufix = reduction_specs[reducer]["sufix"][0]
    if len(reduction_specs[reducer]["sufix"]) == 1:
        for k1 in result:
            for k2 in [*result[k1]]:
                if k2 in reducedBands:
                    result[k1][k2 + "_" + sufix] = result[k1].pop(k2)
    #print("Successful retrieval.")
    
    return result

# Load as a data frame the user-provided CSV file and check its contents, determining the running mode.
def loadInputDF():
    global user_df, input_df
    global running_mode, aoi_mode
    
    if running_mode < 3:
        # If a kml was pointed as input file...
        if input_file[-4:] == ".kml":
            print("Building input data frame...")
            aoi_mode = "kml"
            running_mode = 2
            
            if input_file == "*.kml":
                kmlFiles = [f for f in os.listdir(input_dir) if os.path.isfile(os.path.join(input_dir, f)) and f[-4:] == ".kml"]
            else:
                kmlFiles = [input_file]
            user_df = pd.DataFrame(columns = ["id","start_date","end_date"])
            nKmlFiles = len(kmlFiles)
            if nKmlFiles == 0:
                print("(!) No kml file was found in the folder '" + input_dir + "'.")
                sys.exit(1)
            for i in range(nKmlFiles):
                siteID = kmlFiles[i][:-4]
                user_df.loc[i] = [siteID, "auto", None]
        else:
            print("Opening the input file...")
            # Read the CSV file.
            try:
                user_df = pd.read_csv(input_path)
                print(user_df)
            except Exception as e:
                print("(!) Could not read the input file.")
                raise Exception(e)
        input_df = user_df.copy()
        colnames = [c.lower() for c in [*input_df.columns]]
        if all(col in colnames for col in ["start_date", "end_date"]) and running_mode == 0:
            running_mode = 2
        elif running_mode == 0:
            running_mode = 1

# Convert a 'date-ranges' to a 'specific-dates' data frame.
def toSpecificDatesDF():
    global input_df
    
    colnames = [c.lower() for c in [*input_df.columns]]
    if not (all(col in colnames for col in ["lat", "long", "start_date", "end_date"]) or all(col in colnames for col in ["id", "start_date", "end_date"])):
        print("(!)")
        raise Exception("The input CSV file should have the columns 'start_date', 'end_date' and 'id' or 'lat' and 'long'.")
    
    startDate_col = colnames.index("start_date")
    endDate_col = colnames.index("end_date")
    exportColumns = []
    # ID:
    try:
        id_col = colnames.index("id")
    except:
        pass
    else:
        exportColumns.append(id_col)
    # Lat/Long:
    try:
        lat_col = colnames.index("lat")
        long_col = colnames.index("long")
    except:
        pass
    else:
        exportColumns.extend([lat_col, long_col])

    # Replace empty values in 'end_date' by today's date.
    input_df.iloc[:,endDate_col].fillna(str(pd.to_datetime('today').date()), inplace = True)
    
    # Remove invalid rows.
    #input_df.dropna(subset = exportColumns, inplace = True)
    nrows = input_df.shape[0]
    
    tmpList = []
    for row_i in range(nrows):
        nDates = 0
        try:
            # Get the optimal start date, discarding the dates of the period before the beginning of the sensor operation.
            userStartDateStr = input_df.iloc[row_i, startDate_col]
            if (not isinstance(userStartDateStr, str)) or (userStartDateStr.lower() == "auto") or (userStartDateStr.replace(" ", "") == ""):
                userStartDateStr = "1960-01-01"
            userStartDate = pd.to_datetime(userStartDateStr).date()
            earliestSensorDate = pd.to_datetime('today').date()
            for prodID in product_ids:
                collectionStartDate = pd.to_datetime(product_specs[prodID]["startDate"]).date()
                earliestSensorDate = min(earliestSensorDate, collectionStartDate )
            optimalStartDate = max(userStartDate, earliestSensorDate)
            dates = [*pd.Series(pd.date_range(optimalStartDate, pd.to_datetime(input_df.iloc[row_i, endDate_col]))).astype("str")]            
            nDates = len(dates)
        except:
            pass
        if not nDates > 0:
            print("(!) Could not interpret the date range defined by 'start_date' and 'end_date' in row #" + str(row_i + 1) + " of the input CSV file. The row was ignored.")
            continue
        
        tmpDF = pd.DataFrame({"date": dates})
        for c in exportColumns:
            tmpDF[input_df.columns[c]] = input_df.iloc[row_i, c]
        tmpList.append(tmpDF)
    
    input_df = pd.concat(tmpList)

# Retrieve data in the 'speficic-dates' mode.
## Ideally, the CSV file must include the columns 'date', 'id', 'lat' and long in such order.
def specificDatesRetrieval(date_col = 0, id_col = 1, lat_col = 2, long_col = 3):
    #global image_collection
    global aoi, export_bands, export_vars
    global input_df
    global time_window

    export_bands = []
    export_vars = []

    if running_mode == 2:
        time_window = 0
        print("Converting the date-range format to the specific-dates format...")
        toSpecificDatesDF()
    
    print("Checking data in the input file...")
    
    # Data frame attributes:
    colnames = [c.lower() for c in [*input_df.columns]]
    nrows = input_df.shape[0]
    ncols = input_df.shape[1]

    # Check if the data frame has enough rows and columns:
    if nrows < 1:
        print("(!)")
        raise Exception("The input CSV file must have a header row and at least one data row.")
    if ncols < 3 and aoi_mode != "kml":
        print("(!)")
        raise Exception("The input CSV file must have a header and at least three columns (date, lat, long), unless you are defining your sites trough KML files (option -k), in which case the minimum required columns are 'date' and 'id'.")
    if ncols < 2 and aoi_mode == "kml":
        print("(!)")
        raise Exception("If you choose to define the regions of interest trough KML files, the input CSV file must include, at least, the columns 'date' and 'id'. The KML files' names must be equal to the corresponding 'id' plus the extension '.kml' and the files must be in the same folder as the CSV file.")
    
    # Update, if possible, the index of the "date" column.
    try:
        date_col = colnames.index("date")
    except ValueError:
        pass
    
    # Check the date values:
    try:
        pdDates = pd.to_datetime(input_df.iloc[:,date_col])
        input_df.iloc[:, date_col] = pd.Series(pdDates).dt.date
    except:
        print("(!)")
        raise Exception("The date column in the input file must have valid date values in the format yyyy-mm-dd.")
        
    # Update, if possible, the index of the (site) "id" column.
    try:
        id_col = colnames.index("id")
    except ValueError:
        if ncols < 4 and aoi_mode != "kml":
            id_col = -1
            lat_col = lat_col - 1
            long_col = long_col - 1
    else:
        if ncols < 4 and aoi_mode != "kml":
            print("(!)")
            raise Exception("The input CSV file must include, at least, the columns 'date', 'lat' and 'long', unless you define your sites of interest through kml files (option -k), in which case the 'date' and 'id' columns are enough.") 
    
    # Update, if possible, the index of the lat column.
    try:
        lat_col = colnames.index("lat")
    except ValueError:
        if aoi_mode == "kml":
            lat_col = -1
        
    # Update, if possible, the index of the long column.
    try:
        long_col = colnames.index("long")
    except ValueError:
        if aoi_mode == "kml":
            long_col = -1

    # Unknown id column?
    if id_col == date_col or id_col == lat_col or id_col == long_col:
        if aoi_mode == "kml":
            print("(!)")
            raise Exception("The column containing the sites' name could not be identified. Please, name it as 'id'.")
        else:
            id_col = -1

    # Check the lat/long values:
    if aoi_mode != "kml":
        if (not pd.api.types.is_numeric_dtype(input_df.iloc[:, lat_col])) or ((not pd.api.types.is_numeric_dtype(input_df.iloc[:, long_col]))):
            print("(!)")
            raise Exception("'lat' and 'long' values in the input file must be in decimal degrees.")
    
    # Get the indices of the valid rows (no NaN nor None):
    if aoi_mode == "kml":
        validIDs = input_df.iloc[:,id_col].notna()
        validLats = True
        validLongs = True
    else:
        validIDs = True
        validLats = input_df.iloc[:,lat_col].notna()
        validLongs = input_df.iloc[:,long_col].notna()
    validRows = which(input_df.iloc[:,date_col].notna() & validLats & validLongs & validIDs)
    
    if len(validRows) < 1:
        print("(!)")
        raise Exception("The input CSV file has no valid rows (rows with no missing data).")

    # Results' data frame template:
    resultDF_template = input_df.copy()
    
    # Add the adjacents dates according to the time window.
    nrows_result = nrows
    if time_window != 0:
        print("Expanding the input data to meet the time_window parameter (" + str(time_window) + ")...")
        window_size = 1 + (time_window * 2)
        nrows_tmp = len(validRows) * window_size + (nrows - len(validRows))
        tmpDF = pd.DataFrame(index = range(nrows_tmp), columns = resultDF_template.columns)
        imgDate = pd.Series(index = range(nrows_tmp), name = "img_date", dtype = "float64")
        row_j = 0
        validRows_new = []
        for row_i in range(nrows):
            if row_i in validRows:
                date_j = pd.Timestamp(resultDF_template.iloc[row_i, date_col]) - pd.Timedelta(time_window, "day")
                for window_i in range(window_size):
                    validRows_new.append(row_j)
                    tmpDF.iloc[row_j] = resultDF_template.iloc[row_i]
                    imgDate[row_j] = date_j.date()
                    date_j = date_j + pd.Timedelta(1, "day")
                    row_j = row_j + 1
            else:            
                tmpDF.iloc[row_j] = resultDF_template.iloc[row_i]
                row_j = row_j + 1
        tmpDF.insert(date_col + 1, "img_date", imgDate)
        ncols = ncols + 1
        date_col = date_col + 1
        if date_col <= id_col:
            id_col = id_col + 1
        if date_col <= lat_col:
            lat_col = lat_col + 1
        if date_col <= long_col:
            long_col = long_col + 1            
        resultDF_template = tmpDF.copy()        
        nrows_result = nrows_tmp
        validRows = validRows_new
    
    # Get the unique site IDs:
    if id_col >= 0:
        siteSeries = resultDF_template.iloc[:,id_col].astype(str)
    else:
        siteSeries = pd.Series([str([*resultDF_template.iloc[:, lat_col]][i]) + str([*resultDF_template.iloc[:, long_col]][i]) for i in range(nrows_result)])
    siteList = siteSeries.iloc[validRows].unique().tolist()

    # Result dictionary.
    resultDFs_dictio = {}
    for code_i in range(nProcCodes):
        processingCode = processing_codes[code_i]
        resultDFs_dictio[processingCode] = pd.DataFrame(data = None, index = range(nrows_result))
                 
    # Data retrieval grouped by GEEDaR product and by site.
    print("Processing started at " + str(pd.Timestamp.now()) + ".")
    dataRetrieved = False
    for site in siteList:
        print("")
        print("[Site] " + str(site))
        targetRows = [i for i in which(siteSeries == site) if i in validRows]
        dateList = [*pd.to_datetime(resultDF_template.iloc[targetRows, date_col].sort_values()).dt.strftime("%Y-%m-%d").unique()]
        aoi = None
        if aoi_mode == "kml":
            kmlFile = ""
            searchPath1 = os.path.join(input_dir, site + ".kml")
            searchPath2 = os.path.join(input_dir, "KML", site + ".kml")
            if os.path.isfile(searchPath1):
                kmlFile = searchPath1
            elif os.path.isfile(searchPath2):
                kmlFile = searchPath2
            if kmlFile == "":
                print("(!) File " + kmlFile + " was not found. The site was ignored.")
            else:
                coords = polygonFromKML(kmlFile)
                if coords != []:
                    aoi = ee.Geometry.MultiPolygon(coords)
                else:
                    print("(!) A polygon could not be extracted from the file " + kmlFile + ". The site was ignored.")
        else:                
            # Check if lat/long coordinates are the same for the site.
            lats = [*resultDF_template.iloc[targetRows, lat_col]]
            firstLat = lats[0]
            longs = [*resultDF_template.iloc[targetRows, long_col]]
            firstLong = longs[0]
            if (not all(i == firstLat for i in lats)) or (not all(i == firstLong for i in longs)):
                print("(!) Coordinates were not all the same. The first pair was used.")
            # Define the region of interest.
            aoi = ee.Geometry.Point(coords = [firstLong, firstLat]).buffer(aoi_radius)
        
        if not aoi is None:
            # If more than one processing code was provided, run one by one.
            for code_i in range(nProcCodes):
                processingCode = processing_codes[code_i]
                productID = product_ids[code_i]
                imgProcAlgo = img_proc_algos[code_i]
                estimationAlgo = estimation_algos[code_i]
                reducer = reducers[code_i]
                print("\n(" + str(processingCode) + ")")
                
                if not productID in img_proc_algo_specs[imgProcAlgo]["applicableTo"]:
                    print("(!) The image processing algorithm #" + str(imgProcAlgo) + " is not applicable to the product " + str(productID) + ". This data demand was ignored.")
                    continue

                # Get the available dates.
                tmpDateList = getAvailableDates(productID, dateList)
                availableDates = [d for d in dateList if d in tmpDateList]                
                #if not len(availableDates) == 0:
                #    availableDates = list(set(availableDates.sort()))
                nAvailableDates = len(availableDates)
                if nAvailableDates == 0:
                    print("No available data.")
                elif append_mode:
                    # Get common band names (e.g. 'red', 'blue', etc.).
                    commonBandNames = [k for k,v in product_specs[productID]["commonBands"].items() if v >= 0]
                    commonBandInds = [product_specs[productID]["commonBands"][k] for k in commonBandNames]
                    realBandNames = [product_specs[productID]["bandList"][i] for i in commonBandInds]
                    #commonBandsDictio = {product_specs[productID]["bandList"][v]:k for k,v in product_specs[productID]["commonBands"].items() if v >= 0 and k in commonBandNames}

                # Divide the request in groups to avoid exceeding GEE capacity.
                # First, calculate the number of pixels in the region of interest.
                # Then determine the number of images which correspond to a total of 100 000 pixels.
                nPixelsInAoI = aoi.area().divide(math.pow(product_specs[productID]["roughScale"], 2)).getInfo()
                maxNImgs = math.ceil(max_n_proc_pixels/nPixelsInAoI)
                group_len = min(maxNImgs, img_proc_algo_specs[imgProcAlgo]["nSimImgs"])
                nGroups = math.ceil(nAvailableDates / group_len)
                for g in range(nGroups):
                    dateSublist_inds = range(g * group_len, min(g * group_len + group_len, nAvailableDates))
                    dateSublist = [availableDates[i] for i in dateSublist_inds]
                    print("Requesting data for days " + str(g * group_len + 1) + "-" + str(min(g * group_len + group_len, nAvailableDates)) + "/" + str(nAvailableDates) + "...")
                    # Image processing, parameter estimation and reduction.
                    imageProcessing(imgProcAlgo, productID, dateSublist)
                    estimation(estimationAlgo, productID)
                    result = reduction(reducer, productID)
                    if result is None:
                        print("(!) Failed to retrieve data.")
                    elif result == {}:
                        print("No data retrieved.")
                    else:
                        dataRetrieved = True
                        # Save the retrieved data in the result data frame.
                        for date in [*result]:
                            sameDateRows = [i for i in which(resultDF_template.iloc[:,date_col].astype("str") == date) if i in targetRows]
                            for band in [*result[date]]:
                                colNames = []
                                if append_mode:
                                    for i in range(len(commonBandNames)):
                                        if realBandNames[i] + "_" in band:
                                            colNames.append(band.replace(realBandNames[i], commonBandNames[i]))
                                elif nProcCodes > 1:
                                    colNames = [str(processingCode) + "_" + band]
                                if len(colNames) == 0:
                                    colNames = [band]
                                for row_i in sameDateRows:
                                    for colName in colNames:
                                        resultDFs_dictio[processingCode].loc[row_i, colName] = result[date][band]
                        print("Data successfully retrieved.")
    print("")
    print("Processing finished at " + str(pd.Timestamp.now()) + ".")
    if dataRetrieved:
        print("Consolidating results...")
        resultDF_template.reset_index(inplace = True, drop = True)
        if append_mode:
            # Get all column names.
            cols = []
            for k in resultDFs_dictio:
                cols.extend([*resultDFs_dictio[k].columns])
            cols = set(cols)
            commonBandNames = [*product_specs[101]["commonBands"].keys()]
            # Reorder columns.
            for k in resultDFs_dictio:
                tmpDF = pd.DataFrame()
                for col in cols:
                    wosuffix = col.split("_")[0]                    
                    if not wosuffix in commonBandNames:
                        if col in [*resultDFs_dictio[k].columns]:
                            tmpDF[col] = resultDFs_dictio[k][col]
                        else:
                            tmpDF[col] = math.nan
                tmpDF = tmpDF.reindex(sorted(tmpDF.columns), axis=1)
                for band in commonBandNames:
                    matches = [col for col in cols if (band + "_") in col]
                    for col in matches:
                        if col in [*resultDFs_dictio[k].columns]:
                            tmpDF[col] = resultDFs_dictio[k][col]
                        else:
                            tmpDF[col] = math.nan
                dataColNames = tmpDF.columns
                resultDFs_dictio[k] = tmpDF
                prodID = int(str(k)[0:3])
                sensor = product_specs[prodID]["sensor"]
                resultDFs_dictio[k] = pd.concat([pd.DataFrame({"ProcCode": [k] * nrows_result, "Source": [sensor] * nrows_result}), resultDFs_dictio[k]], axis = 1, sort = False)
                resultDFs_dictio[k] = pd.concat([resultDF_template, resultDFs_dictio[k]], axis = 1, sort = False)
                if(running_mode == 2):
                    resultDFs_dictio[k].dropna(subset = dataColNames, how = "all", inplace = True)
            resultDF = pd.concat([*resultDFs_dictio.values()], sort = False)
        else:
            dataDF = pd.concat([*resultDFs_dictio.values()], axis = 1, sort = False)
            resultDF = pd.concat([resultDF_template, dataDF], axis = 1, sort = False)
            # Remove empty rows (if in running mode 2):
            if(running_mode == 2):
                resultDF.dropna(subset = dataDF.columns, how = "all", inplace = True)
    else:
        resultDF = None
    
    return resultDF

def updateGEEDaRtables():
    try:
        dbconn = sqlite3.connect(input_path)
        dbcur = dbconn.cursor()
        # Synchronize the tables for products and algorithms.
        # Products:
        prodSpecsTable = pd.read_sql_query("SELECT PRODUCTID, PRODUCTNAME, PRODUCTSENSOR, PRODUCTDESCR FROM PRODUCTS", dbconn)
        for prodID in product_specs:
            prodName = product_specs[prodID]["productName"]
            prodDesc = product_specs[prodID]["description"]
            sensor = product_specs[prodID]["sensor"]
            if not prodID in prodSpecsTable["PRODUCTID"].values:
                dbcur.execute("INSERT INTO PRODUCTS (PRODUCTID, PRODUCTNAME, PRODUCTSENSOR, PRODUCTDESCR) VALUES (?,?,?,?)", (prodID, prodName, sensor, prodDesc))
            else:
                dbcur.execute("UPDATE PRODUCTS SET PRODUCTNAME = ?, PRODUCTSENSOR = ?, PRODUCTDESCR = ? WHERE PRODUCTID = ?", (prodName, sensor, prodDesc, prodID))
        # Image processing algorithms:
        procAlgosTable = pd.read_sql_query("SELECT PROCALGOID, PROCALGONAME, PROCALGODESCR, PROCALGOREF FROM PROCALGOS", dbconn)
        for procAlgoID in img_proc_algo_specs:
            procAlgoName = img_proc_algo_specs[procAlgoID]["name"]
            procAlgoDesc = img_proc_algo_specs[procAlgoID]["description"]
            procAlgoRef = img_proc_algo_specs[procAlgoID]["ref"]
            if not procAlgoID in procAlgosTable["PROCALGOID"].values:
                dbcur.execute("INSERT INTO PROCALGOS (PROCALGOID, PROCALGONAME, PROCALGODESCR, PROCALGOREF) VALUES (?,?,?,?)", (procAlgoID, procAlgoName, procAlgoDesc, procAlgoRef))
            else:
                dbcur.execute("UPDATE PROCALGOS SET PROCALGONAME = ?, PROCALGODESCR = ?, PROCALGOREF = ? WHERE PROCALGOID = ?", (procAlgoName, procAlgoDesc, procAlgoRef, procAlgoID))
        # Parameter estimation algorithms:
        estimAlgosTable = pd.read_sql_query("SELECT ESTIMALGOID, ESTIMALGONAME, ESTIMALGODESCR, ESTIMALGOMODEL, ESTIMALGOREF FROM ESTIMALGOS", dbconn)
        for estimAlgoID in estimation_algo_specs:
            estimAlgoName = estimation_algo_specs[estimAlgoID]["name"]
            estimAlgoDesc = estimation_algo_specs[estimAlgoID]["description"]
            estimAlgoModel = estimation_algo_specs[estimAlgoID]["model"]
            estimAlgoRef = estimation_algo_specs[estimAlgoID]["ref"]
            estimAlgoParam_tmp = estimation_algo_specs[estimAlgoID]["paramName"]
            if len(estimAlgoParam_tmp) == 1:
                estimAlgoParam = estimAlgoParam_tmp[0]
            else:
                estimAlgoParam = str(estimAlgoParam_tmp)
            if not estimAlgoID in estimAlgosTable["ESTIMALGOID"].values:
                dbcur.execute("INSERT INTO ESTIMALGOS (ESTIMALGOID, ESTIMALGONAME, ESTIMALGODESCR, ESTIMALGOMODEL, ESTIMALGOREF, ESTIMALGOPARAM) VALUES (?,?,?,?,?,?)", (estimAlgoID, estimAlgoName, estimAlgoDesc, estimAlgoModel, estimAlgoRef, estimAlgoParam))
            else:
                dbcur.execute("UPDATE ESTIMALGOS SET ESTIMALGONAME = ?, ESTIMALGODESCR = ?, ESTIMALGOMODEL = ?, ESTIMALGOREF = ?, ESTIMALGOPARAM = ? WHERE ESTIMALGOID = ?", (estimAlgoName, estimAlgoDesc, estimAlgoModel, estimAlgoRef, estimAlgoParam, estimAlgoID))
        # Statistical reducers:
        reducTable = pd.read_sql_query("SELECT REDUCERID, REDUCERDESCR FROM REDUCERS", dbconn)
        for reducID in reduction_specs:
            reducDesc = reduction_specs[reducID]["description"]
            if not reducID in reducTable["REDUCERID"].values:
                dbcur.execute("INSERT INTO REDUCERS (REDUCERID, REDUCERDESCR) VALUES (?,?)", (reducID, reducDesc))
            else:
                dbcur.execute("UPDATE REDUCERS SET REDUCERDESCR = ? WHERE REDUCERID = ?", (reducDesc, reducID))
        dbconn.commit()
        dbcur.close()
        dbconn.close()
        return 0
    except Exception as e:
        print(e)
        return -1
           
# Retrieve data in a 'database mode' (running_mode in [3, 4, 5]).
# A SQLite database with standard tables is required.
def databaseUpdate():    
    global aoi, bands, image_collection, export_bands, export_vars
    global anyError
    global log_file
    
    export_bands = []
    export_vars = []
    
    # Open the GEEDaR SQLite3 database and check its contents.
    try:
        print("-")
        print("Opening database...")
        dbconn = sqlite3.connect(input_path)
        # Get the application options.
        querystr = "SELECT ATTRIBUTE, VALUE FROM APPLICATION"
        appDF = pd.read_sql_query(querystr, dbconn)
        kmlFolder = str(appDF["VALUE"][appDF["ATTRIBUTE"] == "KMLSUBDIR"].to_list()[0])
        log_file = str(appDF["VALUE"][appDF["ATTRIBUTE"] == "LOGFILE"].to_list()[0])
        runCount = int(appDF["VALUE"][appDF["ATTRIBUTE"] == "RUNCOUNT"].to_list()[0]) + 1
        # Get all the update demands.
        querystr = "SELECT DEMANDID, DEMANDSTID, DEMANDSTATUS, DEMANDPRODUCTID, DEMANDPROCALGOID, DEMANDESTIMALGOID, DEMANDREDUCID, DEMANDSTARTDATE, DEMANDENDDATE, DEMANDAOIMODE, DEMANDAOIRADIUS, DEMANDAOIKMLFILE, STID, STCOD,  STLAT, STLONG, STNAME FROM DEMANDS INNER JOIN STATIONS ON DEMANDSTID = STID WHERE DEMANDSTATUS > 0"
        demandsDF = pd.read_sql_query(querystr, dbconn)
        # Test the availability of mandatory tables and properties.
        querystr = "SELECT DATATIMESERIESID, DATAVARID, DATASTATSID, DATATIME, DATAVALUE FROM DATA LIMIT 1;"
        dataDF = pd.read_sql_query(querystr, dbconn)
        # Get the variable list.
        varDF = pd.read_sql_query("SELECT VARID, VARNAME, VARDESCR FROM VARIABLES", dbconn)
        # Get the statistical parameters list.
        statsDF = pd.read_sql_query("SELECT STATSID, STATSNAME FROM STATISTICS", dbconn)
        # Get the pre-existing time series records.
        querystr = "SELECT TIMESERIESID, TIMESERIESDEMANDID, TIMESERIESDATE FROM TIMESERIES WHERE TIMESERIESDEMANDID IN " + str(demandsDF["DEMANDID"].to_list()).replace("[","(").replace("]",")")
        timeSeriesDF = pd.read_sql_query(querystr, dbconn)        
        # Update the software version and access count (and test writing to the database).
        dbcur = dbconn.cursor()
        dbcur.execute("UPDATE APPLICATION SET VALUE = ? WHERE ATTRIBUTE = 'VERSION'", (myVersion,))
        dbcur.execute("UPDATE APPLICATION SET VALUE = ? WHERE ATTRIBUTE = 'RUNCOUNT'", (str(runCount),))
        dbconn.commit()
        dbcur.close()
        dbconn.close()
        # Synchronize the tables for products and algorithms.
        result = updateGEEDaRtables()
        if not result == 0:
            print("Failed to update products' and algorithms' tables.")
            print("Aborting...")
            sys.exit(1)
    except Exception as e:
        if dbconn:
            dbconn.close()
        print("(!) Aborting...")
        raise Exception(e)
    # Ensure the dates in TIMESERIES are in the right format.
    try:
        toCompare = pd.to_datetime(timeSeriesDF["TIMESERIESDATE"]).dt.strftime("%Y-%m-%d")
        if not toCompare.equals(timeSeriesDF["TIMESERIESDATE"]):
            print("The dates in the TIMESERIES table should be in the string format 'YYYY-MM-DD'.")    
            print("Aborting...")
            sys.exit(1)
    except:
        print("(!) Aborting...")
        raise Exception("The dates in the TIMESERIES table should be in the string format 'YYYY-MM-DD'.")
    # Start processing the demands.
    print("Processing started.")
    print("-")
    writeToLogFile("Starting 'database mode'.", "Benchmark", "-")
    # Exclude (and report) invalid demand records.
    invalidRows = []
    for row_i in range(demandsDF.shape[0]):
        isValidRow = True
        try:
            demandId = int(demandsDF.loc[row_i, "DEMANDID"])
            demandStId = int(demandsDF.loc[row_i, "DEMANDSTID"])
            demandStCod = demandsDF.loc[row_i, "STCOD"]
            demandProdId = int(demandsDF.loc[row_i, "DEMANDPRODUCTID"])
            if not demandProdId in [*product_specs]:
                isValidRow = False
                anyError = True
                writeToLogFile("Unrecognized GEEDaR product ID: " + str(demandProdId) + ".", "Error", "DEMANDID " + str(demandId))
            demandProcAlgoId = int(demandsDF.loc[row_i, "DEMANDPROCALGOID"])
            if not demandProcAlgoId in [*img_proc_algo_specs]:
                isValidRow = False
                anyError = True
                writeToLogFile("Unrecognized image processing algorithm ID: " + str(demandProcAlgoId) + ".", "Error", "DEMANDID " + str(demandId))
            demandEstimAlgoId = int(demandsDF.loc[row_i, "DEMANDESTIMALGOID"])
            if not demandEstimAlgoId in [*estimation_algo_specs]:
                isValidRow = False
                anyError = True
                writeToLogFile("Unrecognized estimation algorithm ID: " + str(demandEstimAlgoId) + ".", "Error", "DEMANDID " + str(demandId))
            demandReducId = int(demandsDF.loc[row_i, "DEMANDREDUCID"])
            if not demandReducId in range(len(reducerList)):
                isValidRow = False
                anyError = True
                writeToLogFile("Unrecognized reducer index: " + str(demandReducId) + ".", "Error", "DEMANDID " + str(demandId))
            demandAoiMode = int(demandsDF.loc[row_i, "DEMANDAOIMODE"])
            if not demandAoiMode in range(len(aoi_modes)):
                isValidRow = False
                anyError = True
                writeToLogFile("Unrecognized 'AoI mode' index: " + str(demandAoiMode) + ".", "Error", "DEMANDID " + str(demandId))
            else:
                if demandAoiMode == 0:
                    demandsDF.loc[row_i, "DEMANDAOIKMLFILE"] = ""
                    if demandsDF.loc[row_i, "DEMANDAOIRADIUS"] is None:
                        isValidRow = False
                        anyError = True
                        writeToLogFile("DEMANDAOIRADIUS should have an integer radius value, in meters, but it was empty.", "Error", "DEMANDID " + str(demandId))
                    else:
                        demandAoiRadius = int(demandsDF.loc[row_i, "DEMANDAOIRADIUS"])
                        if demandAoiRadius <= 0:
                            isValidRow = False
                            anyError = True
                            writeToLogFile("DEMANDAOIRADIUS should have a value greater than zero.", "Error", "DEMANDID " + str(demandId))
                        stLat = float(demandsDF.loc[row_i, "STLAT"])
                        if math.isnan(stLat):
                            isValidRow = False
                            anyError = True
                            writeToLogFile("Latitude value was 'NaN'.", "Error", "DEMANDID " + str(demandId))
                        stLong = float(demandsDF.loc[row_i, "STLONG"])
                        if math.isnan(stLong):
                            isValidRow = False
                            anyError = True
                            writeToLogFile("Longitude value was 'NaN'.", "Error", "DEMANDID " + str(demandId))
                elif demandAoiMode == 1:
                    demandsDF.loc[row_i, "DEMANDAOIRADIUS"] = 0
                    demandAoiKmlFile = demandsDF.loc[row_i, "DEMANDAOIKMLFILE"]
                    if demandAoiKmlFile is None:
                        demandAoiKmlFile = ""
                    else:
                        demandAoiKmlFile = str(demandAoiKmlFile)
                    if (demandAoiKmlFile.lower() == "auto") or (demandAoiKmlFile.replace(" ", "") == ""):
                        if demandStCod is None:
                            demandStCod = ""
                        if demandStCod.replace(" ", "") == "":
                            demandAoiKmlFile = None
                            isValidRow = False
                            anyError = True
                            writeToLogFile("DEMANDSTCOD was empty. The KML file could not be found automatically.", "Error", "DEMANDID " + str(demandId))
                        else:
                            demandAoiKmlFile = os.path.join(input_dir, kmlFolder, demandStCod + ".kml")
                            demandsDF.loc[row_i, "DEMANDAOIKMLFILE"] = demandAoiKmlFile
                    if not demandAoiKmlFile is None:
                        if not os.path.isfile(demandAoiKmlFile):
                            # If the kml file is in not the format STCOD.kml, check if it is in the format STCOD - STNAME.kml.
                            searchStr = demandStCod + " - "
                            searchStr_len = len(searchStr)
                            searchFolder = os.path.join(input_dir, kmlFolder)
                            possiblekmlFiles = [f for f in os.listdir(searchFolder) if os.path.isfile(os.path.join(searchFolder, f)) and f[-4:] == ".kml" and f[:searchStr_len] == searchStr]
                            if len(possiblekmlFiles) >= 1:
                                demandAoiKmlFile = os.path.join(input_dir, kmlFolder, possiblekmlFiles[0])
                                demandsDF.loc[row_i, "DEMANDAOIKMLFILE"] = demandAoiKmlFile
                                if len(possiblekmlFiles) > 1:
                                    writeToLogFile("More than one kml file was found for the station " + demandStCod + ". The first was used.", "Warning", "DEMANDID " + str(demandId))
                            else:
                                isValidRow = False
                                anyError = True
                                writeToLogFile("The file " + demandAoiKmlFile + " was not found.", "Error", "DEMANDID " + str(demandId))
            if demandsDF.loc[row_i, "DEMANDSTARTDATE"] is None:
                demandsDF.loc[row_i, "DEMANDSTARTDATE"] = ""
            if demandsDF.loc[row_i, "DEMANDSTARTDATE"].lower() == "auto" or (demandsDF.loc[row_i, "DEMANDSTARTDATE"].replace(" ", "") == ""):
                demandsDF.loc[row_i, "DEMANDSTARTDATE"] = product_specs[demandProdId]["startDate"]
            try:
                demandStartDate = pd.to_datetime(demandsDF.loc[row_i, "DEMANDSTARTDATE"])
            except:
                isValidRow = False
                anyError = True
                writeToLogFile("The value in DEMANDSTARTDATE was not a string in the format YYYY-MM-DD. The value was: '" + str(demandStartDate) + "'.", "Error", "DEMANDID " + str(demandId))
            if demandsDF.loc[row_i, "DEMANDENDDATE"] is None:
                demandsDF.loc[row_i, "DEMANDENDDATE"] = ""
            if demandsDF.loc[row_i, "DEMANDENDDATE"].lower() == "auto" or (demandsDF.loc[row_i, "DEMANDENDDATE"].replace(" ", "") == ""):
                demandsDF.loc[row_i, "DEMANDENDDATE"] = pd.Timestamp.now().strftime("%Y-%m-%d")
            else:
                try:
                    demandEndDate = pd.to_datetime(demandsDF.loc[row_i, "DEMANDENDDATE"])
                except:
                    isValidRow = False
                    anyError = True
                    writeToLogFile("The value in DEMANDENDDATE was not a string in the format YYYY-MM-DD. The value was: '" + str(demandEndDate) + "'.", "Error", "DEMANDID " + str(demandId))
        except Exception as e:
            isValidRow = False
            anyError = True
            writeToLogFile(e, "Error", "DEMANDID " + str(demandsDF.loc[row_i, "DEMANDID"]))
        if not isValidRow:
            invalidRows.append(row_i)
    # Delete the invalid rows.
    demandsDF.drop(invalidRows, axis = 0, inplace = True)
    validRows = [*demandsDF.index]
    nrows = demandsDF.shape[0]
    if nrows == 0:
        anyError = True
        msg = "No valid demand records found in table DEMANDS."
        writeToLogFile(msg, "Warning", "-")
        print("(!) " + msg)
    
    # Ensure the correct data type:
    dtypes = {
       "DEMANDID":  int,
       "DEMANDSTID": int,
       "DEMANDPRODUCTID": int,
       "DEMANDPROCALGOID": int,
       "DEMANDREDUCID": int,
       "DEMANDAOIMODE": int,
       "DEMANDAOIKMLFILE": str,
       "DEMANDSTID": int,
       "DEMANDSTARTDATE": str,
       "DEMANDENDDATE": str
    }
    demandsDF = demandsDF.astype(dtypes)
    
    # Loop over the demand records.
    alreadyProcessed = []
    for row_i in validRows:
        if not row_i in alreadyProcessed:
            demandId = int(demandsDF.loc[row_i, "DEMANDID"])
            demandStId = int(demandsDF.loc[row_i, "DEMANDSTID"])
            demandStatus = int(demandsDF.loc[row_i, "DEMANDSTATUS"])
            demandProdId = int(demandsDF.loc[row_i, "DEMANDPRODUCTID"])
            demandProcAlgoId = int(demandsDF.loc[row_i, "DEMANDPROCALGOID"])
            demandEstimAlgoId = int(demandsDF.loc[row_i, "DEMANDESTIMALGOID"])
            demandReducId = int(demandsDF.loc[row_i, "DEMANDREDUCID"])
            demandAoiMode = int(demandsDF.loc[row_i, "DEMANDAOIMODE"])
            demandAoiRadius = int(demandsDF.loc[row_i, "DEMANDAOIRADIUS"])
            demandAoiKmlFile = str(demandsDF.loc[row_i, "DEMANDAOIKMLFILE"])
            
            demandRunningMode = running_mode
            # Check the demand status to see if a running mode specific to the current demand should be adopted.
            if demandStatus > 1:
                if demandStatus in [3, 4, 5]:
                    demandRunningMode = demandStatus
                # Set status back to 1 (the standard values are 0 or 1; the possibility of using alternative values such as 4 or 5 as status is just a workaround).
                dbconn = sqlite3.connect(input_path)
                dbcur = dbconn.cursor()
                dbcur.execute("UPDATE DEMANDS SET DEMANDSTATUS = ? WHERE DEMANDID = ?;", (1, demandId))
                dbconn.commit()
                dbcur.close()
                dbconn.close()

            if demandRunningMode < 5:
                inds = which((demandsDF["DEMANDSTID"] == demandStId) & (demandsDF["DEMANDPRODUCTID"] == demandProdId) & (demandsDF["DEMANDPROCALGOID"] == demandProcAlgoId) & (demandsDF["DEMANDREDUCID"] == demandReducId) & (demandsDF["DEMANDAOIMODE"] == demandAoiMode)  & (demandsDF["DEMANDAOIRADIUS"] == demandAoiRadius) & (demandsDF["DEMANDAOIKMLFILE"] == demandAoiKmlFile))
                demandGroup_rows = demandsDF.index[inds]
                demandGroup_ids = demandsDF["DEMANDID"][demandGroup_rows].to_list()
                alreadyProcessed.extend(demandGroup_rows)
            
                # Define the AoI geometry.
                aoi = None
                aoiMode = aoi_modes[demandAoiMode]
                if aoiMode == "kml":
                    try:
                        coords = polygonFromKML(demandAoiKmlFile)
                        if coords == []:
                            anyError = True
                            writeToLogFile("No coordinates could be extracted from " + demandAoiKmlFile + ".", "Error", "DEMANDID " + str(demandId))
                    except:
                        anyError = True
                        writeToLogFile("Failed to read the file " + demandAoiKmlFile + ".", "Error", "DEMANDID " + str(demandId))
                    try:
                        if not coords == []:
                            aoi = ee.Geometry.MultiPolygon(coords)
                    except:
                        anyError = True
                        writeToLogFile("A geometry could not be created from the coordinates extracted from the file " + demandAoiKmlFile + ".", "Error", "DEMANDID " + str(demandId))
                elif aoiMode == "radius":
                    stLat = float(demandsDF.loc[row_i, "STLAT"])
                    stLong = float(demandsDF.loc[row_i, "STLONG"])
                    try:
                        aoi = ee.Geometry.Point(coords = [stLong, stLat]).buffer(demandAoiRadius)
                    except:
                        anyError = True
                        writeToLogFile("A geometry could not be created from the given coordinates (" + str(stLat) + ", " + str(stLong) + ").", "Error", "DEMANDID " + str(demandId))
                if not aoi is None:
                    # Determine the range which comprises the date ranges in the current demand group.
                    collectionStartDate = product_specs[demandProdId]["startDate"]
                    if not collectionStartDate == "":
                        startDate = pd.Series([pd.to_datetime(demandsDF.loc[demandGroup_rows, "DEMANDSTARTDATE"]).min(), pd.to_datetime(collectionStartDate)]).max().strftime("%Y-%m-%d")
                    else:
                        startDate = pd.to_datetime(demandsDF.loc[demandGroup_rows, "DEMANDSTARTDATE"]).min().strftime("%Y-%m-%d")
                    endDate = pd.to_datetime(demandsDF.loc[demandGroup_rows, "DEMANDENDDATE"]).max().strftime("%Y-%m-%d")
                    # Get the list of dates in the range.
                    tmpDateList = pd.date_range(startDate, endDate).strftime("%Y-%m-%d")
                    
                    # If in update mode, keep only the dates not yet in the database (not processed yet).
                    if demandRunningMode == 3:
                        datesToKeep_inds = set()
                        for demandGroup_id in demandGroup_ids:
                            datesToKeep_inds = datesToKeep_inds.union(which(~tmpDateList.isin(timeSeriesDF["TIMESERIESDATE"][timeSeriesDF["TIMESERIESDEMANDID"] == demandGroup_id])))
                        dateList = tmpDateList[list(datesToKeep_inds)].sort_values().to_list()
                    # Or, if in overwrite mode, process everything, ignoring what is already in the database.
                    elif demandRunningMode == 4:                    
                        dateList = tmpDateList.sort_values().to_list() 
                    dateList_len = len(dateList)

                    # Get the latest date record.
                    earliestDateRecord = {}
                    latestDateRecord = {}
                    for demandGroup_id in demandGroup_ids:
                        tmpList = timeSeriesDF["TIMESERIESDATE"][timeSeriesDF["TIMESERIESDEMANDID"] == demandGroup_id]
                        if len(tmpList) == 0:
                            earliestDateRecord[demandGroup_id] = pd.Timestamp(startDate)
                            latestDateRecord[demandGroup_id] = pd.Timestamp(startDate)
                        else:
                            earliestDateRecord[demandGroup_id] = pd.to_datetime(tmpList).min()
                            latestDateRecord[demandGroup_id] = pd.to_datetime(tmpList).max()
                        
                    filteredDateList = []
                    if dateList_len > 0:
                        # Get the available dates and avoid counting the unavailable dates to define the image groups.
                        availableDates = getAvailableDates(demandProdId, dateList)
                        filteredDateList = [d for d in dateList if d in availableDates]
                    filteredDateList_len = len(filteredDateList)
                    # Divide the request in groups to avoid exceeding GEE capacity.
                    groups = []
                    if filteredDateList_len == 0:
                        msg = "There is no new data to be retrieved. The database is up to date."
                        print("[" + "DEMANDID " + str(demandGroup_ids) + "] " + msg)
                        writeToLogFile(msg, "Result", "DEMANDID " + str(demandId))
                    else:
                        
                        # Divide the request in groups to avoid exceeding GEE capacity.
                        # First, calculate the number of pixels in the region of interest.
                        # Then determine the number of images which correspond to a total of 100 000 pixels.
                        nPixelsInAoI = aoi.area().divide(math.pow(product_specs[demandProdId]["roughScale"], 2)).getInfo()
                        maxNImgs = math.ceil(max_n_proc_pixels/nPixelsInAoI)
                        nSimImgs = min(maxNImgs, img_proc_algo_specs[demandProcAlgoId]["nSimImgs"])
                        nGroups = math.ceil(filteredDateList_len / nSimImgs)
                        dateInd = 0
                        for g in range(nGroups):
                            groups.append([])
                            nImgsIncluded = 0
                            while nImgsIncluded < nSimImgs and dateInd < dateList_len:
                                date = dateList[dateInd]
                                groups[g].append(date)
                                if date in filteredDateList:
                                    nImgsIncluded = nImgsIncluded + 1
                                dateInd = dateInd + 1
                    
                    for dateSublist in groups:
                        dateMin = dateSublist[0]
                        dateMax = dateSublist[-1]
                        print("[DEMANDID " + str(demandGroup_ids) + "] Requesting data for dates " + str(dateSublist[0]) + " to " + str(dateSublist[-1]) + ".")

                        # Image processing, parameter estimation and reduction.
                        imageProcessing(demandProcAlgoId, demandProdId, dateSublist)
                        estimation([*demandsDF["DEMANDESTIMALGOID"][demandGroup_rows]], demandProdId, demandGroup_ids)
                        result = reduction(demandReducId, demandProdId)
                        if result is None:
                            anyError = True
                            msg = "Failed to retrieve data for dates " + str(dateSublist[0]) + " to " + str(dateSublist[-1]) + "."
                            writeToLogFile(msg, "Error", "DEMANDID " + str(demandGroup_ids))
                            print("(!) " + msg)
                        #elif result == {}:
                        #    msg = "There is no available data for days " + str(g * group_len) + "-" + str(min(g * group_len + group_len - 1, dateList_len)) + "/" + str(dateList_len) + " [" + dateMin + " - " + dateMax + "]."
                        #    writeToLogFile(msg, "Result", "DEMANDID " + str(demandGroup_ids))
                        #    print("[DEMANDID " + str(demandGroup_ids) + "] " + msg)
                        else:
                            #latestReturnedDate = pd.to_datetime([*result]).max()
                            # Open database:
                            dbconn = sqlite3.connect(input_path)
                            dbcur = dbconn.cursor()
                            lastTimeSeriesId = pd.read_sql_query("SELECT MAX(TIMESERIESID) AS LASTID FROM TIMESERIES", dbconn)["LASTID"].to_list()
                            if lastTimeSeriesId[0] is None:
                                lastTimeSeriesId = 0
                            else:
                                lastTimeSeriesId = int(lastTimeSeriesId[0])
                            timeSeriesBenchmark = lastTimeSeriesId
                            for demandGroup_id in demandGroup_ids:
                                if not (result == {} and (pd.to_datetime(dateMin) > latestDateRecord[demandGroup_id] or pd.to_datetime(dateMax) < earliestDateRecord[demandGroup_id] or latestDateRecord[demandGroup_id] == pd.Timestamp(startDate))):
                                    for date in dateSublist:
                                        currentDate = pd.to_datetime(date)
                                        # Insert a new record in TIMESERIES or get the record id of the record to be overwritten.
                                        # A new record is inserted even if there is no data for the current date, as long as it is not in the end of the series. Doing so avoids a new data search for such date in the future.
                                        sameDate_rows = which((timeSeriesDF["TIMESERIESDATE"] == date) & (timeSeriesDF["TIMESERIESDEMANDID"] == demandGroup_id))
                                        if len(sameDate_rows) == 0 and (date in [*result] or (currentDate > earliestDateRecord[demandGroup_id] and currentDate < latestDateRecord[demandGroup_id])):
                                            lastTimeSeriesId = lastTimeSeriesId + 1
                                            currTimeSeriesId = lastTimeSeriesId
                                            dbcur.execute("INSERT INTO TIMESERIES (TIMESERIESID, TIMESERIESDEMANDID, TIMESERIESDATE, TIMESERIESPROCESSDATE) VALUES (?, ?, ?, DATE('now','localtime'))", (currTimeSeriesId, demandGroup_id, date))
                                            timeSeriesDF = timeSeriesDF.append({"TIMESERIESID": currTimeSeriesId, "TIMESERIESDEMANDID": demandGroup_id, "TIMESERIESDATE": date}, ignore_index=True)
                                        # Or, if a time series record already exists for the current date, get the id and delete the associated data recoreds.
                                        elif len(sameDate_rows) > 0:
                                            currTimeSeriesId = timeSeriesDF["TIMESERIESID"][sameDate_rows].to_list()[-1]
                                            if demandRunningMode == 4:
                                                dbcur.execute("DELETE FROM DATA WHERE DATATIMESERIESID = ?;", (str(currTimeSeriesId),))
                                            
                                        # If there is any data for the current date, update the database.
                                        if date in [*result]:
                                            # Get the data time, if available.
                                            if "img_time" in [*result[date]]:
                                                imgTime = result[date]["img_time"]
                                                result[date].pop("img_time")
                                            else:
                                                imgTime = "00:00"
                                            # Save the variables' values one by one.
                                            for var, value in result[date].items():
                                                # Identify the variable name and the statistical parameter (reducer) applied.
                                                if var in export_vars:
                                                    suffix = ""
                                                    stats = "none"
                                                    varName = var
                                                else:
                                                    suffix = var.split("_")[-1]
                                                    if suffix == var:
                                                        suffix = ""
                                                        stats = "none"
                                                        varName = var
                                                    else:
                                                        stats = suffix
                                                        varName = var[:var.index("_" + suffix)]                                    
                                                # Get the statistics ID. If necessary, insert a new record to STATISTICS.
                                                stats_rows = which(statsDF["STATSNAME"] == stats)
                                                if len(stats_rows) == 0:
                                                    currId = pd.read_sql_query("SELECT MAX(STATSID) AS LASTID FROM STATISTICS", dbconn)["LASTID"].to_list()
                                                    if currId[0] is None:
                                                        statsId = 1
                                                    else:
                                                        statsId = currId[0] + 1
                                                    dbcur.execute("INSERT INTO STATISTICS (STATSID, STATSNAME) VALUES (?, ?)", (statsId, stats))
                                                    statsDF = statsDF.append({"STATSID": statsId, "STATSNAME": stats}, ignore_index=True)
                                                    writeToLogFile("Statistics '" + stats + "' added to the table VARIABLES.", "Info", "DEMANDID " + str(demandGroup_id))
                                                else:
                                                    statsId = statsDF["STATSID"][stats_rows].to_list()[-1]
                                                # Get the variable ID. If necessary, insert a new record to VARIABLES.
                                                var_rows = which(varDF["VARNAME"] == varName)
                                                if len(var_rows) == 0:
                                                    currId = pd.read_sql_query("SELECT MAX(VARID) AS LASTID FROM VARIABLES", dbconn)["LASTID"].to_list()
                                                    if currId[0] is None:
                                                        varId = 1
                                                    else:
                                                        varId = currId[0] + 1
                                                    dbcur.execute("INSERT INTO VARIABLES (VARID, VARNAME) VALUES (?, ?)", (varId, varName))
                                                    varDF = varDF.append({"VARID": varId, "VARNAME": varName}, ignore_index=True)
                                                    writeToLogFile("Variable '" + varName + "' added to the table VARIABLES.", "Info", "DEMANDID " + str(demandGroup_id))
                                                else:
                                                    varId = varDF["VARID"][var_rows].to_list()[-1]
                                                # Insert data records.
                                                values = (currTimeSeriesId, varId, statsId, imgTime, value)
                                                dbcur.execute("INSERT INTO DATA (DATATIMESERIESID, DATAVARID, DATASTATSID, DATATIME, DATAVALUE) VALUES (?, ?, ?, ?, ?);", values)
                                        # Perform, at once, all the changes related to the current date.
                                        dbconn.commit()
                                if result == {}:
                                    msg = "There is no available data for dates " + str(dateSublist[0]) + " to " + str(dateSublist[-1]) + "."
                                    if lastTimeSeriesId > timeSeriesBenchmark:
                                        msg = msg + "The TIMESERIES table was updated, though, to fill the date gaps."
                                else:
                                    msg = "Saved the records for dates " + str(dateSublist[0]) + " to " + str(dateSublist[-1]) + "."
                                writeToLogFile(msg, "Result", "DEMANDID " + str(demandGroup_id))
                                print("[DEMANDID " + str(demandGroup_id) + "] " + msg)
                                writeToLogFile(msg, "Result", "DEMANDID " + str(demandGroup_id))
                            dbcur.close()
                            dbconn.close()
            elif demandRunningMode == 5:
                dbconn = sqlite3.connect(input_path)
                dbcur = dbconn.cursor()
                varName = estimation_algo_specs[demandEstimAlgoId]["paramName"]
                if not varName == "":
                    # Get the variable ID. If necessary, insert a new record to VARIABLES.
                    var_rows = which(varDF["VARNAME"] == varName)
                    if len(var_rows) == 0:
                        currId = pd.read_sql_query("SELECT MAX(VARID) AS LASTID FROM VARIABLES", dbconn)["LASTID"].to_list()
                        if len(currId) == 0:
                            varId = 1
                        else:
                            varId = currId[0] + 1
                        dbcur.execute("INSERT INTO VARIABLES (VARID, VARNAME) VALUES (?, ?)", (varId, varName))
                        varDF = varDF.append({"VARID": varId, "VARNAME": varName}, ignore_index=True)
                        writeToLogFile("Variable '" + varName + "' added to the table VARIABLES.", "Info", "DEMANDID " + str(demandId))
                    else:
                        varId = varDF["VARID"][var_rows].to_list()[-1]
                    # Get the required bands to make the estimation.
                    bands = getSpectralBands(demandProdId)
                    requiredBands = [bands[k] for k in estimation_algo_specs[demandEstimAlgoId]["requiredBands"]]
                    if not len(requiredBands) == 0:
                        print("[DEMANDID " + str(demandId) + "] Trying to apply the estimation algorithm #" + str(demandEstimAlgoId) + " to the satellite data already in the database.")
                        # Build query.
                        querystr = "SELECT TIMESERIESID, TIMESERIESDATE, DATATIME, STATSID, "
                        for band in requiredBands:
                            querystr = querystr + "MAX(CASE WHEN VARNAME='" + band + "' THEN DATAVALUE END) " + band + ", "
                        querystr = querystr + "MAX(CASE WHEN VARNAME='" + varName + "' THEN DATAVALUE END) " + varName \
                            + " FROM DATA LEFT JOIN VARIABLES ON DATAVARID = VARID LEFT JOIN STATISTICS ON DATASTATSID = STATSID LEFT JOIN TIMESERIES ON DATATIMESERIESID = TIMESERIESID LEFT JOIN DEMANDS ON TIMESERIESDEMANDID = DEMANDID WHERE DEMANDID = " + str(demandId) + " AND STATSNAME IN ('mean', 'median', 'min', 'max') GROUP BY TIMESERIESID, STATSID;"
                        # Get data.
                        varDataDF = pd.read_sql_query(querystr, dbconn)
                        nrows = varDataDF.shape[0]
                        if nrows > 0:
                            # Fake image collection.
                            productBands = list({*bands.values()})
                            aoi = ee.Geometry.Point([0, 0]).buffer(1)
                            eeImgList = []
                            for row_j in range(nrows):
                                image = ee.Image(varDataDF[requiredBands[0]][row_j]).rename(requiredBands[0])
                                for band in requiredBands[1:]:
                                    image = image.addBands(ee.Image(varDataDF[band][row_j]).rename(band))
                                for band in productBands:
                                    if not band in requiredBands:
                                        image = image.addBands(ee.Image(0).rename(band))
                                eeImgList.append(image.clip(aoi).set("img_date", varDataDF["TIMESERIESDATE"][row_j], "img_time", varDataDF["DATATIME"][row_j]))
                            image_collection = ee.ImageCollection(eeImgList).set("product_id", demandProdId, "export_vars", [], "export_bands", [])
                            # Apply prediction algorithm.
                            estimation(demandEstimAlgoId, demandProdId)
                            # Get the result dictionary.
                            def reduce(image, result):
                                return ee.Dictionary(result).set(ee.Image(image).get("img_date"), ee.Image(image).reduceRegion(reducer = ee.Reducer.first(), geometry = aoi, scale = 1, bestEffort = True))
                            first = ee.Dictionary()
                            result = ee.Dictionary(ee.ImageCollection(image_collection).iterate(reduce, first)).getInfo()
                            # Insert the result values into the DATA table.
                            for row_j in range(nrows):
                                recordDate = str(varDataDF["TIMESERIESDATE"][row_j])
                                timeSeriesId = int(varDataDF["TIMESERIESID"][row_j])
                                statsId = int(varDataDF["STATSID"][row_j])
                                dataTime = str(varDataDF["DATATIME"][row_j])
                                dataValue = result[recordDate][varName]
                                if varDataDF[varName][row_j] is None:
                                    dbcur.execute("INSERT INTO DATA (DATATIMESERIESID, DATAVARID, DATASTATSID, DATATIME, DATAVALUE) VALUES (?, ?, ?, ?, ?)", (timeSeriesId, varId, statsId, dataTime, dataValue))
                                else:
                                    #dataId = int(varDataDF["DATAID"][row_j])
                                    dbcur.execute("UPDATE DATA SET DATAVALUE = ? WHERE DATATIMESERIESIDID = ? AND DATAVARID = ? AND DATASTATSID = ?", (dataValue, timeSeriesId, varId, statsId))
                            msg = "Estimation algorithm successfully applied."
                            print("[DEMANDID " + str(demandId) + "] " + msg + "\n")
                            writeToLogFile(msg, "Result", "DEMANDID " + str(demandId))
                        dbconn.commit()
                    else:
                        msg = "The required bands for the estimation algorithm #" + str(demandEstimAlgoId) + " were not defined."
                        writeToLogFile(msg, "Warning", "DEMANDID " + str(demandId))
                        print("(!) " + msg)                
                else:
                    msg = "The estimation algorithm #" + str(demandEstimAlgoId) + " yields no estimated variable. Demand ignored."
                    writeToLogFile(msg, "Warning", "DEMANDID " + str(demandId))
                    print("(!) " + msg)
                dbcur.close()
                dbconn.close()
    writeToLogFile("Finishing 'database mode'.", "Benchmark", "-")
    # Warn about errors.
    if anyError:
        print("")
        print("(!) One or more errors ocurred. Check the log file (" + log_file + ").")
    # Close the database connection before leaving, if still open.
    if dbconn:
        dbconn.close()
    # Goodbye:
    print("-")
    print("Processing finished.")

def createGEEDaRdb(input_path):
    try:
        dbconn = sqlite3.connect(input_path)
    except:
        pass
    if not os.path.isfile(input_path):
        return -1
    try:
        dbcur = dbconn.cursor()
        querystr = """
            CREATE TABLE APPLICATION (
                ATTRIBUTE TEXT UNIQUE
                               NOT NULL,
                VALUE     TEXT
            );
            
            INSERT INTO APPLICATION (ATTRIBUTE, VALUE)
            VALUES
                ('NAME', 'GEEDaR'),
                ('VERSION', '0.00'),
                ('AUTHOR', 'Dhalton Ventura'),
                ('RUNCOUNT', 0),
            	('KMLSUBDIR', 'KML'),
            	('LOGFILE', 'GEEDaR_log.txt');
            
            CREATE TABLE STATIONS (
                STID         INTEGER   PRIMARY KEY ASC AUTOINCREMENT
                                       UNIQUE
                                       NOT NULL,
                STCOD        TEXT (13),
                STNAME       TEXT (32),
                STLAT        REAL,
                STLONG       REAL,
                STLOCATION   TEXT (255),
                STVIS        INTEGER   NOT NULL
                                       DEFAULT (1) 
            );
            
            INSERT INTO STATIONS (STID, STCOD, STLAT, STLONG, STNAME, STVIS)
            VALUES
                (0, '1547S04749W0', -15.787233, -47.81478, 'Test Site', 0);
            
            CREATE TABLE STATISTICS (
                STATSID   INTEGER   PRIMARY KEY ASC AUTOINCREMENT
                                    UNIQUE
                                    NOT NULL,
                STATSNAME TEXT (16) 
            );
            
            INSERT INTO STATISTICS (STATSID, STATSNAME)
            VALUES
            	(0, 'none'),
                (1, 'median'),
                (2, 'mean'),
                (3, 'stdDev'),
                (4, 'min'),
                (5, 'max'),
                (6, 'count'),
                (7, 'sum');
            
            CREATE TABLE VARIABLES (
                VARID    INTEGER   PRIMARY KEY ASC AUTOINCREMENT
                                   UNIQUE
                                   NOT NULL,
                VARNAME  TEXT (32) UNIQUE,
                VARDESCR TEXT (64) 
            );
            CREATE TABLE PRODUCTS (
                PRODUCTID     INTEGER PRIMARY KEY ASC
                                      UNIQUE
                                      NOT NULL,
                PRODUCTNAME   TEXT,
                PRODUCTSENSOR TEXT,
                PRODUCTDESCR  TEXT
            );
            CREATE TABLE PROCALGOS (
                PROCALGOID    INTEGER    PRIMARY KEY ASC
                                         UNIQUE
                                         NOT NULL,
                PROCALGONAME  TEXT (32),
                PROCALGODESCR TEXT (256),
                PROCALGOREF   TEXT (256) 
            );
            CREATE TABLE ESTIMALGOS (
                ESTIMALGOID    INTEGER PRIMARY KEY ASC
                                       UNIQUE
                                       NOT NULL,
                ESTIMALGONAME  TEXT,
                ESTIMALGODESCR TEXT,
                ESTIMALGOMODEL TEXT,
                ESTIMALGOREF   TEXT,
                ESTIMALGOPARAM TEXT    DEFAULT ('') 
                                       NOT NULL
            );
            CREATE TABLE REDUCERS (
                REDUCERID    INTEGER   PRIMARY KEY ASC
                                       UNIQUE
                                       NOT NULL,
                REDUCERDESCR TEXT (16) 
            );
            CREATE TABLE DEMANDS (
                DEMANDID          INTEGER     PRIMARY KEY ASC AUTOINCREMENT
                                              UNIQUE
                                              NOT NULL,
                DEMANDSTATUS      INTEGER (1) DEFAULT (1) 
                                              NOT NULL,
                DEMANDSTID        INTEGER     REFERENCES STATIONS (STID)    ON DELETE CASCADE
                                                                            ON UPDATE CASCADE
                                              NOT NULL,
                DEMANDPRODUCTID   INTEGER (2) NOT NULL
                                              REFERENCES PRODUCTS (PRODUCTID),
                DEMANDPROCALGOID  INTEGER (1) NOT NULL
                                              REFERENCES PROCALGOS (PROCALGOID),
                DEMANDESTIMALGOID INTEGER (1) NOT NULL
                                              REFERENCES ESTIMALGOS (ESTIMALGOID),
                DEMANDREDUCID     INTEGER (1) NOT NULL
                                              REFERENCES REDUCERS (REDUCERID),
                DEMANDSTARTDATE   TEXT (5)    DEFAULT ('auto'),                                              
                DEMANDENDDATE     TEXT (5),
                DEMANDAOIMODE     INTEGER     DEFAULT (0) 
                                              NOT NULL,
                DEMANDAOIRADIUS   INTEGER,
                DEMANDAOIKMLFILE  TEXT        DEFAULT ('auto') 
            );
            
            INSERT INTO DEMANDS (DEMANDSTATUS, DEMANDSTID, DEMANDPRODUCTID, DEMANDPROCALGOID, DEMANDESTIMALGOID, DEMANDREDUCID, DEMANDSTARTDATE, DEMANDENDDATE, DEMANDAOIMODE, DEMANDAOIRADIUS)
            VALUES
                (1, 0, 105, 3, 0, 3, '2002-01-01', '2002-01-10', 0, 750);
            
            CREATE TABLE TIMESERIES (
                TIMESERIESID            INTEGER   PRIMARY KEY ASC AUTOINCREMENT
                                             UNIQUE
                                             NOT NULL,
                TIMESERIESDEMANDID      INTEGER   REFERENCES DEMANDS (DEMANDID)  ON DELETE CASCADE
                                                                            ON UPDATE CASCADE,
                TIMESERIESDATE          TEXT (10),
                TIMESERIESPROCESSDATE   TEXT (10)
            );
            
            CREATE TABLE DATA (
                DATATIMESERIESID INTEGER  REFERENCES TIMESERIES (TIMESERIESID)  ON DELETE CASCADE
                                                                                ON UPDATE CASCADE,
                DATAVARID        INTEGER  REFERENCES VARIABLES (VARID)  ON DELETE CASCADE
                                                                        ON UPDATE CASCADE,
                DATASTATSID      INTEGER  REFERENCES STATISTICS (STATSID)   ON DELETE CASCADE
                                                                            ON UPDATE CASCADE,
                DATATIME         TEXT (5),
                DATAVALUE        REAL,
                DATASTATUS       INTEGER  NOT NULL
                                          DEFAULT (1)
            );
            
            CREATE VIEW view_demands AS
                SELECT 
                    DEMANDID AS id,
                    DEMANDSTATUS AS status,
                    STCOD AS stCode,
                    STNAME AS stName,  
                    PRODUCTSENSOR AS sensor,
                    DEMANDPRODUCTID AS prodId,
                    PROCALGONAME AS procAlgo,
                    ESTIMALGONAME AS estimAgo,
                    REDUCERDESCR AS reducer,
                    DEMANDSTARTDATE AS startDate,
                    DEMANDENDDATE AS endDate,
                    DEMANDAOIMODE AS aoiMode,
                    DEMANDAOIRADIUS AS radius,
                    DEMANDAOIKMLFILE AS kml
                FROM
                    DEMANDS
                    LEFT JOIN STATIONS ON DEMANDSTID = STID
                    LEFT JOIN PRODUCTS ON DEMANDPRODUCTID = PRODUCTID
                    LEFT JOIN PROCALGOS ON DEMANDPROCALGOID = PROCALGOID
                    LEFT JOIN ESTIMALGOS ON DEMANDESTIMALGOID = ESTIMALGOID
                    LEFT JOIN REDUCERS ON DEMANDREDUCID = REDUCERID;
            		   
            CREATE VIEW view_vardata AS
                SELECT TIMESERIESID,
                       DEMANDID,
                       STID,
                       STCOD,
                       STNAME AS STATION,
                       TIMESERIESDATE AS DATE,
                       DATATIME AS TIME,
                       DATAVALUE AS VALUE,
                       STATSNAME AS STATS,
                       VARNAME AS VARIABLE,
                       PRODUCTSENSOR AS SENSOR
                  FROM DATA
                       LEFT JOIN
                       VARIABLES ON DATAVARID = VARID
                       LEFT JOIN
                       STATISTICS ON DATASTATSID = STATSID
                       LEFT JOIN
                       TIMESERIES ON DATATIMESERIESID = TIMESERIESID
                       LEFT JOIN
                       DEMANDS ON TIMESERIESDEMANDID = DEMANDID
                       LEFT JOIN
                       STATIONS ON DEMANDSTID = STID
                       LEFT JOIN
                       PRODUCTS ON DEMANDPRODUCTID = PRODUCTID;
            
        """
        dbcur.executescript(querystr)
        dbconn.commit()
        dbcur.close()
        dbconn.close()
        # Now populate the special tables (products, algorithms, reducers):
        result = updateGEEDaRtables()
    except Exception as e:
        print(e)
        return -1
    return result
        
    
#%% Initialization

# Change the working dir to the script's dir.
os.chdir(os.path.realpath(sys.path[0]))

# Update the 'run parameters' with the passed command-line arguments and verify their validity:
arg_list = sys.argv

## Only the script name was passed, hence the path of the input file was not.
if len(arg_list) < 2:
    print("!")
    raise Exception("The input (CSV) file path was not provided.")  

## The user needs help.
if len(arg_list) == 2 and (arg_list[1] in ["?", "-?", "-h", "-help", "--help", "help"]):
    print("Sorry, no help available yet.")
    quit()

## Check the arguments out.
input_passed = False
for arg in arg_list[1:]:
    # If there is no leading hyphen, it is treated as the input_file parameter.
    if not arg[0] == "-":
        if not input_passed:
            arg = "-i:" + arg
            input_passed = True
        else:
            print("!")
            raise Exception("Unrecognized or incomplete command-line argument: '" + arg + "'. Do not forget the hyphen and do not use spaces. Right forms: -i:input_file.csv; -i:'input file.csv'. Wrong: -i: input_file.csv; -i:input file.csv.")
    if arg[0] == "-" and len(arg) == 2:
        arg = arg + ":True"
    if arg[0] == "-" and arg[2] == ":":
        if len(arg) < 4:
            print("!")
            raise Exception("Unrecognized or incomplete command-line argument: '" + arg + "'. Do not use spaces. Right forms: -i:input_file.csv; -i:'input file.csv'. Wrong: -i: input_file.csv; -i:input file.csv.")
        key = arg[1]
        if not key in run_par:
            print("!")
            raise Exception("Unrecognized identifier '" + key + "' in '" + arg + "'.")
        run_par[key] = arg[3:]
        if key == "i":
            input_passed = True
    else:
        print("!")
        raise Exception("Unrecognized or incomplete command-line argument: '" + arg + "'. Do not use spaces. Right forms: -i:input_file.csv; -i:'input file.csv'. Wrong: -i: input_file.csv; -i:input file.csv.")

### Help?
if run_par["h"] != "":
    print("Sorry, no help available yet.")
    quit()

### Check the input file path:
input_path = run_par["i"]
if input_path == "":
    print("!")
    raise Exception("The input file path was not provided.")  
try:
    splittedPath = os.path.split(input_path)
    input_dir = splittedPath[0]
    if input_dir == "":
        input_dir = "./"
    input_file = splittedPath[1]
except:
    print("!")
    raise Exception("Unrecognized input file path: '" + input_path + "'.")  

### Determine the running mode.
if run_par["m"] == "":
    if (not input_path[-3:] == ".db") and (not input_path[-4:] == ".csv") and (not input_path[-4:] == ".kml"):
        print("!")
        raise Exception("For the running mode to be automatically determined, the input file's extension must be '.csv' (running mode 1 or 2), '.kml' (running mode 2) or '.db' (running mode 3).")
    if input_path[-3:] == ".db":
        running_mode = 3
    else:
        # Set running_mode to 0 and determine it later in the function 'loadInputDF', where it will be set to 1 or 2, depending on the CSV's columns.
        running_mode = 0
else:
    try:
        running_mode = int(run_par["m"])
    except:
        print("!")
        raise Exception("Running mode must be an integer. Available modes: ".join(running_modes))
    else: 
        if not running_mode in range(1, len(running_modes) + 1):
            print("!")
            raise Exception("Unrecognized running mode: '" + str(running_mode) + "'. Available modes: ".join(running_modes))

if running_mode < 3:
    # Confirm the existence of the input file.
    if not os.path.isfile(input_path) and input_file != "*.kml":
        print("!")
        raise Exception("File not found: '" + input_path + "'.")

    ## Unfold the processing code into the IDs of the product, the image processing algorithm, estimation algorithm and reducer.
    processing_codes, product_ids, img_proc_algos, estimation_algos, reducers = unfoldProcessingCode(run_par["c"])
    nProcCodes = len(processing_codes)

    output_path = run_par["o"]
    if output_path == "":
        output_dir = input_dir
        if input_file == "*.kml":
            output_file = "kml_result.csv"
        else:
            output_file = input_file[:-4] + "_result.csv"
        output_path = os.path.join(output_dir, output_file)
    else:
        try:
            splittedPath = os.path.split(output_path)
            output_dir = splittedPath[0]
            output_file = splittedPath[1]
        except:
            print("!")
            raise Exception("Unrecognized output file path: '" + output_path + "'.")  
        if output_dir == "":
            output_dir = input_dir #"./"
            output_path = os.path.join(output_dir, output_file)
        elif not os.path.exists(output_dir):
            print("!")
            raise Exception("Directory not found: '" + output_dir + "'.")
    # Check for preexisting file:
    if os.path.isfile(output_path):
        copyfile(output_path, output_path + ".bkp")
        print("(!) Output file already existed, so a backup was created: '" + output_file + ".bkp'.")
    
    ## Area of Interest (AOI) method:
    if run_par["r"] != "":
        try:
            aoi_radius = int(run_par["r"])
        except:
            print("!")
            raise Exception("The 'aoi_radius' must be a number greater than zero.")
        if aoi_radius <= 0:
            print("!")
            raise Exception("The 'aoi_radius' must be a number greater than zero.")
    elif not run_par["k"] in ["", "False", "0"]:
        aoi_mode = "kml" 
    
    ## Time window parameter:
    try:
        time_window = int(run_par["t"])
    except:
        print("!")
        raise Exception("The 'time window' must be an integer greater or equal to zero.")
    if time_window < 0:
        print("!")
        raise Exception("The 'time window' must be an integer greater or equal to zero.")
    
    ## Append mode:
    if not run_par["a"] in ["", "False", "0"]:
        append_mode = True
    
elif running_mode >= 3:
    # If a database file does not exist, create one and inform the need to add demand records into the table 'DEMANDS'.
    if not os.path.isfile(input_path):
        #createGeedarDb(input_path)
        result = createGEEDaRdb(input_path)        
        if result == 0:
            print("The file '" + input_path + "' did not exist, so an SQLite3 database with the same name was created and filled with the GEEDaR standard tables.")
            print("Before being able to run GEEDaR in a database mode (running_mode 3, 4 or 5), you must fill the tables 'STATIONS' and 'DEMANDS'.")
        else:
            print("(!) The file '" + input_path + "' was not found and an attempt to create a database with the same name failed.")
        sys.exit(1)
    

#%% Main

### TEMP ####
"""
# Modes 1 or 2
os.chdir(r"D:\OneDrive - Agência Nacional de Águas\_Server\GEEDaR")
input_dir = "D:/Agência Nacional de Águas/Hidrologia Espacial - Documentos/Profissionais/Dhalton/Apoio/Doce/Calib_DadosCompilados/"
input_file = "DadosCompiladosAdaptados.csv"
input_path = input_dir + input_file
output_path = input_dir + "result_" + input_file
aoi_mode = "kml"
aoi_radius = 1000
running_mode = 0
processing_codes, product_ids, img_proc_algos, estimation_algos, reducers = unfoldProcessingCode(
    "[30109001,30209001,30309001,30112001,30212001,30312001]"
    )
nProcCodes = len(processing_codes)
time_window = 2
append_mode = True
date_col = 1; id_col = 3; lat_col = 7; long_col = 8

# Mode 3
os.chdir("D:\Agência Nacional de Águas\Hidrologia Espacial - Documentos\Profissionais\Dhalton\GEEDaR")
input_dir = "D:/Agência Nacional de Águas/Hidrologia Espacial - Documentos/Projetos/Brumadinho/NT/"
input_file = "series.db"
input_path = input_dir + input_file
running_mode = 3
"""

# Retrieve data according to the running mode:
if running_mode < 3:
    loadInputDF()
    resultDF = specificDatesRetrieval()
    if resultDF is None:
        print("No results to be saved.")
    else:
        # Save results.
        print("Saving...")
        try:
            resultDF.to_csv(output_path, index = False)
        except Exception as e:
            print("(!) Failed to save the results to '" + output_path + "'.")
            print(e)
        else:
            print("Results saved to file '" + output_path + "'.")
elif running_mode in [3,4,5]:
    databaseUpdate()