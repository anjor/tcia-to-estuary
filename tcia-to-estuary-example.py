# imports

import requests
import pandas as pd
import json
import zipfile
from io import BytesIO
from pestuary import content_add

# set API base URLs

base_url = "https://services.cancerimagingarchive.net/nbia-api/services/v1/"
nlst_url = "https://services.cancerimagingarchive.net/nlst-api/services/v1/"


# define a function to accept a list of seriesInstanceUIDs and download it
# reminder: this only downloads the first 3 scans unless you comment out that section

def downloadSeries(api_url, series_data, csv_filename=""):
    manifestDF = pd.DataFrame()
    count = 0
    for x in series_data:
        seriesUID = x
        data_url = api_url + "getImage?SeriesInstanceUID=" + seriesUID
        print("Downloading " + data_url)
        data = requests.get(data_url)
        file = zipfile.ZipFile(BytesIO(data.content))
        # print(file.namelist())
        file.extractall(path="download/" + "/" + seriesUID)
        # write the series metadata to a dataframe
        metadata_url = api_url + "getSeriesMetaData?SeriesInstanceUID=" + seriesUID
        metadata = requests.get(metadata_url).json()
        df = pd.DataFrame(metadata)
        manifestDF = pd.concat([manifestDF, df])
        # Repeat n times for demo purposes - comment out these next 3 lines to download a full results
        count += 1
        if count == 3:
            break

    manifestDF.to_csv('download/' + csv_filename + '.csv')


# get list of available collections as JSON

data_url = base_url + "getCollectionValues"
data = requests.get(data_url).json()
print(json.dumps(data, indent=2))

collection = "Soft-tissue-Sarcoma"

data_url = base_url + "getSeries?Collection=" + collection
data = requests.get(data_url)
if data.text != "":
    df = pd.DataFrame(data.json())
    series_data = [seriesUID for seriesUID in df['SeriesInstanceUID']]
    print("Collection contains", len(series_data), "Series Instance UIDs (scans).")
else:
    print("Collection not found.")

# feed series_data to our downloadSeries function
downloadSeries(base_url, series_data, collection + "_full_Collection")

# upload data to estuary
responses, collection = content_add("./download", create_collection=True)