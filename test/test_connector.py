#!/usr/bin/python

import json
import requests

headers =  {"Content-Type":"application/json"}

with open("asset.json", "r") as f:
    data = f.read()

asset_data = json.loads(data)

print("Creating asset")
response = requests.post("http://localhost:8080/createAsset", json=asset_data, headers=headers)

if response.status_code != 200:
    print("error code: " + str(response.status_code))
    print("error message: " + str(response.text))

assert response.status_code == 200

assetID = response.json()["assetID"]
print("Created Asset " + assetID)

print("Let us attempt to create the same asset and make sure we fail")
response = requests.post("http://localhost:8080/createAsset", json=asset_data, headers=headers)

assert response.status_code == 400

print("Now let us read that asset")
data = {"assetID": assetID, "operationType": "read"}
response = requests.post("http://localhost:8080/getAssetInfo", json=data, headers=headers)
assert response.status_code == 200

assert json.loads(response.text) == asset_data

print("Now let us update that asset")
data = {"assetID": assetID, "name": "New Name"}
response = requests.patch("http://localhost:8080/updateAsset", json=data, headers=headers)
assert response.status_code == 200

print("Now let us make sure that the asset has indeed changed")
data = {"assetID": assetID, "operationType": "read"}
response = requests.post("http://localhost:8080/getAssetInfo", json=data, headers=headers)
assert response.status_code == 200

assert json.loads(response.text) != asset_data
asset_data["resourceMetadata"]["name"] = "New Name"
assert json.loads(response.text) == asset_data

print("Cleaning up -- deleting the asset")
data = {"assetID": assetID}
response = requests.delete("http://localhost:8080/deleteAsset", json=data, headers=headers)
assert response.status_code == 200
