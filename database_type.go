package main

import models "github.com/fybrik/datacatalog-go-models"

type databaseType interface {
	OMTypeName() string
	translateFybrikConfigToOpenMetadataConfig(map[string]interface{}) map[string]interface{}
	constructFullAssetId(serviceName string, createAssetRequest models.CreateAssetRequest, assetName string) string
}
