package main

import models "github.com/fybrik/datacatalog-go-models"

type mysql struct {
}

func (m *mysql) translateFybrikConfigToOpenMetadataConfig(config map[string]interface{}) map[string]interface{} {
	return config
}

func (m *mysql) OMTypeName() string {
	return "Mysql"
}

func (m *mysql) constructFullAssetId(serviceName string, createAssetRequest models.CreateAssetRequest) string {
	connectionProperties := createAssetRequest.Details.GetConnection().AdditionalProperties["mysql"].(map[string]interface{})
	assetName := *createAssetRequest.DestinationAssetID
	databaseSchema, found := connectionProperties["databaseSchema"]
	if found {
		return serviceName + ".default." + databaseSchema.(string) + "." + assetName
	} else {
		return serviceName + ".default." + assetName
	}
}
