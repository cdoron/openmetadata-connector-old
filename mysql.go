package main

import models "github.com/fybrik/datacatalog-go-models"

type mysql struct {
	StandardFields map[string]bool
}

func NewMysql() *mysql {
	standardFields := map[string]bool{
		"databaseSchema": true,
		"hostPort":       true,
		"password":       true,
		"scheme":         true,
		"username":       true,
	}
	return &mysql{StandardFields: standardFields}
}

func (m *mysql) translateFybrikConfigToOpenMetadataConfig(config map[string]interface{}) map[string]interface{} {
	return config
}

func (m *mysql) translateOpenMetadataConfigToFybrikConfig(config map[string]interface{}) map[string]interface{} {
	other := make(map[string]interface{})
	ret := make(map[string]interface{})
	for key, value := range config {
		if _, ok := m.StandardFields[key]; ok {
			ret[key] = value
		} else {
			other[key] = value
		}
	}
	if other != nil {
		ret["other"] = other
	}
	return ret
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
