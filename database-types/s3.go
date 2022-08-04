package database_types

import (
	models "github.com/fybrik/datacatalog-go-models"
	utils "github.com/fybrik/openmetadata-connector/utils"
)

type s3 struct {
	Translate    map[string]string
	TranslateInv map[string]string
}

func NewS3() *s3 {
	translate := map[string]string{
		"region":           "awsRegion",
		"endpoint":         "endPointURL",
		"access_key_id":    "awsAccessKeyId",
		"secret_access_id": "awsSecretAccessKey",
	}
	translateInv := map[string]string{
		"awsRegion":          "region",
		"endPointURL":        "endpoint",
		"awsAccessKeyId":     "access_key_id",
		"awsSecretAccessKey": "secret_access_id",
	}
	return &s3{Translate: translate, TranslateInv: translateInv}
}

func (m *s3) TranslateFybrikConfigToOpenMetadataConfig(config map[string]interface{}) map[string]interface{} {
	ret := make(map[string]interface{})
	configSourceMap := make(map[string]interface{})
	ret["type"] = "Datalake"
	bucketName, found := config["bucket"]
	if found {
		ret["bucketName"] = bucketName
	}

	securityMap := make(map[string]interface{})
	securityMap["awsRegion"] = "eu-de" // awsRegion field is mandatory, although it is persumably ignored if endpoint is provided
	for key, value := range config {
		translation, found := m.Translate[key]
		if found {
			securityMap[translation] = value
		}
	}

	configSourceMap["securityConfig"] = securityMap
	ret["configSource"] = configSourceMap
	return ret
}

func (m *s3) TranslateOpenMetadataConfigToFybrikConfig(config map[string]interface{}) map[string]interface{} {
	ret := make(map[string]interface{})

	securityConfig := config["configSource"].(map[string]interface{})["securityConfig"].(map[string]interface{})

	for key, value := range securityConfig {
		if translation, found := m.TranslateInv[key]; found {
			ret[translation] = value
		}
	}
	if value, found := config["bucketName"]; found {
		ret["bucket"] = value
	}

	return ret
}

func (m *s3) OMTypeName() string {
	return "Datalake"
}

func (m *s3) ConstructFullAssetId(serviceName string, createAssetRequest models.CreateAssetRequest) string {
	connectionProperties := createAssetRequest.Details.GetConnection().AdditionalProperties["s3"].(map[string]interface{})
	assetName := *createAssetRequest.DestinationAssetID
	bucket, found := connectionProperties["bucket"]
	if found {
		return utils.AppendStrings(serviceName+".default."+bucket.(string), assetName)
	} else {
		return serviceName + ".default." + assetName
	}
}
