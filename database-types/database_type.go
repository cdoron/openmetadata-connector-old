package database_types

import models "github.com/fybrik/datacatalog-go-models"

type DatabaseType interface {
	OMTypeName() string
	TranslateFybrikConfigToOpenMetadataConfig(map[string]interface{}, *string) map[string]interface{}
	TranslateOpenMetadataConfigToFybrikConfig(map[string]interface{}) map[string]interface{}
	ConstructFullAssetId(serviceName string, createAssetRequest models.CreateAssetRequest) string
}
