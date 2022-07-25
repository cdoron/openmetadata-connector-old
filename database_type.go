package main

type databaseType interface {
	OMTypeName() string
	translateFybrikConfigToOpenMetadataConfig(map[string]interface{}) map[string]interface{}
}
