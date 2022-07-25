package main

type s3 struct {
}

func (m *s3) translateFybrikConfigToOpenMetadataConfig(config map[string]interface{}) map[string]interface{} {
	return config
}

func (m *s3) OMTypeName() string {
	return "Datalake"
}
