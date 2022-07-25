package main

type mysql struct {
}

func (m *mysql) translateFybrikConfigToOpenMetadataConfig(config map[string]interface{}) map[string]interface{} {
	return config
}

func (m *mysql) OMTypeName() string {
	return "Mysql"
}
