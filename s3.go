package main

type s3 struct {
	Translate map[string]string
}

func NewS3() *s3 {
	translate := map[string]string{
		"region":           "awsRegion",
		"endpoint":         "endPointURL",
		"access_key_id":    "awsAccessKeyId",
		"secret_access_id": "awsSecretAccessKey",
	}
	return &s3{Translate: translate}
}

func (m *s3) translateFybrikConfigToOpenMetadataConfig(config map[string]interface{}) map[string]interface{} {
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

func (m *s3) OMTypeName() string {
	return "Datalake"
}