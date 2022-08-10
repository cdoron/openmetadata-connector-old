package database_types

import (
	"reflect"

	models "github.com/fybrik/datacatalog-go-models"
	utils "github.com/fybrik/openmetadata-connector/utils"
	vault "github.com/fybrik/openmetadata-connector/vault"
)

type s3 struct {
	Translate                map[string]string
	TranslateInv             map[string]string
	VaultClientConfiguration map[interface{}]interface{}
}

func NewS3(vaultClientConfiguration map[interface{}]interface{}) *s3 {
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
	return &s3{Translate: translate, TranslateInv: translateInv, VaultClientConfiguration: vaultClientConfiguration}
}

func getS3Credentials(vaultClientConfiguration map[interface{}]interface{}, credentialsPath *string) (string, string) {
	client := vault.NewVaultClient(vaultClientConfiguration)
	token, err := client.GetToken()
	if err != nil {
		return "", ""
	}
	secret, err := client.GetSecret(token, *credentialsPath)
	if err != nil {
		return "", ""
	}
	return vault.ExtractS3CredentialsFromSecret(secret)
}

func (m *s3) TranslateFybrikConfigToOpenMetadataConfig(config map[string]interface{}, credentialsPath *string) map[string]interface{} {
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

	if m.VaultClientConfiguration != nil && credentialsPath != nil {
		awsAccessKeyId, awsSecretAccessKey := getS3Credentials(m.VaultClientConfiguration, credentialsPath)
		if awsAccessKeyId != "" && awsSecretAccessKey != "" {
			securityMap["awsAccessKeyId"] = awsAccessKeyId
			securityMap["awsSecretAccessKey"] = awsSecretAccessKey
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
		objectKey, found := connectionProperties["object_key"]
		if found {
			return utils.AppendStrings(serviceName+".default."+bucket.(string), objectKey.(string))
		} else {
			return utils.AppendStrings(serviceName+".default."+bucket.(string), assetName)
		}
	} else {
		return serviceName + ".default." + assetName
	}
}

func (m *s3) compareConfigSource(fromService map[string]interface{}, fromRequest map[string]interface{}) bool {
	// ignore some fields, such as 'aws_token' which would appear only serviceSecurityConfig
	serviceSecurityConfig := fromService["securityConfig"].(map[string]interface{})
	requestSecurityConfig := fromRequest["securityConfig"].(map[string]interface{})
	for property, value := range requestSecurityConfig {
		if !reflect.DeepEqual(serviceSecurityConfig[property], value) {
			return false
		}
	}
	return true
}

func (m *s3) CompareServiceConfigurations(requestConfig map[string]interface{}, serviceConfig map[string]interface{}) bool {
	for property, value := range requestConfig {
		if property == "configSource" {
			if !m.compareConfigSource(serviceConfig[property].(map[string]interface{}), value.(map[string]interface{})) {
				return false
			}
		} else {
			if !reflect.DeepEqual(serviceConfig[property], value) {
				return false
			}
		}
	}
	return true
}
