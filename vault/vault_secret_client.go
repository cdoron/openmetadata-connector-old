package vault

import (
	"encoding/json"
	"io/ioutil"
	"net/http"
	"os"
	"strings"
)

type VaultClient struct {
	address       string
	authPath      string
	role          string
	jwt_file_path string
}

func NewVaultClient(conf map[interface{}]interface{}) VaultClient {
	address := "http://vault.fybrik-system:8200"
	authPath := "/v1/auth/kubernetes/login"
	role := "fybrik"
	jwt_file_path := "/var/run/secrets/kubernetes.io/serviceaccount/token"
	if conf != nil {
		if addressConf, ok := conf["address"]; ok {
			address = addressConf.(string)
		}
		if authPathConf, ok := conf["authPath"]; ok {
			authPath = authPathConf.(string)
		}
		if roleConf, ok := conf["role"]; ok {
			role = roleConf.(string)
		}
		if jwtFilePathConf, ok := conf["jwt_file_path"]; ok {
			jwt_file_path = jwtFilePathConf.(string)
		}
	}
	return VaultClient{address: address, authPath: authPath, role: role, jwt_file_path: jwt_file_path}
}

func (v *VaultClient) GetToken() (string, error) {
	jwt, err := os.ReadFile(v.jwt_file_path)
	if err != nil {
		return "", err
	}

	j := make(map[string]string)
	j["jwt"] = string(jwt)
	j["role"] = v.role

	full_auth_path := v.address + v.authPath
	jsonStr, err := json.Marshal(j)
	if err != nil {
		return "", err
	}

	// request token from vault
	requestBody := strings.NewReader(string(jsonStr))
	resp, _ := http.Post(full_auth_path, "encoding/json", requestBody)
	responseBody, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		return "", err
	}

	// parse response
	responseMap := make(map[string]interface{})
	err = json.Unmarshal(responseBody, &responseMap)
	if err != nil {
		return "", err
	}

	token := responseMap["auth"].(map[string]interface{})["client_token"].(string)
	return token, nil
}

func (v *VaultClient) GetSecret(token string, secretPath string) ([]byte, error) {
	req, err := http.NewRequest("GET", v.address+secretPath, nil)
	if err != nil {
		return nil, err
	}

	req.Header.Set("X-Vault-Token", token)
	client := &http.Client{}
	resp, err := client.Do(req)
	if err != nil || resp.StatusCode != http.StatusOK {
		return nil, err
	}

	responseBody, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		return nil, err
	}

	return responseBody, nil
}

func ExtractS3CredentialsFromSecret(secret []byte) (string, string) {
	secretMap := make(map[string]interface{})
	json.Unmarshal(secret, &secretMap)
	data := secretMap["data"].(map[string]interface{})
	return data["access_key"].(string), data["secret_key"].(string)
}
