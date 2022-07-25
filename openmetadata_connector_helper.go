package main

import (
	"context"
	"fmt"
	"os"

	client "github.com/fybrik/datacatalog-go-client"
	models "github.com/fybrik/datacatalog-go-models"
)

func (s *OpenMetadataApiService) findService(ctx context.Context,
	c *client.APIClient,
	createAssetRequest models.CreateAssetRequest, connectionName string) bool {
	connectionProperties := createAssetRequest.Details.GetConnection().AdditionalProperties[connectionName].(map[string]interface{})
	// Let us begin with checking whether the database service already exists
	var foundService client.DatabaseService

	serviceList, _, err := c.DatabaseServiceApi.ListDatabaseServices(ctx).Execute()
	if err != nil {
		fmt.Fprintln(os.Stderr, "Service does not exist yet")
		return false
	}
	for _, service := range serviceList.Data {
		found := true
		// XXXX - Check type of service (for instance "mysql")
		for property, value := range connectionProperties {
			if service.Connection.Config[property] != value {
				found = false
				break
			}
		}
		if found {
			foundService = service
			break
		}
	}
	fmt.Fprintln(os.Stderr, foundService)
	return true
}
