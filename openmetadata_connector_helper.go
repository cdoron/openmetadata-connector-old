package main

import (
	"context"
	"fmt"
	"os"
	"time"

	client "github.com/fybrik/datacatalog-go-client"
	models "github.com/fybrik/datacatalog-go-models"
)

func (s *OpenMetadataApiService) findService(ctx context.Context,
	c *client.APIClient,
	createAssetRequest models.CreateAssetRequest, connectionName string) (string, string, bool) {
	connectionProperties := createAssetRequest.Details.GetConnection().AdditionalProperties[connectionName].(map[string]interface{})

	serviceList, _, err := c.DatabaseServiceApi.ListDatabaseServices(ctx).Execute()
	if err != nil {
		fmt.Fprintln(os.Stderr, "Service does not exist yet")
		return "", "", false
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
			return service.Id, *service.FullyQualifiedName, true
		}
	}
	return "", "", false
}

func (s *OpenMetadataApiService) createDatabaseService(ctx context.Context,
	c *client.APIClient,
	createAssetRequest models.CreateAssetRequest,
	connectionName string,
	dt databaseType) (string, string, error) {
	connection := client.NewDatabaseConnection()

	OMConfig := dt.translateFybrikConfigToOpenMetadataConfig(createAssetRequest.Details.GetConnection().AdditionalProperties[connectionName].(map[string]interface{}))

	connection.SetConfig(OMConfig)
	createDatabaseService := client.NewCreateDatabaseService(*connection, createAssetRequest.DestinationCatalogID+"-"+connectionName,
		dt.OMTypeName())

	databaseService, r, err := c.DatabaseServiceApi.CreateDatabaseService(ctx).CreateDatabaseService(*createDatabaseService).Execute()
	if err != nil {
		fmt.Fprintf(os.Stderr, "Error when calling `ServicesApi.CreateDatabaseService``: %v\n", err)
		fmt.Fprintf(os.Stderr, "Full HTTP response: %v\n", r)
		return "", "", err
	}
	return databaseService.Id, *databaseService.FullyQualifiedName, nil
}

func (s *OpenMetadataApiService) waitUntilAssetIsDiscovered(ctx context.Context, c *client.APIClient, name string) (bool, *client.Table) {
	count := 0
	for {
		fmt.Println("running GetTableByFQN")
		table, _, err := c.TablesApi.GetTableByFQN(ctx, name).Execute()
		if err == nil {
			fmt.Println("Found the table!")
			return true, table
		} else {
			fmt.Println("Could not find the table. Let's try again")
		}

		if count == s.NumRetries {
			break
		}
		count++
		time.Sleep(time.Duration(s.SleepIntervalMS) * time.Millisecond)
	}
	fmt.Println("Too many retries. Could not find table. Giving up")
	return false, nil
}

func (s *OpenMetadataApiService) findAsset(ctx context.Context, c *client.APIClient, assetId string) bool {
	_, _, err := c.TablesApi.GetTableByFQN(ctx, assetId).Execute()
	return err == nil
}
