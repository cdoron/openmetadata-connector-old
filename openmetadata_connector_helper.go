package main

import (
	"context"
	"fmt"
	"net/http"
	"os"
	"reflect"
	"time"

	client "github.com/fybrik/datacatalog-go-client"
	models "github.com/fybrik/datacatalog-go-models"
)

func (s *OpenMetadataApiService) findService(ctx context.Context,
	c *client.APIClient,
	connectionProperties map[string]interface{}, connectionName string) (string, string, bool) {

	serviceList, _, err := c.DatabaseServiceApi.ListDatabaseServices(ctx).Execute()
	if err != nil {
		fmt.Fprintln(os.Stderr, "Service does not exist yet")
		return "", "", false
	}
	for _, service := range serviceList.Data {
		found := true
		// XXXX - Check type of service (for instance "mysql")
		for property, value := range connectionProperties {
			if !reflect.DeepEqual(service.Connection.Config[property], value) {
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
	OMConfig map[string]interface{},
	OMTypeName string) (string, string, error) {
	connection := client.NewDatabaseConnection()

	connection.SetConfig(OMConfig)
	createDatabaseService := client.NewCreateDatabaseService(*connection, createAssetRequest.DestinationCatalogID+"-"+connectionName,
		OMTypeName)

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
	fmt.Println("Too many retries. Could not find table " + name + ". Giving up")
	return false, nil
}

func (s *OpenMetadataApiService) findAsset(ctx context.Context, c *client.APIClient, assetId string) bool {
	_, r, err := c.TablesApi.GetTableByFQN(ctx, assetId).Execute()
	if err != nil {
		fmt.Fprintf(os.Stderr, "Error when calling `IngestionPipelinesApi.GetTableByFQN``: %v\n", err)
		fmt.Fprintf(os.Stderr, "Full HTTP response: %v\n", r)
	}
	return err == nil
}

func (s *OpenMetadataApiService) findIngestionPipeline(ctx context.Context, c *client.APIClient, ingestionPipelineName string) (string, bool) {
	pipeline, _, err := c.IngestionPipelinesApi.GetSpecificIngestionPipelineByFQN(ctx, ingestionPipelineName).Execute()
	if err != nil {
		return "", false
	}
	return *pipeline.Id, true
}

func (s *OpenMetadataApiService) createIngestionPipeline(ctx context.Context,
	c *client.APIClient,
	databaseServiceId string,
	ingestionPipelineName string) (string, error) {
	sourceConfig := *client.NewSourceConfig()
	sourceConfig.SetConfig(map[string]interface{}{"type": "DatabaseMetadata"})
	newCreateIngestionPipeline := *client.NewCreateIngestionPipeline(*&client.AirflowConfig{},
		ingestionPipelineName,
		"metadata", *client.NewEntityReference(databaseServiceId, "databaseService"),
		sourceConfig)

	ingestionPipeline, r, err := c.IngestionPipelinesApi.CreateIngestionPipeline(ctx).CreateIngestionPipeline(newCreateIngestionPipeline).Execute()
	if err != nil {
		fmt.Fprintf(os.Stderr, "Error when calling `IngestionPipelinesApi.CreateIngestionPipeline``: %v\n", err)
		fmt.Fprintf(os.Stderr, "Full HTTP response: %v\n", r)
		return "", err
	}
	return *ingestionPipeline.Id, nil
}

func (s *OpenMetadataApiService) deployAndRunIngestionPipeline(ctx context.Context,
	c *client.APIClient,
	ingestionPipelineID string) error {
	// Let us deploy the ingestion pipeline
	_, r, err := c.IngestionPipelinesApi.DeployIngestion(ctx, ingestionPipelineID).Execute()
	if err != nil {
		fmt.Fprintf(os.Stderr, "Error when calling `IngestionPipelinesApi.DeployIngestion``: %v\n", err)
		fmt.Fprintf(os.Stderr, "Full HTTP response: %v\n", r)
		return err
	}

	// Let us trigger a run of the ingestion pipeline
	_, r, err = c.IngestionPipelinesApi.TriggerIngestionPipelineRun(ctx, ingestionPipelineID).Execute()
	if err != nil {
		fmt.Fprintf(os.Stderr, "Error when calling `IngestionPipelinesApi.TriggerIngestion``: %v\n", err)
		fmt.Fprintf(os.Stderr, "Full HTTP response: %v\n", r)
		return err
	}

	return nil
}

func (s *OpenMetadataApiService) enrichAsset(createAssetRequest models.CreateAssetRequest,
	ctx context.Context, table *client.Table, c *client.APIClient) (bool, error) {
	var requestBody []map[string]interface{}

	customProperties := make(map[string]interface{})
	if createAssetRequest.Credentials != nil {
		customProperties["credentials"] = createAssetRequest.Credentials
	}
	if createAssetRequest.ResourceMetadata.Geography != nil {
		customProperties["geography"] = createAssetRequest.ResourceMetadata.Geography
	}
	if createAssetRequest.Details.DataFormat != nil {
		customProperties["dataFormat"] = createAssetRequest.Details.DataFormat
	}
	if createAssetRequest.ResourceMetadata.Name != nil {
		customProperties["name"] = createAssetRequest.ResourceMetadata.Name
	}
	if createAssetRequest.ResourceMetadata.Owner != nil {
		customProperties["owner"] = createAssetRequest.ResourceMetadata.Owner
	}

	init := make(map[string]interface{})
	init["op"] = "add"
	init["path"] = "/extension"
	init["value"] = customProperties
	requestBody = append(requestBody, init)

	var tags []client.TagLabel
	// traverse createAssetRequest.ResourceMetadata.Tags
	// use only the key, ignore the value (assume value is 'true')
	for tagFQN := range createAssetRequest.ResourceMetadata.Tags {
		tags = append(tags, getTag(ctx, c, tagFQN))
	}

	tagsUpdate := make(map[string]interface{})
	tagsUpdate["op"] = "add"
	tagsUpdate["path"] = "/tags"
	tagsUpdate["value"] = tags
	requestBody = append(requestBody, tagsUpdate)

	columns := table.Columns

	for _, col := range createAssetRequest.ResourceMetadata.Columns {
		if len(col.Tags) > 0 {
			columns = tagColumn(ctx, c, columns, col.Name, col.Tags)
		}
	}

	columnUpdate := make(map[string]interface{})
	columnUpdate["op"] = "add"
	columnUpdate["path"] = "/columns"
	columnUpdate["value"] = columns
	requestBody = append(requestBody, columnUpdate)

	resp, err := c.TablesApi.PatchTable(ctx, table.Id).RequestBody(requestBody).Execute()
	if err != nil {
		fmt.Fprintf(os.Stderr, "Error when calling `TablesApi.PatchTable``: %v\n", err)
		fmt.Fprintf(os.Stderr, "Full HTTP response: %v\n", resp)
		return false, err
	}

	return true, nil
}

func (s *OpenMetadataApiService) deleteAsset(ctx context.Context, c *client.APIClient, assetId string) (int, error) {
	table, r, err := c.TablesApi.GetTableByFQN(ctx, assetId).Execute()
	if err != nil {
		fmt.Fprintf(os.Stderr, "Error when calling `TablesApi.GetTableByFQN``: %v\n", err)
		fmt.Fprintf(os.Stderr, "Full HTTP response: %v\n", r)
		return http.StatusNotFound, err
	}

	r, err = c.TablesApi.DeleteTable(ctx, table.Id).Execute()
	if err != nil {
		fmt.Fprintf(os.Stderr, "Error when calling `TablesApi.DeleteTable``: %v\n", err)
		fmt.Fprintf(os.Stderr, "Full HTTP response: %v\n", r)
		return http.StatusBadRequest, err
	}
	return http.StatusOK, nil
}
