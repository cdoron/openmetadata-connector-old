package main

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"os"
	"reflect"
	"time"

	client "github.com/fybrik/datacatalog-go-client"
	models "github.com/fybrik/datacatalog-go-models"
	api "github.com/fybrik/datacatalog-go/go"
	utils "github.com/fybrik/openmetadata-connector/utils"
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
		s.logger.Info().Msg(fmt.Sprintf("Error when calling `ServicesApi.CreateDatabaseService``: %v\n", err))
		s.logger.Info().Msg(fmt.Sprintf("Full HTTP response: %v\n", r))
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

func (s *OpenMetadataApiService) findAsset(ctx context.Context, c *client.APIClient, assetId string) (bool, *client.Table) {
	fields := "tags"
	include := "non-deleted"
	table, r, err := c.TablesApi.GetTableByFQN(ctx, assetId).Fields(fields).Include(include).Execute()
	if err != nil {
		s.logger.Info().Msg(fmt.Sprintf("Error when calling `IngestionPipelinesApi.GetTableByFQN``: %v\n", err))
		s.logger.Info().Msg(fmt.Sprintf("Full HTTP response: %v\n", r))
	}
	return err == nil, table
}

func (s *OpenMetadataApiService) findLatestAsset(ctx context.Context, c *client.APIClient, assetId string) (bool, *client.Table) {
	found, table := s.findAsset(ctx, c, assetId)
	if !found {
		return false, nil
	}
	version := fmt.Sprintf("%f", *table.Version)
	table, r, err := c.TablesApi.GetSpecificDatabaseVersion1(ctx, table.Id, version).Execute()
	if err != nil {
		s.logger.Info().Msg(fmt.Sprintf("Error when calling `TablesApi.GetSpecificDatabaseVersion1``: %v\n", err))
		s.logger.Info().Msg(fmt.Sprintf("Full HTTP response: %v\n", r))
		return false, nil
	}
	return true, table
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
		s.logger.Info().Msg(fmt.Sprintf("Error when calling `IngestionPipelinesApi.CreateIngestionPipeline``: %v\n", err))
		s.logger.Info().Msg(fmt.Sprintf("Full HTTP response: %v\n", r))
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
		s.logger.Info().Msg(fmt.Sprintf("Error when calling `IngestionPipelinesApi.DeployIngestion``: %v\n", err))
		s.logger.Info().Msg(fmt.Sprintf("Full HTTP response: %v\n", r))
		return err
	}

	// Let us trigger a run of the ingestion pipeline
	_, r, err = c.IngestionPipelinesApi.TriggerIngestionPipelineRun(ctx, ingestionPipelineID).Execute()
	if err != nil {
		s.logger.Info().Msg(fmt.Sprintf("Error when calling `IngestionPipelinesApi.TriggerIngestion``: %v\n", err))
		s.logger.Info().Msg(fmt.Sprintf("Full HTTP response: %v\n", r))
		return err
	}

	return nil
}

func (s *OpenMetadataApiService) enrichAsset(ctx context.Context, table *client.Table, c *client.APIClient,
	credentials *string, geography *string, name *string, owner *string,
	dataFormat *string,
	requestTags map[string]interface{},
	requestColumnsModels []models.ResourceColumn,
	requestColumnsApi []api.ResourceColumn, connectionType string) error {
	var requestBody []map[string]interface{}

	customProperties := make(map[string]interface{})
	utils.UpdateCustomProperty(customProperties, table.Extension, "credentials", credentials)
	utils.UpdateCustomProperty(customProperties, table.Extension, "geography", geography)
	utils.UpdateCustomProperty(customProperties, table.Extension, "name", name)
	utils.UpdateCustomProperty(customProperties, table.Extension, "owner", owner)
	utils.UpdateCustomProperty(customProperties, table.Extension, "dataFormat", dataFormat)
	utils.UpdateCustomProperty(customProperties, table.Extension, "connectionType", &connectionType)

	init := make(map[string]interface{})
	init["op"] = "add"
	init["path"] = "/extension"
	init["value"] = customProperties
	requestBody = append(requestBody, init)

	if requestTags != nil {
		var tags []client.TagLabel
		// traverse createAssetRequest.ResourceMetadata.Tags
		// use only the key, ignore the value (assume value is 'true')
		for tagFQN := range requestTags {
			tags = append(tags, getTag(ctx, c, tagFQN))
		}

		tagsUpdate := make(map[string]interface{})
		tagsUpdate["op"] = "add"
		tagsUpdate["path"] = "/tags"
		tagsUpdate["value"] = tags
		requestBody = append(requestBody, tagsUpdate)
	}

	if requestColumnsModels != nil || requestColumnsApi != nil {
		columns := table.Columns

		for _, col := range requestColumnsModels {
			if len(col.Tags) > 0 {
				columns = tagColumn(ctx, c, columns, col.Name, col.Tags)
			}
		}

		for _, col := range requestColumnsApi {
			if len(col.Tags) > 0 {
				columns = tagColumn(ctx, c, columns, col.Name, col.Tags)
			}
		}

		columnUpdate := make(map[string]interface{})
		columnUpdate["op"] = "add"
		columnUpdate["path"] = "/columns"
		columnUpdate["value"] = columns
		requestBody = append(requestBody, columnUpdate)
	}

	resp, err := c.TablesApi.PatchTable(ctx, table.Id).RequestBody(requestBody).Execute()
	if err != nil {
		s.logger.Info().Msg(fmt.Sprintf("Error when calling `TablesApi.PatchTable``: %v\n", err))
		s.logger.Info().Msg(fmt.Sprintf("Full HTTP response: %v\n", resp))
		return err
	}

	return nil
}

func (s *OpenMetadataApiService) deleteAsset(ctx context.Context, c *client.APIClient, assetId string) (int, error) {
	table, r, err := c.TablesApi.GetTableByFQN(ctx, assetId).Execute()
	if err != nil {
		s.logger.Info().Msg(fmt.Sprintf("Error when calling `TablesApi.GetTableByFQN``: %v\n", err))
		s.logger.Info().Msg(fmt.Sprintf("Full HTTP response: %v\n", r))
		return http.StatusNotFound, err
	}

	r, err = c.TablesApi.DeleteTable(ctx, table.Id).Execute()
	if err != nil {
		s.logger.Info().Msg(fmt.Sprintf("Error when calling `TablesApi.DeleteTable``: %v\n", err))
		s.logger.Info().Msg(fmt.Sprintf("Full HTTP response: %v\n", r))
		return http.StatusBadRequest, err
	}
	return http.StatusOK, nil
}

func (s *OpenMetadataApiService) constructAssetResponse(ctx context.Context,
	c *client.APIClient,
	table *client.Table) (*models.GetAssetResponse, error) {
	ret := &models.GetAssetResponse{}
	customProperties := table.GetExtension()

	credentials := customProperties["credentials"]
	if credentials != nil {
		ret.Credentials = credentials.(string)
	}
	name := customProperties["name"]
	if name != nil {
		nameStr := name.(string)
		ret.ResourceMetadata.Name = &nameStr
	}

	owner := customProperties["owner"]
	if owner != nil {
		ownerStr := owner.(string)
		ret.ResourceMetadata.Owner = &ownerStr
	}

	geography := customProperties["geography"]
	if geography != nil {
		geographyStr := geography.(string)
		ret.ResourceMetadata.Geography = &geographyStr
	}

	dataFormat := customProperties["dataFormat"]
	if dataFormat != nil {
		dataFormatStr := dataFormat.(string)
		ret.Details.DataFormat = &dataFormatStr
	}

	connectionType := customProperties["connectionType"].(string)

	respService, r, err := c.DatabaseServiceApi.GetDatabaseServiceByID(ctx, table.Service.Id).Execute()
	if err != nil {
		s.logger.Info().Msg(fmt.Sprintf("Error when calling `ServicesApi.GetDatabaseServiceByID``: %v\n", err))
		s.logger.Info().Msg(fmt.Sprintf("Full HTTP response: %v\n", r))
		return nil, err
	}

	dt, found := s.NameToDatabaseStruct[connectionType]
	if !found {
		return nil, errors.New("Unrecognized connection type: " + connectionType)
	}

	config := dt.TranslateOpenMetadataConfigToFybrikConfig(respService.Connection.GetConfig())

	additionalProperties := make(map[string]interface{})
	ret.Details.Connection.Name = connectionType
	additionalProperties[connectionType] = config
	ret.Details.Connection.AdditionalProperties = additionalProperties

	for _, s := range table.Columns {

		if len(s.Tags) > 0 {
			tags := make(map[string]interface{})
			for _, t := range s.Tags {
				tags[utils.StripTag(t.TagFQN)] = "true"
			}
			ret.ResourceMetadata.Columns = append(ret.ResourceMetadata.Columns, models.ResourceColumn{Name: s.Name, Tags: tags})
		} else {
			ret.ResourceMetadata.Columns = append(ret.ResourceMetadata.Columns, models.ResourceColumn{Name: s.Name})
		}
	}

	if len(table.Tags) > 0 {
		tags := make(map[string]interface{})
		for _, s := range table.Tags {
			tags[utils.StripTag(s.TagFQN)] = "true"
		}
		ret.ResourceMetadata.Tags = tags
	}

	return ret, nil
}
