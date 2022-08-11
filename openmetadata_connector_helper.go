package main

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"os"
	"strings"
	"time"

	client "github.com/fybrik/datacatalog-go-client"
	models "github.com/fybrik/datacatalog-go-models"
	api "github.com/fybrik/datacatalog-go/go"
	database_types "github.com/fybrik/openmetadata-connector/database-types"
	utils "github.com/fybrik/openmetadata-connector/utils"
	"github.com/rs/zerolog"
)

func getTag(ctx context.Context, c *client.APIClient, tagFQN string) client.TagLabel {
	if strings.Count(tagFQN, ".") == 0 {
		// Since this is not a 'category.primary' or 'category.primary.secondary' format,
		// we will translate it to 'Fybrik.tagFQN'. We try to create it
		// (whether it exists or not)
		createTag := *client.NewCreateTag(tagFQN, tagFQN)
		c.TagsApi.CreatePrimaryTag(ctx, "Fybrik").CreateTag(createTag).Execute()
		tagFQN = "Fybrik." + tagFQN
	}
	return *&client.TagLabel{
		LabelType: "Manual",
		Source:    "Tag",
		State:     "Confirmed",
		TagFQN:    tagFQN,
	}
}

func tagColumn(ctx context.Context, c *client.APIClient, columns []client.Column, colName string, colTags map[string]interface{}) []client.Column {
	// traverse columns
	for i, col := range columns {
		// search for colName
		if col.Name == colName {
			for tag := range colTags {
				col.Tags = append(col.Tags, getTag(ctx, c, tag))
			}
			columns[i] = col
			return columns
		}
	}
	return columns
}

func (s *OpenMetadataApiService) prepareOpenMetadataForFybrik() {
	ctx := context.Background()
	c := s.getOpenMetadataClient()

	// Create Tag Category for Fybrik
	c.TagsApi.CreateTagCategory(ctx).CreateTagCategory(*client.NewCreateTagCategory("Classification",
		"Parent Category for all Fybrik labels", "Fybrik")).Execute()

	// Find the ID for the 'table' entity
	var tableID string

	typeList, r, err := c.MetadataApi.ListTypes(ctx).Category("entity").Limit(100).Execute()
	if err != nil {
		s.logger.Fatal().Msg("Error in prepareOpenMetadataForFybrik")
		s.logger.Fatal().Msg(fmt.Sprintf("Error when calling `MetadataApi.ListTypes``: %v\n", err))
		s.logger.Fatal().Msg(fmt.Sprintf("Full HTTP response: %v\n", r))
		return
	}
	for _, t := range typeList.Data {
		if *t.FullyQualifiedName == "table" {
			tableID = *t.Id
			break
		}
	}

	if tableID == "" {
		s.logger.Fatal().Msg("Error in prepareOpenMetadataForFybrik")
		s.logger.Fatal().Msg("Failed to find the ID for entity 'table'")
		return
	}

	// Find the ID for the 'string' type
	var stringID string
	typeList, r, err = c.MetadataApi.ListTypes(ctx).Category("field").Limit(100).Execute()
	if err != nil {
		s.logger.Info().Msg(fmt.Sprintf("Error when calling `MetadataApi.ListTypes``: %v\n", err))
		s.logger.Info().Msg(fmt.Sprintf("Full HTTP response: %v\n", r))
		return
	}
	for _, t := range typeList.Data {
		if *t.FullyQualifiedName == "string" {
			stringID = *t.Id
			break
		}
	}

	if stringID == "" {
		s.logger.Fatal().Msg("Error in prepareOpenMetadataForFybrik")
		s.logger.Fatal().Msg("Failed to find the ID for entity 'string'")
		return
	}

	// Add custom properties for tables
	c.MetadataApi.AddProperty(ctx, tableID).CustomProperty(*client.NewCustomProperty(
		"The vault plugin path where the destination data credentials will be stored as kubernetes secrets", "credentials",
		*client.NewEntityReference(stringID, "string"))).Execute()
	c.MetadataApi.AddProperty(ctx, tableID).CustomProperty(*client.NewCustomProperty(
		"Connection type, e.g.: s3 or mysql", "connectionType",
		*client.NewEntityReference(stringID, "string"))).Execute()
	c.MetadataApi.AddProperty(ctx, tableID).CustomProperty(*client.NewCustomProperty(
		"Name of the resource", "name",
		*client.NewEntityReference(stringID, "string"))).Execute()
	c.MetadataApi.AddProperty(ctx, tableID).CustomProperty(*client.NewCustomProperty(
		"Geography of the resource", "geography",
		*client.NewEntityReference(stringID, "string"))).Execute()
	c.MetadataApi.AddProperty(ctx, tableID).CustomProperty(*client.NewCustomProperty(
		"Owner of the resource", "owner",
		*client.NewEntityReference(stringID, "string"))).Execute()
	c.MetadataApi.AddProperty(ctx, tableID).CustomProperty(*client.NewCustomProperty(
		"Data format", "dataFormat",
		*client.NewEntityReference(stringID, "string"))).Execute()
}

// NewOpenMetadataApiService creates a new api service
func NewOpenMetadataApiService(conf map[interface{}]interface{}, logger zerolog.Logger) OpenMetadataApiServicer {
	var SleepIntervalMS int
	var NumRetries int

	var vaultConf map[interface{}]interface{} = nil
	if vaultConfMap, ok := conf["vault"]; ok {
		vaultConf = vaultConfMap.(map[interface{}]interface{})
	}

	value, ok := conf["openmetadata_sleep_interval"]
	if ok {
		SleepIntervalMS = value.(int)
	} else {
		SleepIntervalMS = 500
	}

	value, ok = conf["openmetadata_num_retries"]
	if ok {
		NumRetries = value.(int)
	} else {
		NumRetries = 20
	}

	nameToDatabaseStruct := make(map[string]database_types.DatabaseType)
	nameToDatabaseStruct["mysql"] = database_types.NewMysql()
	nameToDatabaseStruct["s3"] = database_types.NewS3(vaultConf)

	s := &OpenMetadataApiService{Endpoint: conf["openmetadata_endpoint"].(string),
		SleepIntervalMS:      SleepIntervalMS,
		NumRetries:           NumRetries,
		NameToDatabaseStruct: nameToDatabaseStruct,
		logger:               logger,
		NumRenameRetries:     10}

	s.prepareOpenMetadataForFybrik()

	return s
}

func (s *OpenMetadataApiService) getOpenMetadataClient() *client.APIClient {
	conf := client.Configuration{Servers: client.ServerConfigurations{
		{
			URL:         s.Endpoint,
			Description: "Endpoint URL",
		},
	},
	}
	return client.NewAPIClient(&conf)
}

func (s *OpenMetadataApiService) findService(ctx context.Context,
	c *client.APIClient,
	dt database_types.DatabaseType,
	connectionProperties map[string]interface{}) (string, string, bool) {
	connectionType := dt.OMTypeName()

	serviceList, _, err := c.DatabaseServiceApi.ListDatabaseServices(ctx).Execute()
	if err != nil {
		fmt.Fprintln(os.Stderr, "Service does not exist yet")
		return "", "", false
	}
	for _, service := range serviceList.Data {
		found := true
		if connectionType != service.ServiceType {
			found = false
		} else {
			if !dt.CompareServiceConfigurations(connectionProperties, service.Connection.Config) {
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
	connection := client.DatabaseConnection{Config: OMConfig}

	databaseServiceName := createAssetRequest.DestinationCatalogID + "-" + connectionName
	createDatabaseService := client.NewCreateDatabaseService(connection, databaseServiceName, OMTypeName)

	databaseService, r, err := c.DatabaseServiceApi.CreateDatabaseService(ctx).CreateDatabaseService(*createDatabaseService).Execute()
	if err != nil {
		s.logger.Info().Msg(fmt.Sprintf("Error when calling `ServicesApi.CreateDatabaseService``: %v\n", err))
		s.logger.Info().Msg(fmt.Sprintf("Full HTTP response: %v\n", r))

		// let's try creating the service with different names
		for i := 0; i < s.NumRenameRetries; i++ {
			createDatabaseService.SetName(databaseServiceName + "-" + utils.RandStringBytes(5))
			databaseService, r, err := c.DatabaseServiceApi.CreateDatabaseService(ctx).CreateDatabaseService(*createDatabaseService).Execute()
			if err == nil {
				return databaseService.Id, *databaseService.FullyQualifiedName, nil
			} else {
				s.logger.Info().Msg(fmt.Sprintf("Error when calling `ServicesApi.CreateDatabaseService``: %v\n", err))
				s.logger.Info().Msg(fmt.Sprintf("Full HTTP response: %v\n", r))
			}
		}

		return "", "", err
	}
	return databaseService.Id, *databaseService.FullyQualifiedName, nil
}

func (s *OpenMetadataApiService) waitUntilAssetIsDiscovered(ctx context.Context, c *client.APIClient, name string) (bool, *client.Table) {
	count := 0
	for {
		s.logger.Info().Msg("running GetTableByFQN")
		table, _, err := c.TablesApi.GetTableByFQN(ctx, name).Execute()
		if err == nil {
			s.logger.Info().Msg("Found the table!")
			return true, table
		} else {
			s.logger.Info().Msg("Could not find the table. Let's try again")
		}

		if count == s.NumRetries {
			break
		}
		count++
		time.Sleep(time.Duration(s.SleepIntervalMS) * time.Millisecond)
	}
	s.logger.Info().Msg("Too many retries. Could not find table " + name + ". Giving up")
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

// populate the values in a GetAssetResponse structure to include everything:
// credentials, name, owner, geography, dataFormat, connection informations,
// tags, and columns
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
