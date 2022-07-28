/*
 * Data Catalog Service - Asset Details
 *
 * API version: 1.0.0
 * Based on code Generated by: OpenAPI Generator (https://openapi-generator.tech)
 */

// CHANGE-FROM-GENERATED-CODE: All code in this file is different from auto-generated code.
// This code is specific for working with OpenMetadata

package main

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"os"
	"strings"

	client "github.com/fybrik/datacatalog-go-client"
	models "github.com/fybrik/datacatalog-go-models"
	api "github.com/fybrik/datacatalog-go/go"
)

type OpenMetadataApiService struct {
	Endpoint             string
	SleepIntervalMS      int
	NumRetries           int
	NameToDatabaseStruct map[string]databaseType
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
		fmt.Fprintf(os.Stderr, "Error when calling `MetadataApi.ListTypes``: %v\n", err)
		fmt.Fprintf(os.Stderr, "Full HTTP response: %v\n", r)
		return
	}
	for _, t := range typeList.Data {
		if *t.FullyQualifiedName == "table" {
			tableID = *t.Id
			break
		}
	}

	// Find the ID for the 'string' type
	var stringID string
	typeList, r, err = c.MetadataApi.ListTypes(ctx).Category("field").Limit(100).Execute()
	if err != nil {
		fmt.Fprintf(os.Stderr, "Error when calling `MetadataApi.ListTypes``: %v\n", err)
		fmt.Fprintf(os.Stderr, "Full HTTP response: %v\n", r)
		return
	}
	for _, t := range typeList.Data {
		if *t.FullyQualifiedName == "string" {
			stringID = *t.Id
			break
		}
	}

	// Add custom properties for tables
	c.MetadataApi.AddProperty(ctx, tableID).CustomProperty(*client.NewCustomProperty(
		"The vault plugin path where the destination data credentials will be stored as kubernetes secrets", "credentials",
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
func NewOpenMetadataApiService(conf map[interface{}]interface{}) OpenMetadataApiServicer {
	var SleepIntervalMS int
	var NumRetries int

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

	nameToDatabaseStruct := make(map[string]databaseType)
	nameToDatabaseStruct["mysql"] = &mysql{}
	nameToDatabaseStruct["s3"] = NewS3()

	s := &OpenMetadataApiService{Endpoint: conf["openmetadata_endpoint"].(string),
		SleepIntervalMS:      SleepIntervalMS,
		NumRetries:           NumRetries,
		NameToDatabaseStruct: nameToDatabaseStruct}

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

func getTag(ctx context.Context, c *client.APIClient, tagFQN string) client.TagLabel {
	if strings.Count(tagFQN, ".") == 0 {
		// not a 'category.primary' or 'category.primary.secondary' format
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
	for i, col := range columns {
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

// CreateAsset - This REST API writes data asset information to the data catalog configured in fybrik
func (s *OpenMetadataApiService) CreateAsset(ctx context.Context,
	xRequestDatacatalogWriteCred string,
	createAssetRequest models.CreateAssetRequest) (api.ImplResponse, error) {

	connectionName := createAssetRequest.Details.Connection.Name

	dt, found := s.NameToDatabaseStruct[connectionName]
	if !found {
		return api.Response(http.StatusBadRequest, nil), errors.New("currently, " + connectionName +
			" connection type not supported")
	}

	c := s.getOpenMetadataClient()

	var databaseServiceId string
	var databaseServiceName string
	var err error

	// Let us begin with checking whether the database service already exists
	OMConfig := dt.translateFybrikConfigToOpenMetadataConfig(createAssetRequest.Details.GetConnection().AdditionalProperties[connectionName].(map[string]interface{}))
	databaseServiceId, databaseServiceName, found = s.findService(ctx, c, OMConfig, connectionName)
	if !found {
		// If does not exist, let us create database service
		databaseServiceId, databaseServiceName, err = s.createDatabaseService(ctx, c, createAssetRequest, connectionName, OMConfig, dt.OMTypeName())
		if err != nil {
			return api.Response(http.StatusBadRequest, nil), err
		}
	}

	// now that we know the of the database service, we can determine the asset name in OpenMetadata
	assetId := dt.constructFullAssetId(databaseServiceName, createAssetRequest)

	// Let's check whether OM already has this asset
	found, _ = s.findAsset(ctx, c, assetId)
	if found {
		return api.Response(http.StatusBadRequest, nil), errors.New("Asset already exists")
	}

	// Asset not discovered yet
	// Let's check whether there is an ingestion pipeline we can trigger
	ingestionPipelineName := "pipeline-" + createAssetRequest.DestinationCatalogID + "." + *createAssetRequest.DestinationAssetID
	ingestionPipelineNameFull := appendStrings(databaseServiceName, ingestionPipelineName)

	var ingestionPipelineID string
	ingestionPipelineID, found = s.findIngestionPipeline(ctx, c, ingestionPipelineNameFull)

	if !found {
		// Let us create an ingestion pipeline
		ingestionPipelineID, err = s.createIngestionPipeline(ctx, c, databaseServiceId, ingestionPipelineName)
	}

	// Let us deploy and run the ingestion pipeline
	err = s.deployAndRunIngestionPipeline(ctx, c, ingestionPipelineID)
	if err != nil {
		return api.Response(http.StatusBadRequest, nil), err
	}

	// We just triggered a run of the ingestion pipeline.
	// Now we need to wait unti the asset is discovered
	success, table := s.waitUntilAssetIsDiscovered(ctx, c, assetId)

	if !success {
		return api.Response(http.StatusBadRequest, nil), errors.New("Could not find table " + assetId)
	}

	// Now that OM is aware of the asset, we need to enrich it --
	// add tags to asset and to columns, and populate the custom properties
	err = s.enrichAsset(ctx, table, c,
		createAssetRequest.Credentials, createAssetRequest.ResourceMetadata.Geography,
		createAssetRequest.ResourceMetadata.Name, createAssetRequest.ResourceMetadata.Owner,
		createAssetRequest.Details.DataFormat,
		createAssetRequest.ResourceMetadata.Tags,
		createAssetRequest.ResourceMetadata.Columns, nil)

	if err != nil {
		return api.Response(http.StatusBadRequest, nil), err
	}

	return api.Response(http.StatusCreated, api.CreateAssetResponse{AssetID: assetId}), nil
}

// DeleteAsset - This REST API deletes data asset
func (s *OpenMetadataApiService) DeleteAsset(ctx context.Context, xRequestDatacatalogCred string, deleteAssetRequest api.DeleteAssetRequest) (api.ImplResponse, error) {
	c := s.getOpenMetadataClient()
	errorCode, err := s.deleteAsset(ctx, c, deleteAssetRequest.AssetID)

	if err != nil {
		return api.Response(errorCode, nil), err
	}

	return api.Response(200, api.DeleteAssetResponse{}), nil
}

// GetAssetInfo - This REST API gets data asset information from the data catalog configured in fybrik for the data sets indicated in FybrikApplication yaml
func (s *OpenMetadataApiService) GetAssetInfo(ctx context.Context, xRequestDatacatalogCred string, getAssetRequest api.GetAssetRequest) (api.ImplResponse, error) {
	c := s.getOpenMetadataClient()

	assetID := getAssetRequest.AssetID

	found, table := s.findAsset(ctx, c, assetID)
	if !found {
		return api.Response(http.StatusNotFound, nil), errors.New("Asset not found")
	}

	version := fmt.Sprintf("%f", *table.Version)
	table, r, err := c.TablesApi.GetSpecificDatabaseVersion1(ctx, table.Id, version).Execute()
	if err != nil {
		fmt.Fprintf(os.Stderr, "Error when calling `TablesApi.GetSpecificDatabaseVersion1``: %v\n", err)
		fmt.Fprintf(os.Stderr, "Full HTTP response: %v\n", r)
		return api.Response(http.StatusBadRequest, nil), err
	}

	assetResponse, err := s.constructAssetResponse(ctx, c, table)
	if err != nil {
		return api.Response(http.StatusBadRequest, nil), err
	}
	return api.Response(http.StatusOK, assetResponse), nil
}

// UpdateAsset - This REST API updates data asset information in the data catalog configured in fybrik
func (s *OpenMetadataApiService) UpdateAsset(ctx context.Context, xRequestDatacatalogUpdateCred string, updateAssetRequest api.UpdateAssetRequest) (api.ImplResponse, error) {
	c := s.getOpenMetadataClient()
	assetId := updateAssetRequest.AssetID

	found, table := s.findLatestAsset(ctx, c, assetId)
	if !found {
		return api.Response(http.StatusNotFound, nil), errors.New("Asset not found")
	}

	err := s.enrichAsset(ctx, table, c, nil, nil, &updateAssetRequest.Name, &updateAssetRequest.Owner, nil,
		updateAssetRequest.Tags, nil, updateAssetRequest.Columns)

	if err != nil {
		return api.Response(http.StatusBadRequest, nil), err
	}

	return api.Response(http.StatusOK, api.UpdateAssetResponse{Status: "Asset update operation successful"}), nil
}
