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
	"time"

	client "github.com/fybrik/datacatalog-go-client"
	models "github.com/fybrik/datacatalog-go-models"
	api "github.com/fybrik/datacatalog-go/go"
)

type OpenMetadataApiService struct {
	Endpoint        string
	SleepIntervalMS int
	NumRetries      int
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

	s := &OpenMetadataApiService{Endpoint: conf["openmetadata_endpoint"].(string),
		SleepIntervalMS: SleepIntervalMS,
		NumRetries:      NumRetries}

	s.prepareOpenMetadataForFybrik()

	return s
}

func (s *OpenMetadataApiService) waitUntilAssetIsDiscovered(ctx context.Context, c *client.APIClient, name string) (bool, *client.Table) {
	count := 0
	for {
		fmt.Println("running GetByName5")
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

// CreateAsset - This REST API writes data asset information to the data catalog configured in fybrik
func (s *OpenMetadataApiService) CreateAsset(ctx context.Context,
	xRequestDatacatalogWriteCred string,
	createAssetRequest models.CreateAssetRequest) (api.ImplResponse, error) {

	if createAssetRequest.Details.Connection.Name != "mysql" {
		return api.Response(http.StatusBadRequest, nil), errors.New("currently, we only support the mysql connection")
	}

	c := s.getOpenMetadataClient()

	// Let us begin with checking whether the database service already exists
	// XXXXXXXXX

	// If does not exist, let us create database service
	connection := client.NewDatabaseConnection()
	connection.SetConfig(createAssetRequest.Details.GetConnection().AdditionalProperties["mysql"].(map[string]interface{}))
	createDatabaseService := client.NewCreateDatabaseService(*connection, createAssetRequest.DestinationCatalogID+"-mysql", "Mysql")

	databaseService, r, err := c.DatabaseServiceApi.CreateDatabaseService(ctx).CreateDatabaseService(*createDatabaseService).Execute()
	if err != nil {
		fmt.Fprintf(os.Stderr, "Error when calling `ServicesApi.Create16``: %v\n", err)
		fmt.Fprintf(os.Stderr, "Full HTTP response: %v\n", r)
		return api.Response(r.StatusCode, nil), err
	}

	// Next let us create an ingestion pipeline
	sourceConfig := *client.NewSourceConfig()
	sourceConfig.SetConfig(map[string]interface{}{"type": "DatabaseMetadata"})
	newCreateIngestionPipeline := *client.NewCreateIngestionPipeline(*&client.AirflowConfig{},
		"pipeline-"+*createAssetRequest.DestinationAssetID,
		"metadata", *client.NewEntityReference(databaseService.Id, "databaseService"),
		sourceConfig)

	ingestionPipeline, r, err := c.IngestionPipelinesApi.CreateIngestionPipeline(ctx).CreateIngestionPipeline(newCreateIngestionPipeline).Execute()
	if err != nil {
		fmt.Fprintf(os.Stderr, "Error when calling `IngestionPipelinesApi.Create17``: %v\n", err)
		fmt.Fprintf(os.Stderr, "Full HTTP response: %v\n", r)
		return api.Response(r.StatusCode, nil), err
	}

	// Let us deploy the ingestion pipeline
	ingestionPipeline, r, err = c.IngestionPipelinesApi.DeployIngestion(ctx, *ingestionPipeline.Id).Execute()
	if err != nil {
		fmt.Fprintf(os.Stderr, "Error when calling `IngestionPipelinesApi.DeployIngestion``: %v\n", err)
		fmt.Fprintf(os.Stderr, "Full HTTP response: %v\n", r)
		return api.Response(r.StatusCode, nil), err
	}
	ingestionPipeline, r, err = c.IngestionPipelinesApi.TriggerIngestionPipelineRun(ctx, *ingestionPipeline.Id).Execute()
	if err != nil {
		fmt.Fprintf(os.Stderr, "Error when calling `IngestionPipelinesApi.TriggerIngestion``: %v\n", err)
		fmt.Fprintf(os.Stderr, "Full HTTP response: %v\n", r)
		return api.Response(r.StatusCode, nil), err
	}

	assetID := *ingestionPipeline.Service.FullyQualifiedName + "." + *createAssetRequest.DestinationAssetID
	success, table := s.waitUntilAssetIsDiscovered(ctx, c, assetID)

	var requestBody []map[string]interface{}
	init := make(map[string]interface{})
	init["op"] = "add"
	init["path"] = "/extension"
	init["value"] = make(map[string]interface{})

	geography := make(map[string]interface{})
	geography["op"] = "add"
	geography["path"] = "/extension/geography"
	geography["value"] = "theshire"

	requestBody = append(requestBody, init)
	requestBody = append(requestBody, geography)

	resp, err := c.TablesApi.PatchTable(ctx, table.Id).RequestBody(requestBody).Execute()
	if err != nil {
		fmt.Fprintf(os.Stderr, "Error when calling `TablesApi.PatchTable``: %v\n", err)
		fmt.Fprintf(os.Stderr, "Full HTTP response: %v\n", resp)
	}

	if success {
		return api.Response(http.StatusCreated, api.CreateAssetResponse{AssetID: assetID}), nil
	} else {
		return api.Response(http.StatusBadRequest, nil), errors.New("Could not find table " + assetID)
	}
}

// DeleteAsset - This REST API deletes data asset
func (s *OpenMetadataApiService) DeleteAsset(ctx context.Context, xRequestDatacatalogCred string, deleteAssetRequest api.DeleteAssetRequest) (api.ImplResponse, error) {
	// TODO - update DeleteAsset with the required logic for this service method.
	// Add api_default_service.go to the .openapi-generator-ignore to avoid overwriting this service implementation when updating open api generation.

	//TODO: Uncomment the next line to return response Response(200, DeleteAssetResponse{}) or use other options such as http.Ok ...
	//return Response(200, DeleteAssetResponse{}), nil

	//TODO: Uncomment the next line to return response Response(400, {}) or use other options such as http.Ok ...
	//return Response(400, nil),nil

	//TODO: Uncomment the next line to return response Response(404, {}) or use other options such as http.Ok ...
	//return Response(404, nil),nil

	//TODO: Uncomment the next line to return response Response(401, {}) or use other options such as http.Ok ...
	//return Response(401, nil),nil

	return api.Response(http.StatusNotImplemented, nil), errors.New("DeleteAsset method not implemented")
}

// GetAssetInfo - This REST API gets data asset information from the data catalog configured in fybrik for the data sets indicated in FybrikApplication yaml
func (s *OpenMetadataApiService) GetAssetInfo(ctx context.Context, xRequestDatacatalogCred string, getAssetRequest api.GetAssetRequest) (api.ImplResponse, error) {
	c := s.getOpenMetadataClient()

	assetID := getAssetRequest.AssetID

	//fields := "tableConstraints,tablePartition,usageSummary,owner,profileSample,customMetrics,tags,followers,joins,sampleData,viewDefinition,tableProfile,location,tableQueries,dataModel,tests" // string | Fields requested in the returned resource (optional)
	fields := "tags"
	include := "non-deleted" // string | Include all, deleted, or non-deleted entities. (optional) (default to "non-deleted")
	respAsset, r, err := c.TablesApi.GetTableByFQN(ctx, assetID).Fields(fields).Include(include).Execute()
	if err != nil {
		fmt.Fprintf(os.Stderr, "Error when calling `TablesApi.GetByName5``: %v\n", err)
		fmt.Fprintf(os.Stderr, "Full HTTP response: %v\n", r)
		return api.Response(http.StatusBadRequest, nil), err
	}

	serviceType := strings.ToLower(*respAsset.ServiceType)

	ret := &models.GetAssetResponse{}
	ret.Details.Connection.Name = serviceType
	dataFormat := "SQL"
	ret.Details.DataFormat = &dataFormat

	respService, r, err := c.DatabaseServiceApi.GetDatabaseServiceByID(ctx, respAsset.Service.Id).Execute()
	if err != nil {
		fmt.Fprintf(os.Stderr, "Error when calling `ServicesApi.Get19``: %v\n", err)
		fmt.Fprintf(os.Stderr, "Full HTTP response: %v\n", r)
		return api.Response(http.StatusBadRequest, nil), err
	}

	config := respService.Connection.GetConfig()

	additionalProperties := make(map[string]interface{})
	additionalProperties[serviceType] = config
	ret.Details.Connection.AdditionalProperties = additionalProperties
	ret.ResourceMetadata.Name = respAsset.FullyQualifiedName

	ret.Credentials = config["username"].(string) + ":" + config["password"].(string)

	for _, s := range respAsset.Columns {
		tags := make(map[string]interface{})
		for _, t := range s.Tags {
			tags[t.TagFQN] = "true"
		}
		ret.ResourceMetadata.Columns = append(ret.ResourceMetadata.Columns, models.ResourceColumn{Name: s.Name, Tags: tags})
	}

	tags := make(map[string]interface{})
	for _, s := range respAsset.Tags {
		tags[s.TagFQN] = "true"
	}
	ret.ResourceMetadata.Tags = tags

	return api.Response(200, ret), nil
}

// UpdateAsset - This REST API updates data asset information in the data catalog configured in fybrik
func (s *OpenMetadataApiService) UpdateAsset(ctx context.Context, xRequestDatacatalogUpdateCred string, updateAssetRequest api.UpdateAssetRequest) (api.ImplResponse, error) {
	// TODO - update UpdateAsset with the required logic for this service method.
	// Add api_default_service.go to the .openapi-generator-ignore to avoid overwriting this service implementation when updating open api generation.

	//TODO: Uncomment the next line to return response Response(200, UpdateAssetResponse{}) or use other options such as http.Ok ...
	//return Response(200, UpdateAssetResponse{}), nil

	//TODO: Uncomment the next line to return response Response(400, {}) or use other options such as http.Ok ...
	//return Response(400, nil),nil

	//TODO: Uncomment the next line to return response Response(404, {}) or use other options such as http.Ok ...
	//return Response(404, nil),nil

	//TODO: Uncomment the next line to return response Response(401, {}) or use other options such as http.Ok ...
	//return Response(401, nil),nil

	return api.Response(http.StatusNotImplemented, nil), errors.New("UpdateAsset method not implemented")
}
