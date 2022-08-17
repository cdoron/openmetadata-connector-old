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
	"net/http"

	"github.com/rs/zerolog"

	models "github.com/fybrik/datacatalog-go-models"
	api "github.com/fybrik/datacatalog-go/go"
	database_types "github.com/fybrik/openmetadata-connector/database-types"
	utils "github.com/fybrik/openmetadata-connector/utils"
)

type OpenMetadataApiService struct {
	Endpoint             string
	SleepIntervalMS      int
	NumRetries           int
	NameToDatabaseStruct map[string]database_types.DatabaseType
	logger               zerolog.Logger
	NumRenameRetries     int
	initialized          bool
}

// CreateAsset - This REST API writes data asset information to the data catalog configured in fybrik
func (s *OpenMetadataApiService) CreateAsset(ctx context.Context,
	xRequestDatacatalogWriteCred string,
	createAssetRequest models.CreateAssetRequest) (api.ImplResponse, error) {
	if !s.initialized {
		s.initialized = s.prepareOpenMetadataForFybrik()
	}

	connectionType := createAssetRequest.Details.Connection.Name

	// check whether connectionType is one of the connection types supported by the OM connector
	dt, found := s.NameToDatabaseStruct[connectionType]
	if !found {
		s.logger.Error().Msg("currently, " + connectionType + " connection type not supported")
		return api.Response(http.StatusBadRequest, nil), errors.New("currently, " + connectionType +
			" connection type not supported")
	}

	c := s.getOpenMetadataClient()

	var databaseServiceId string
	var databaseServiceName string
	var err error

	// Let us begin by checking whether the database service already exists.
	// step 1: Translate the fybrik connection information to the OM connection information.
	//         This configuration information will later be used to create an OM connection
	//         (if it does not already exist)
	OMConfig := dt.TranslateFybrikConfigToOpenMetadataConfig(
		createAssetRequest.Details.GetConnection().AdditionalProperties[connectionType].(map[string]interface{}),
		createAssetRequest.Credentials)
	// step 2: compare the transformed connection information to that of all existing services
	databaseServiceId, databaseServiceName, found = s.findService(ctx, c, dt, OMConfig)

	if !found {
		// If does not exist, let us create database service
		databaseServiceId, databaseServiceName, err = s.createDatabaseService(ctx, c, createAssetRequest, connectionType, OMConfig, dt.OMTypeName())
		if err != nil {
			s.logger.Error().Msg("unable to create Database Service for " + dt.OMTypeName() + " connection")
			return api.Response(http.StatusBadRequest, nil), err
		}
	}

	// now that we know the of the database service, we can determine the asset name in OpenMetadata
	assetId := dt.ConstructFullAssetId(databaseServiceName, createAssetRequest)

	// Let's check whether OM already has this asset
	found, _ = s.findAsset(ctx, c, assetId)
	if found {
		s.logger.Error().Msg("Could not create asset, as asset already exists")
		return api.Response(http.StatusBadRequest, nil), errors.New("Asset already exists")
	}

	// Asset not discovered yet
	// Let's check whether there is an ingestion pipeline we can trigger
	ingestionPipelineName := "pipeline-" + createAssetRequest.DestinationCatalogID + "." + *createAssetRequest.DestinationAssetID
	ingestionPipelineNameFull := utils.AppendStrings(databaseServiceName, ingestionPipelineName)

	var ingestionPipelineID string
	ingestionPipelineID, found = s.findIngestionPipeline(ctx, c, ingestionPipelineNameFull)

	if !found {
		// Let us create an ingestion pipeline
		s.logger.Info().Msg("Ingestion Pipeline not found. Creating.")
		ingestionPipelineID, err = s.createIngestionPipeline(ctx, c, databaseServiceId, ingestionPipelineName)
	}

	s.logger.Info().Msg("About to deploy and run ingestion Pipeline.")
	// Let us deploy and run the ingestion pipeline
	err = s.deployAndRunIngestionPipeline(ctx, c, ingestionPipelineID)
	if err != nil {
		return api.Response(http.StatusBadRequest, nil), err
	}

	// We just triggered a run of the ingestion pipeline.
	// Now we need to wait until the asset is discovered
	s.logger.Info().Msg("Waiting for asset to be discovered")
	success, table := s.waitUntilAssetIsDiscovered(ctx, c, assetId)

	if !success {
		return api.Response(http.StatusBadRequest, nil), errors.New("Could not find table " + assetId)
	}

	s.logger.Info().Msg("Enriching asset with additional information (e.g. tags)")
	// Now that OM is aware of the asset, we need to enrich it --
	// add tags to asset and to columns, and populate the custom properties
	err = s.enrichAsset(ctx, table, c,
		createAssetRequest.Credentials, createAssetRequest.ResourceMetadata.Geography,
		createAssetRequest.ResourceMetadata.Name, createAssetRequest.ResourceMetadata.Owner,
		createAssetRequest.Details.DataFormat,
		createAssetRequest.ResourceMetadata.Tags,
		createAssetRequest.ResourceMetadata.Columns, nil, connectionType)

	if err != nil {
		s.logger.Error().Msg("Asset enrichment failed")
		return api.Response(http.StatusBadRequest, nil), err
	}

	s.logger.Info().Msg("Asset creation and enrichment successful")
	return api.Response(http.StatusCreated, api.CreateAssetResponse{AssetID: assetId}), nil
}

// DeleteAsset - This REST API deletes data asset
func (s *OpenMetadataApiService) DeleteAsset(ctx context.Context, xRequestDatacatalogCred string, deleteAssetRequest api.DeleteAssetRequest) (api.ImplResponse, error) {
	if !s.initialized {
		s.initialized = s.prepareOpenMetadataForFybrik()
	}

	c := s.getOpenMetadataClient()
	errorCode, err := s.deleteAsset(ctx, c, deleteAssetRequest.AssetID)

	if err != nil {
		s.logger.Info().Msg("Asset deletion failed")
		return api.Response(errorCode, nil), err
	}

	s.logger.Info().Msg("Asset deletion successful")
	return api.Response(200, api.DeleteAssetResponse{}), nil
}

// GetAssetInfo - This REST API gets data asset information from the data catalog configured in fybrik for the data sets indicated in FybrikApplication yaml
func (s *OpenMetadataApiService) GetAssetInfo(ctx context.Context, xRequestDatacatalogCred string, getAssetRequest api.GetAssetRequest) (api.ImplResponse, error) {
	if !s.initialized {
		s.initialized = s.prepareOpenMetadataForFybrik()
	}

	c := s.getOpenMetadataClient()

	assetID := getAssetRequest.AssetID

	found, table := s.findLatestAsset(ctx, c, assetID)
	if !found {
		s.logger.Error().Msg("Asset not found")
		return api.Response(http.StatusNotFound, nil), errors.New("Asset not found")
	}

	assetResponse, err := s.constructAssetResponse(ctx, c, table)
	if err != nil {
		s.logger.Error().Msg("Construction of Asset Reponse failed")
		return api.Response(http.StatusBadRequest, nil), err
	}

	s.logger.Info().Msg("GetAssetInfo successful")
	return api.Response(http.StatusOK, assetResponse), nil
}

// UpdateAsset - This REST API updates data asset information in the data catalog configured in fybrik
func (s *OpenMetadataApiService) UpdateAsset(ctx context.Context, xRequestDatacatalogUpdateCred string, updateAssetRequest api.UpdateAssetRequest) (api.ImplResponse, error) {
	if !s.initialized {
		s.initialized = s.prepareOpenMetadataForFybrik()
	}

	c := s.getOpenMetadataClient()
	assetId := updateAssetRequest.AssetID

	found, table := s.findLatestAsset(ctx, c, assetId)
	if !found {
		s.logger.Error().Msg("Asset not found")
		return api.Response(http.StatusNotFound, nil), errors.New("Asset not found")
	}

	err := s.enrichAsset(ctx, table, c, nil, nil, &updateAssetRequest.Name, &updateAssetRequest.Owner, nil,
		updateAssetRequest.Tags, nil, updateAssetRequest.Columns, "")
	if err != nil {
		s.logger.Error().Msg("Asset enrichment failed")
		return api.Response(http.StatusBadRequest, nil), err
	}

	s.logger.Info().Msg("UpdateAsset successful")
	return api.Response(http.StatusOK, api.UpdateAssetResponse{Status: "Asset update operation successful"}), nil
}
