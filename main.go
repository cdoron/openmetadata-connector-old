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
	"fmt"
	"io/ioutil"
	"net/http"
	"os"
	"strconv"

	logging "fybrik.io/fybrik/pkg/logging"
	api "github.com/fybrik/datacatalog-go/go"
	"github.com/spf13/cobra"
	"gopkg.in/yaml.v2"
)

// RunCmd defines the command for running the connector
func RunCmd() *cobra.Command {
	logger := logging.LogInit(logging.CONNECTOR, "OpenMetadata Connector")
	configFile := "/etc/conf/conf.yaml"
	port := 8081
	cmd := &cobra.Command{
		Use:   "run",
		Short: "Run the connector",
		RunE: func(cmd *cobra.Command, args []string) error {
			// TODO - add logging level

			yamlFile, err := ioutil.ReadFile(configFile)

			if err != nil {
				return err
			}

			conf := make(map[interface{}]interface{})

			err = yaml.Unmarshal(yamlFile, &conf)
			if err != nil {
				return err
			}

			logger.Info().Msg("Server started")

			DefaultApiService := NewOpenMetadataApiService(conf, logger)
			DefaultApiController := NewOpenMetadataApiController(DefaultApiService)

			router := api.NewRouter(DefaultApiController)

			http.ListenAndServe(":"+strconv.Itoa(port), router)

			return nil
		},
	}
	cmd.Flags().StringVar(&configFile, "config", configFile, "Configuration file")
	cmd.Flags().IntVar(&port, "port", port, "Listening port")
	return cmd
}

// RootCmd defines the root cli command
func RootCmd() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "openmetadata-connector",
		Short: "Kubernetes based OpenMetadata data catalog connector for Fybrik",
	}
	cmd.AddCommand(RunCmd())
	return cmd
}

func main() {
	// Run the cli
	if err := RootCmd().Execute(); err != nil {
		fmt.Println(err)
		os.Exit(1)
	}
}
