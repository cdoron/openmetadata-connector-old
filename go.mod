module github.com/fybrik/openmetadata-connector

go 1.17

require (
	github.com/fybrik/datacatalog-go v0.0.0
	github.com/fybrik/datacatalog-go-client v0.0.0-00010101000000-000000000000
	github.com/fybrik/datacatalog-go-models v0.0.0
	github.com/spf13/cobra v1.5.0
	gopkg.in/yaml.v2 v2.4.0
)

require (
	github.com/golang/protobuf v1.4.2 // indirect
	github.com/gorilla/mux v1.7.3 // indirect
	github.com/inconshreveable/mousetrap v1.0.0 // indirect
	github.com/spf13/pflag v1.0.5 // indirect
	golang.org/x/net v0.0.0-20200822124328-c89045814202 // indirect
	golang.org/x/oauth2 v0.0.0-20210323180902-22b0adad7558 // indirect
	google.golang.org/appengine v1.6.6 // indirect
	google.golang.org/protobuf v1.25.0 // indirect
)

replace github.com/fybrik/datacatalog-go => ./api

replace github.com/fybrik/datacatalog-go-models => ./models

replace github.com/fybrik/datacatalog-go-client => ./client
