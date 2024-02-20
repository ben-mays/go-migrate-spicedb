package client

import (
	pb "github.com/authzed/authzed-go/proto/authzed/api/v1"
)

// Client is an interface fulfilled by the authzed-go client. This is pulled out to avoid a cycle during mocking.
type Client interface {
	pb.SchemaServiceClient
	pb.PermissionsServiceClient
	pb.WatchServiceClient
}
