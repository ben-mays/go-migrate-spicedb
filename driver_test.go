package spicedb

import (
	"crypto/md5"
	"fmt"
	"log"
	"net/url"
	"testing"

	pb "github.com/authzed/authzed-go/proto/authzed/api/v1"
	"github.com/ben-mays/go-migrate-spicedb/fakes"
	"github.com/stretchr/testify/assert"
)

func strPtr(s string) *string {
	return &s
}

func unescape(s string) string {
	u, err := url.QueryUnescape(s)
	if err != nil {
		panic(err)
	}
	return u
}

func TestDriverOpenURLParsing(t *testing.T) {
	// Generate encoded/decoded versions of the same postgres URL
	postgresStr := "postgres://user:password@localhost:50052/dbname"
	postgresQueryArgs := "sslmode=disable&insecure=true"
	unencodedPostgresStr := fmt.Sprintf("%s?%s", postgresStr, postgresQueryArgs)
	encodedPostgresStr := url.QueryEscape(unencodedPostgresStr)
	testcases := []struct {
		uri         string
		expectedUrl parsedUrl
		errMsg      *string
	}{
		{uri: "localhost:50051", errMsg: strPtr("this driver only supports `spicedb://` schemes")},
		{uri: "spicedb://localhost:50051", errMsg: strPtr("query arg `wrapped-conn-str` required")},
		{uri: "spicedb://localhost:50051?wrapped-conn-str=dbstr", errMsg: strPtr("query arg `pre-shared-key` required")},
		{uri: "spicedb://localhost:50051?wrapped-conn-str=dbstr&pre-shared-key=123", expectedUrl: parsedUrl{
			WrappedConnStr: "dbstr",
			SpiceDBURI:     "localhost:50051",
			SpiceDBKey:     "123",
		}},
		// Test mix of decoded and encoded query args
		// No query args on wrapped conn str
		{uri: fmt.Sprintf("spicedb://localhost:50051?wrapped-conn-str=%s&pre-shared-key=123", postgresStr), expectedUrl: parsedUrl{
			WrappedConnStr: postgresStr,
			SpiceDBURI:     "localhost:50051",
			SpiceDBKey:     "123",
		}},
		// With query args, not encoded
		{
			uri:    fmt.Sprintf("spicedb://localhost:50051?wrapped-conn-str=%s&pre-shared-key=123", unencodedPostgresStr),
			errMsg: strPtr("only pre-shared-key and wrapped-conn-str query args are supported. the presence of more query args may indicate that wrapped-conn-str was not encoded!")},
		// With query args, encoded
		{
			uri: fmt.Sprintf("spicedb://localhost:50051?wrapped-conn-str=%s&pre-shared-key=123", encodedPostgresStr),
			expectedUrl: parsedUrl{
				WrappedConnStr: encodedPostgresStr,
				SpiceDBURI:     "localhost:50051",
				SpiceDBKey:     "123",
			}},
	}
	for _, test := range testcases {
		parsed, err := parseURL(test.uri)
		if test.errMsg == nil {
			assert.Nil(t, err)
			assert.Equal(t, test.expectedUrl.SpiceDBKey, parsed.SpiceDBKey)
			assert.Equal(t, test.expectedUrl.SpiceDBURI, parsed.SpiceDBURI)
			assert.Equal(t, unescape(test.expectedUrl.WrappedConnStr), parsed.WrappedConnStr)
		} else {
			assert.NotNil(t, err)
			assert.Equal(t, *test.errMsg, err.Error())
		}
	}
}

func TestSpiceDBDataMigrations(t *testing.T) {
	type migrationEffect struct {
		op        string
		rscType   string
		rscId     string
		rel       string
		subType   string
		subId     string
		optSubRel string
	}

	testcases := []struct {
		line   string
		effect migrationEffect
		errMsg *string
	}{
		// happy case
		{
			line:   "add, resource:123, can_read, user:456",
			effect: migrationEffect{"add", "resource", "123", "can_read", "user", "456", ""},
		},
		{
			line:   "add, resource:123, can_read, user:456, member",
			effect: migrationEffect{"add", "resource", "123", "can_read", "user", "456", "member"},
		},
		{
			line:   "remove, resource:123, can_read, user:456",
			effect: migrationEffect{"remove", "resource", "123", "can_read", "user", "456", ""},
		},
		// ignores whitespace
		{
			line:   "  remove,      resource:123,     can_read,     user:456 ",
			effect: migrationEffect{"remove", "resource", "123", "can_read", "user", "456", ""},
		},
		// error handling
		{
			line:   "remove, resource:123,",
			errMsg: strPtr("invalid data migration; format is: <op>, <resource>, <relation>, <subject>, <optional-subject-relation>"),
		},
		{
			line:   "remove, resource:, can_read, user:456",
			errMsg: strPtr("invalid data migration; resource/subject format is: <resource_type>:<resource_id>"),
		},
	}

	for _, test := range testcases {
		fakeClient := &fakes.FakeClient{}
		fakeWrapperDriver := &fakes.FakeDriver{}
		driver := NewSpiceDBDriver(fakeWrapperDriver, fakeClient, log.Default())
		spicedbDriver := driver.(*SpiceDBDriver)
		err := spicedbDriver.RunMigration(test.line)
		if test.errMsg == nil {
			assert.Nil(t, err)
			assert.Equal(t, fakeClient.WriteRelationshipsCallCount(), 1)
			_, request, _ := fakeClient.WriteRelationshipsArgsForCall(0)
			assert.Equal(t, request.Updates[0].Relationship.Resource.ObjectType, test.effect.rscType)
			assert.Equal(t, request.Updates[0].Relationship.Relation, test.effect.rel)
			assert.Equal(t, request.Updates[0].Relationship.Resource.ObjectId, test.effect.rscId)
			assert.Equal(t, request.Updates[0].Relationship.Subject.Object.ObjectType, test.effect.subType)
			assert.Equal(t, request.Updates[0].Relationship.Subject.Object.ObjectId, test.effect.subId)
			assert.Equal(t, request.Updates[0].Relationship.Subject.OptionalRelation, test.effect.optSubRel)
			assert.Equal(t, request.Updates[0].Relationship.Subject.OptionalRelation, test.effect.optSubRel)
			if test.effect.op == "add" {
				assert.Equal(t, pb.RelationshipUpdate_OPERATION_CREATE, request.Updates[0].Operation)
			} else {
				assert.Equal(t, pb.RelationshipUpdate_OPERATION_DELETE, request.Updates[0].Operation)
			}
		} else {
			assert.NotNil(t, err)
			assert.Equal(t, *test.errMsg, err.Error())
		}
	}
}

func TestSpiceDBSchemaMigrations(t *testing.T) {
	sum := func(schemaText string) string {
		return fmt.Sprintf("%x", md5.Sum([]byte(schemaText)))

	}
	type migrationEffect struct {
		op         string
		schemaText string
		schemaPath string
	}
	testcases := []struct {
		line   string
		effect migrationEffect
		errMsg *string
	}{
		{
			line: fmt.Sprintf("schema, path/to/schema, %s", sum("test")),
			effect: migrationEffect{
				op:         "schema",
				schemaText: "test",
				schemaPath: "path/to/schema",
			},
		},
		{
			line: fmt.Sprintf("schema, path/to/schema, %s", sum("not-test")),
			effect: migrationEffect{
				schemaText: "test", // trigger checksum mismatch
			},
			errMsg: strPtr("checksum mismatch! bfd88eb8429462393f7277685b17ff32 != 098f6bcd4621d373cade4e832627b4f6"),
		},
	}
	for _, test := range testcases {
		fakeClient := &fakes.FakeClient{}
		fakeWrapperDriver := &fakes.FakeDriver{}
		driver := NewSpiceDBDriver(fakeWrapperDriver, fakeClient, log.Default())
		spicedbdriver := driver.(*SpiceDBDriver)
		var calledWithFilePath string
		spicedbdriver.schemaReader = func(path string) ([]byte, error) {
			calledWithFilePath = path
			return []byte(test.effect.schemaText), nil
		}
		err := spicedbdriver.RunMigration(test.line)
		if test.errMsg == nil {
			assert.Nil(t, err)
			assert.Equal(t, fakeClient.WriteSchemaCallCount(), 1)
			ctx, schema, _ := fakeClient.WriteSchemaArgsForCall(0)
			assert.NotNil(t, ctx)
			assert.Equal(t, test.effect.schemaPath, calledWithFilePath)
			assert.Equal(t, test.effect.schemaText, schema.Schema)
		} else {
			assert.NotNil(t, err)
			assert.Equal(t, *test.errMsg, err.Error())
		}
	}
}
