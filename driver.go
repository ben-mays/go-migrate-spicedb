package spicedb

//go:generate go run  github.com/maxbrunsfeld/counterfeiter/v6 -o fakes/ github.com/golang-migrate/migrate/v4/database.Driver
//go:generate go run  github.com/maxbrunsfeld/counterfeiter/v6 -o ./fakes ./client Client

import (
	"bufio"
	"context"
	"crypto/md5"
	"errors"
	"fmt"
	"io"
	"log"
	"os"
	"strings"

	"net/url"

	pb "github.com/authzed/authzed-go/proto/authzed/api/v1"
	"github.com/authzed/authzed-go/v1"
	"github.com/authzed/grpcutil"
	"github.com/ben-mays/go-migrate-spicedb/client"
	"github.com/golang-migrate/migrate/v4/database"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

func init() {
	db := SpiceDBDriver{}
	database.Register("spicedb", &db)
}

// SpiceDBDriver is a custom driver that wraps the PSQL driver. It reads/writes
// data to SpiceDB while also keeping migration state in PSQL.
// There are two types of migrations: Data migrations and Schema migrations.
//
// Data migrations should be in CSV format of the form:
//
//	<operation>, <resource>, <relation>, <subject>
//
// Both resource and subject strings should of the form: `<type>:<id>`. Valid data migration operations
// are `add` and `remove`.
//
// Schema migrations can be intermixed with data migrations. A special operation `schema` will call WriteSchema:
//
//	schema, <path-to-schema>, <hex-encoded-md5sum>
//
// A checksum is performed on the given schema file and it must match. To generate an md5 for a schema file:
//   - `python3 -c 'import hashlib; print(hashlib.md5(open(input("File to encode: ")).read().encode()).hexdigest())'`
//
// To instantiate the driver via the `Open(conn string)` API, you must pass two query args:
//  1. `pre-shared-key` for authenticating with spicedb
//  2. `wrapped-conn-str` which is a url encoded query string to pass to the wrapped database.Driver#Open constructor.
//
// Additionally, you'll need to set the wrappedDriver to an empty struct via NewSpiceDBDriver.
// Ex:
// (encoded) spicedb://localhost:50051/?pre-shared-key=123&wrapper-conn-str=postgresql%3A%2F%2Fuser%3Apass%40localhost%3A50052%2Fdb
type SpiceDBDriver struct {
	client client.Client
	logger *log.Logger

	wrappedDriver database.Driver
	schemaReader  schemaReader
}

// exists for testing, constructors are set to os.Readfile
type schemaReader func(fileName string) ([]byte, error)

func NewSpiceDBDriver(wrappedDriver database.Driver, client client.Client, logger *log.Logger) database.Driver {
	return &SpiceDBDriver{wrappedDriver: wrappedDriver, client: client, logger: logger, schemaReader: os.ReadFile}
}

const Scheme = "spicedb"
const PresharedKey = "pre-shared-key"
const WrappedConnStr = "wrapped-conn-str"

type operation string

const (
	add        operation = "add"
	remove     operation = "remove"
	schema     operation = "schema"
	schemaDiff operation = "schema-diff"
)

func newRemoteClient(spicedbKey, spicedbURI string) (*authzed.Client, error) {
	return authzed.NewClient(
		spicedbURI,
		grpcutil.WithInsecureBearerToken(spicedbKey),
		// No TLS..
		grpc.WithTransportCredentials(insecure.NewCredentials()),
	)
}

type parsedUrl struct {
	WrappedConnStr string
	SpiceDBURI     string
	SpiceDBKey     string
}

// ParseURL returns a validated parsed URL for the spicedb driver. This is exposed for testing.
func parseURL(connurl string) (parsedUrl, error) {
	parsed, err := url.Parse(connurl)
	if err != nil {
		return parsedUrl{}, err
	}
	if parsed.Scheme != Scheme {
		return parsedUrl{}, fmt.Errorf("this driver only supports `%s://` schemes", Scheme)
	}
	values := parsed.Query()
	if len(values) > 2 {
		return parsedUrl{}, fmt.Errorf("only %s and %s query args are supported. the presence of more query args may indicate that wrapped-conn-str was not encoded!", PresharedKey, WrappedConnStr)
	}
	wrappedConnStr := values.Get(WrappedConnStr)
	if wrappedConnStr == "" {
		return parsedUrl{}, fmt.Errorf("query arg `%s` required", WrappedConnStr)
	}
	// Unescape the wrappedConnStr to propagate to the wrapped driver. Is this necessary?
	wrappedConnStr, err = url.QueryUnescape(wrappedConnStr)
	if wrappedConnStr == "" {
		return parsedUrl{}, err
	}
	key := values.Get(PresharedKey)
	if key == "" {
		return parsedUrl{}, fmt.Errorf("query arg `%s` required", PresharedKey)
	}
	return parsedUrl{WrappedConnStr: wrappedConnStr, SpiceDBKey: key, SpiceDBURI: parsed.Host}, nil
}

func checkSchemaMd5(givenSum, schematext string) error {
	sum := fmt.Sprintf("%x", md5.Sum([]byte(schematext)))
	if givenSum != sum {
		return errors.New(fmt.Sprintf("checksum mismatch! %s != %s", givenSum, sum))
	}
	return nil
}

func trimAllStrings(parts []string) []string {
	res := make([]string, len(parts))
	for i, part := range parts {
		res[i] = strings.Trim(part, " ")
	}
	return res
}

func hasEmptyStrings(parts []string) bool {
	for _, part := range parts {
		if part == "" {
			return true
		}
	}
	return false
}

func (d *SpiceDBDriver) handleDataMigration(parts []string) error {
	parts = trimAllStrings(parts)
	if len(parts) < 4 || len(parts) > 5 || hasEmptyStrings(parts) {
		return errors.New("invalid data migration; format is: <op>, <resource>, <relation>, <subject>, <optional-subject-relation>")
	}
	op := parts[0]
	resource := parts[1]
	relation := parts[2]
	subject := parts[3]
	optSubRel := ""
	if len(parts) == 5 {
		optSubRel = parts[4]
	}

	resourceParts := strings.Split(resource, ":")
	subjectParts := strings.Split(subject, ":")

	if len(resourceParts) != 2 || len(subjectParts) != 2 || hasEmptyStrings(resourceParts) || hasEmptyStrings(subjectParts) {
		return errors.New("invalid data migration; resource/subject format is: <resource_type>:<resource_id>")
	}

	var grpcOp pb.RelationshipUpdate_Operation
	switch operation(op) {
	case add:
		grpcOp = pb.RelationshipUpdate_OPERATION_CREATE
	case remove:
		grpcOp = pb.RelationshipUpdate_OPERATION_DELETE
	}

	_, err := d.client.WriteRelationships(context.Background(), &pb.WriteRelationshipsRequest{Updates: []*pb.RelationshipUpdate{{
		Operation: grpcOp,
		Relationship: &pb.Relationship{
			Resource: &pb.ObjectReference{ObjectType: resourceParts[0], ObjectId: resourceParts[1]},
			Subject:  &pb.SubjectReference{Object: &pb.ObjectReference{ObjectType: subjectParts[0], ObjectId: subjectParts[1]}, OptionalRelation: optSubRel},
			Relation: relation,
		},
	}}})
	if err != nil {
		return err
	}
	return nil
}

func (d *SpiceDBDriver) handleSchemaMigration(parts []string) error {
	parts = trimAllStrings(parts)
	if len(parts) != 3 || hasEmptyStrings(parts) {
		return errors.New("invalid schema migration; format is: <op>, <path-to-schema>, <hex-encoded-md5sum")
	}
	schemafile := parts[1]
	schemabytes, err := d.schemaReader(schemafile)
	if err != nil {
		return err
	}
	schematext := string(schemabytes)
	sum := parts[2]
	err = checkSchemaMd5(sum, schematext)
	if err != nil {
		return err
	}
	_, err = d.client.WriteSchema(context.Background(), &pb.WriteSchemaRequest{Schema: schematext})
	return err
}

// Open returns a new driver instance configured with parameters
// coming from the URL string. Migrate will call this function
// only once per instance.
func (d *SpiceDBDriver) Open(connurl string) (database.Driver, error) {
	parsed, err := parseURL(connurl)
	if err != nil {
		return nil, err
	}
	// Open the wrapped driver. This should be one of the registered drivers.
	wrapperDriver, err := d.wrappedDriver.Open(parsed.WrappedConnStr)
	if err != nil {
		return nil, err
	}
	client, err := newRemoteClient(parsed.SpiceDBKey, parsed.SpiceDBURI)
	if err != nil {
		return nil, err
	}
	d.wrappedDriver = wrapperDriver
	d.client = client
	d.schemaReader = os.ReadFile
	return d, nil
}

// Close closes the underlying database instance managed by the driver.
// Migrate will call this function only once per instance.
func (d *SpiceDBDriver) Close() error {
	return d.wrappedDriver.Close()
}

// Lock should acquire a database lock so that only one migration process
// can run at a time. Migrate will call this function before Run is called.
// If the implementation can't provide this functionality, return nil.
// Return database.ErrLocked if database is already locked.
func (d *SpiceDBDriver) Lock() error {
	return d.wrappedDriver.Lock()
}

// Unlock should release the lock. Migrate will call this function after
// all migrations have been run.
func (d *SpiceDBDriver) Unlock() error {
	return d.wrappedDriver.Unlock()
}

// Exposed for testing
func (d *SpiceDBDriver) RunMigration(line string) error {
	d.logger.Printf("migrating: %s\n", line)
	parts := strings.Split(line, ",")
	// only validate op field, subhandlers will validate their operations
	if len(parts) < 2 {
		return errors.New("invalid migration given, format is `<operation>, ...args`")
	}
	op := operation(strings.Trim(parts[0], " "))
	if op != add && op != remove && op != schema {
		return fmt.Errorf("invalid operation given %s, allowed operations are: add, remove, schema", op)
	}
	var err error
	switch op {
	case schema, schemaDiff:
		err = d.handleSchemaMigration(parts)
	case add, remove:
		err = d.handleDataMigration(parts)
	default:
		err = fmt.Errorf("invalid operation %s given", op)
	}
	if err != nil {
		return err
	}
	return nil
}

// Run applies a migration to the database. migration is guaranteed to be not nil.
func (d *SpiceDBDriver) Run(migration io.Reader) error {
	scanner := bufio.NewScanner(migration)
	count := 0
	for scanner.Scan() {
		line := scanner.Text()
		err := d.RunMigration(line)
		if err != nil {
			return err
		}
		count += 1
	}
	d.logger.Printf("migrated %d migrations\n", count)
	return nil
}

// SetVersion saves version and dirty state.
// Migrate will call this function before and after each call to Run.
// version must be >= -1. -1 means NilVersion.
func (d *SpiceDBDriver) SetVersion(version int, dirty bool) error {
	return d.wrappedDriver.SetVersion(version, dirty)
}

// Version returns the currently active version and if the database is dirty.
// When no migration has been applied, it must return version -1.
// Dirty means, a previous migration failed and user interaction is required.
func (d *SpiceDBDriver) Version() (version int, dirty bool, err error) {
	return d.wrappedDriver.Version()
}

// Drop deletes everything in the database.
// Note that this is a breaking action, a new call to Open() is necessary to
// ensure subsequent calls work as expected.
func (d *SpiceDBDriver) Drop() error {
	return d.wrappedDriver.Drop()
}
