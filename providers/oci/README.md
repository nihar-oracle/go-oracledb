# OCI IAM database token provider

This module obtains OCI IAM scoped-access tokens for Oracle Database token authentication. It is standalone: it has no dependency on a particular `go-oracledb` release and satisfies the driver's signed-token-provider interface structurally.

## Configuration

`Config.Principal` selects one of these OCI IAM authentication modes:

- `oci.InstancePrincipal`
- `oci.ResourcePrincipal`
- `oci.OKEWorkloadIdentity`
- `oci.ConfigProfile`

For `ConfigProfile`, set `ConfigProfile`; `ConfigFile` is optional and defaults to the OCI SDK configuration-file location. Set `Scope` to use an explicit scoped-access-token scope. Otherwise, `CompartmentOCID` and `DatabaseOCID` derive the database scope as `urn:oracle:db::id::<compartment>::<database>`. `Region` overrides the region reported by the selected principal.

## Connector usage

```go
provider, err := oci.New(oci.Config{
    Principal:       oci.InstancePrincipal,
    CompartmentOCID: compartmentOCID,
    DatabaseOCID:    databaseOCID,
    Region:          "us-ashburn-1", // optional
})
if err != nil {
    return err
}

connector, err := oracle.NewOracleConnector(driverConfig)
if err != nil {
    return err
}
registrar, ok := connector.(providers.ProviderRegistrar)
if !ok {
    return fmt.Errorf("connector does not support runtime providers")
}
registrar.RegisterProvider(provider)
db := sql.OpenDB(connector)
```

The imports in the example are `database/sql`, `fmt`, `github.com/oracle/go-oracledb/v26/oracle`, `github.com/oracle/go-oracledb/v26/oracle/providers`, and `github.com/oracle/go-oracledb/providers/oci`.

## Token and key lifecycle

`Token` reuses a token while it has more than five minutes remaining. When it refreshes, the provider creates a new RSA key and sends its public half to OCI. It retains the private key for every token it returned until that token expires, so `PrivateKeyForToken` resolves the exact token/key pair during in-flight connection work. Expired and unknown tokens are rejected. The provider is safe for concurrent use and does not log tokens or key material itself; applications should also avoid enabling OCI SDK debug logging for credential flows.
