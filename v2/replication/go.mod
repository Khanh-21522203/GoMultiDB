module GoMultiDB/v2/replication

go 1.23.0

require (
	GoMultiDB/v2/contracts v0.0.0
	GoMultiDB/v2/infra v0.0.0
)

replace GoMultiDB/v2/contracts => ../contracts
replace GoMultiDB/v2/infra => ../infra
