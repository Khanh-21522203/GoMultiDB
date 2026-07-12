module GoMultiDB/v2/server

go 1.23.0

require (
	GoMultiDB/v2/contracts v0.0.0
	GoMultiDB/v2/infra v0.0.0
	GoMultiDB/v2/engine v0.0.0
	GoMultiDB/v2/gateway v0.0.0
)

replace GoMultiDB/v2/contracts => ../contracts
replace GoMultiDB/v2/infra => ../infra
replace GoMultiDB/v2/engine => ../engine
replace GoMultiDB/v2/gateway => ../gateway
