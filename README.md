Simple Java RDM Symbol List publisher in RFA, provides a single symbol list populated by the first column of a CSV file.

usage:

```bash
	cd src
	./run.sh --symbol-path=10.csv  --session=rssl://nylabadh2/NI_VTA --symbol-list=NYSE
```

example consumption:

```bash
rsslConsumer.exe -h nylabads2 -p 14002 -s NI_VTA -sl NYSE
Proxy host:
Proxy port:

Input arguments...

Using Connection Type = 0
srvrHostname: nylabads2
srvrPortNo: 14002
serviceName: NI_VTA

Attempting to connect to server nylabads2:14002...

Attempting to connect to server nylabads2:14002...

Channel IPC descriptor = 124

Channel 124 In Progress...

Channel 124 Is Active
Connected to ads3.0.0.L1.linux.rrg 64-bit device.
Ping Timeout = 30

Received Login Response for Username: reutadmin
        State: Open/Ok/None - text: "Login accepted by host nylabads2."


Received Source Directory Response
        State: Open/Ok/None - text: ""

Received serviceName: NI_VTA


Received Item StatusMsg for stream 400
        State: Open/Suspect/None - text: "A17: Service is down. Will call when service becomes available."


Received Source Directory Update

NYSE
DOMAIN: RSSL_DMT_SYMBOL_LIST
State: Open/Ok/None - text: ""
AA_pb.N RSSL_MPEA_ADD_ENTRY
AAV.N   RSSL_MPEA_ADD_ENTRY
AAP.N   RSSL_MPEA_ADD_ENTRY
AA.N    RSSL_MPEA_ADD_ENTRY
AAC.N   RSSL_MPEA_ADD_ENTRY
ABB.N   RSSL_MPEA_ADD_ENTRY
AB.N    RSSL_MPEA_ADD_ENTRY
A.N     RSSL_MPEA_ADD_ENTRY
AAT.N   RSSL_MPEA_ADD_ENTRY
AAN.N   RSSL_MPEA_ADD_ENTRY
```
