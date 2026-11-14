# 30: Authenticated controller networking

Loopback HTTP remains the default. Non-loopback controller addresses require all
three flags: `-tls-cert`, `-tls-key`, and `-tls-ca`. Both peer and client listeners
then use TLS 1.3 with verified client certificates. All advertised peer URLs must
use HTTPS; there is no plaintext downgrade or skip-verification flag.

## Identity and authority

Use a **dedicated CA for one cluster**. Do not use a general company-wide client CA.
The leaf certificate must have exactly one URI SAN:

| URI | Access |
| --- | --- |
| `spiffe://distributed-llm-serving/peer/a` | Member `a`; its certificate must also have serverAuth/clientAuth EKUs and DNS/IP SANs for its advertised addresses |
| `spiffe://distributed-llm-serving/admin/operator` | Read and mutate the configuration API, sessions, retention, and diagnostics |
| `spiffe://distributed-llm-serving/reader/gateway` | GET `/config` and `/status`; cannot write or send Raft messages |

The peer endpoint checks that the authenticated identity equals message `From`.
Outbound peer connections verify both the destination hostname and expected member
identity. An admin certificate cannot submit a peer message; a peer certificate
cannot use the configuration client endpoint. This is role authorization, not a
multi-tenant access-control service.

Certificates need normal CA, validity and EKU verification. Protect keys outside
Git; `.secrets/`, `.key` and `.pem` are ignored. The CA controls membership identities
and administrative authority. Compromised peers or CA keys are outside the crash-fault
Raft model; TLS does not make this Byzantine fault tolerant. Certificate renewal
requires restarting with replaced files; revocation/rotation automation is not implemented.

## Provisioning example

Use your own CA tooling for deployment. For an isolated experiment, OpenSSL can make
short-lived credentials. These commands are Linux shell examples. Keep the CA key
off the serving hosts after issuance. Replace IP SANs with actual addresses.

```sh
mkdir -m 700 .secrets
openssl req -x509 -newkey ec -pkeyopt ec_paramgen_curve:P-256 -nodes -days 1 \
  -subj /CN=fleet-test-ca -keyout .secrets/ca.key -out .secrets/ca.pem
openssl req -new -newkey ec -pkeyopt ec_paramgen_curve:P-256 -nodes \
  -subj /CN=peer-a -keyout .secrets/a.key -out .secrets/a.csr
printf '%s\n' 'basicConstraints=critical,CA:FALSE' 'keyUsage=critical,digitalSignature' \
  'extendedKeyUsage=serverAuth,clientAuth' \
  'subjectAltName=IP:10.10.0.11,URI:spiffe://distributed-llm-serving/peer/a' > .secrets/a.cnf
openssl x509 -req -in .secrets/a.csr -CA .secrets/ca.pem -CAkey .secrets/ca.key \
  -CAcreateserial -days 1 -extfile .secrets/a.cnf -out .secrets/a.pem
chmod 600 .secrets/*.key
```

Repeat issuance with a distinct key and URI for `b`, `c`, `admin/operator`, and
`reader/gateway`. Admin/reader leaves need clientAuth. This is disposable test
provisioning, not an operational certificate lifecycle.

Start each member with its own durable directory and key. The complete fixed
membership and retained group must agree across members:

```sh
bin/raftpeer -configuration -id a \
  -peers a=https://10.10.0.11:19201,b=https://10.10.0.12:19201,c=https://10.10.0.13:19201 \
  -peer-listen 0.0.0.0:19201 -client-listen 0.0.0.0:19301 \
  -retained-dir data/a -retained-group example-cluster \
  -tls-cert .secrets/a.pem -tls-key .secrets/a.key -tls-ca .secrets/ca.pem
```

Poll from the gateway with reader credentials:

```sh
bin/gateway -backend http://127.0.0.1:18081 \
  -controllers https://10.10.0.11:19301,https://10.10.0.12:19301,https://10.10.0.13:19301 \
  -controller-tls-cert .secrets/gateway.pem -controller-tls-key .secrets/gateway.key \
  -controller-tls-ca .secrets/ca.pem
curl --cacert .secrets/ca.pem --cert .secrets/operator.pem --key .secrets/operator.key \
  https://10.10.0.11:19301/config
```

Firewall peer listeners to the fixed members and client listeners to intended
clients. Do not expose the unauthenticated model engines or gateway serving API
directly to the public Internet; application ingress authentication is a separate
deployment responsibility. The gateway's admin listener remains loopback-only.

## Checks and their limits

```sh
python scripts/check_secure_peers.py bin/raftpeer --gateway bin/gateway \
  --output artifacts/network/local.json
sudo python3 scripts/check_secure_peers.py bin/raftpeer --gateway bin/gateway \
  --network-namespaces --output artifacts/network/namespaces.json
```

The first command also runs on Windows (`.exe` paths) using OpenSSL from PATH or
Git for Windows. It creates disposable certificates, starts three owned processes,
checks authenticated replication and retention, rejects missing/untrusted client
certificates and forged identities, verifies gateway polling, kills a leader, and
restarts the cluster. No model execution is claimed by this control-plane check.

Namespace mode requires Linux root with `ip` and `tc`. It gives each peer its own
network stack and IP, adds 15 ms delay on each side of its veth pair, cuts traffic
to/from one peer, confirms majority progress, then heals and checks catch-up.
Only its uniquely named namespaces/devices are removed. CI runs both modes and
uploads raw reports. Check the evidence ledger for successful source-specific runs.

Separate network stacks on one kernel are useful fault injection. They do **not**
demonstrate independent-host, physical NIC, rack, power, or disk-failure resilience.
The same secure commands support a later independent-machine experiment without
relaxing loopback safety. That experiment still requires actual hosts.
