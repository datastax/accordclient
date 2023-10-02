# Jepsen-based transaction consistency tests for Accord

This application is based on a [previous version](https://github.com/jkni/lwtclient) testing Cassandra's LWT transaction consistency.

There are several models supported by various Jepsen transactional consistency checkers:
- [rw-register](https://github.com/jepsen-io/elle/blob/main/src/elle/rw_register.clj) ([Elle](https://github.com/jepsen-io/elle) checker)
- [list-append](https://github.com/jepsen-io/elle/blob/main/src/elle/list_append.clj) (Elle checker)
- cas-register ([Knossos](https://github.com/jepsen-io/knossos/tree/master) checker)
- mutex (Knossos checker)
- bank (Jepsen’s checker)
- counter (Jepsen’s checker)
- long-fork (Jepsen’s checker)
- set (Jepsen’s checker)
- set-full (Jepsen’s checker)
- comments (custom checker)
- sequential (standalone checker)

All these various checkers can be run either from code in Clojure and Java or via a standalone CLI application called [elle-cli](https://github.com/ligurio/elle-cli).
The [Jepsen History](https://github.com/jepsen-io/history) library requires files to be written in the [edn](https://github.com/edn-format/edn) format.

Currently a Clojure client was written, based on [an internally written application to check LWT transactions with Knossos](https://github.com/jkni/lwtclient)
against the cas-register model. The new client is able to run workloads against Cassandra and create edn files for the following models:
- cas-register
- rw-register
- list-append

## Remaining action items
- run a double-op workload (read-read, read-followed-by-write, write-write) against the list-append model containing 2 operations in a transaction, like:
``` 
[[:r 5 nil] [:append 1 1]] or
[[:append 2 2] [:append 4 3]]
```
- run a mixed workload (all reads precede any writes) against the rw-register model containing more than 2 operations in a transaction, like two reads followed by three writes (see notes in the code for the :mix case):
```
[[:r 5 nil] [:r 3 nil] [:w 1 1] [:w 2 2] [:w 4 3]]
```
- run a mixed workload (all reads precede any writes) against the list-append model containing more than 2 operations in a transaction, like two reads followed by three appends:
```
[[:r 5 nil] [:r 3 nil] [:append 1 1] [:append 2 2] [:append 4 3]]
```
## Blocked action items due to Accord limitations
- run a read-after-write workload against the rw-register model (see notes in the code for the :single-wr-mix case):
```
- [[:w 1 1] [:r 1 nil]]
```
- run a mixed workload against the rw-register model containing more than 2 operations in a transaction (see notes for :mix case):
``` 
[[:r 5 nil] [:w 3 1] [:w 4 2] [:r 4 mil] [:r 3 nil]]
```
- run a read-after-write workload against the list-append model:
```
[[:append 1 1] [:r 1 nil]]
```
- run a mixed workload against the list-append model containing more than 2 operations in a transaction:
```
[[:r 5 nil] [:append 3 1] [:append 4 2] [:r 4 mil] [:r 3 nil]]
```

## cas-register
The _cas-register_ model is implemented as a table with two integer type columns, one of which is the primary key:
```
CREATE TABLE IF NOT EXISTS accord.cas_registers (
id int PRIMARY KEY,
contents int
);
```
The goal is to run operations in a transactional fashion. In case of reads this means to run the SELECT statement enclosed by a TRANSACTION. For writes the IF IS NULL / IF IS NOT NULL statements are used to run the INSERT / UPDATE commands respectively.Finally for compare-and-set operations IF is used for the compare logic.
The client runs these transactions and reports the results in the edn format for later evaluation.
This line:
```
{:type :invoke, :f :read, :process 1, :value nil, :register 5, :time 3956778600}
```
means that one thread is running a process with id “1”. As part of this process a “read” operation has been invoked against the register with id “5”.

```
{:type :ok, :f :read, :process 1, :value nil, :register 5, :time 3984328200}
```
This line means that the operation was a success and the retrieved value is null, because there was no row with register id “5” in the table. This is expected, as at the beginning of the test, the table has to be empty.
Similarly, this line:
```
{:type :invoke, :f :cas, :process 1, :value [3 0], :register 5, :time 3984724700}
```
means that a compare-and-set operation has been invoked against the row with register id = “5”. If the current value is “3”, then it will be updated to “0”. This line in reply:
```
{:type :fail, :f :cas, :process 1, :value [3 0], :register 5, :time 4012765800}
```
means that the cas operation failed, because the current value was not “3”.

### Workload
There are three workloads randomly picked for each iteration:
- read
```
BEGIN TRANSACTION
LET row = (SELECT * FROM cas_registers WHERE id = ?);
SELECT row.contents;
COMMIT TRANSACTION;
```
- update if exists, otherwise write
```
BEGIN TRANSACTION
LET row = (SELECT * FROM cas_registers WHERE id=?);
SELECT row.contents;
IF row IS NOT NULL THEN
UPDATE cas_registers SET contents=? WHERE id=?;
END IF
COMMIT TRANSACTION;

BEGIN TRANSACTION
LET row = (SELECT * FROM cas_registers WHERE id=?);
SELECT row.contents;
IF row IS NULL THEN
INSERT INTO cas_registers(id, contents) VALUES (?, ?);
END IF
COMMIT TRANSACTION;
```
- compare-and-set
```
- BEGIN TRANSACTION
LET row = (SELECT * FROM cas_registers WHERE id = ?);
SELECT row.contents;
IF row.contents = ? THEN
UPDATE cas_registers SET contents = ? WHERE id = ?;
END IF
COMMIT TRANSACTION;
```
### Execution
2 threads, 100 cycles (50 cycles per thread), register ids = {1,2,3,4,5}
```
# cqlsh -e "truncate accord.cas_registers"
# lein run --cas-register -t 2 -n 100 -r 1,2,3,4,5 -s 1000 > cas-test.edn
```
### Output (excerpt)
```
{:type :invoke, :f :read, :process 1, :value nil, :register 5, :time 3956778600}
{:type :invoke, :f :read, :process 2, :value nil, :register 2, :time 3965300800}
{:type :ok, :f :read, :process 1, :value nil, :register 5, :time 3984328200}
{:type :invoke, :f :cas, :process 1, :value [3 0], :register 5, :time 3984724700}
{:type :ok, :f :read, :process 2, :value nil, :register 2, :time 3989305300}
{:type :invoke, :f :read, :process 2, :value nil, :register 3, :time 3990437000}
{:type :ok, :f :read, :process 2, :value nil, :register 3, :time 4005486100}
...
{:type :ok, :f :read, :process 1, :value 0, :register 1, :time 5860270300}
{:type :invoke, :f :write, :process 1, :value 2, :register 3, :time 5860434200}
{:type :ok, :f :write, :process 1, :value 2, :register 3, :time 5872023200}
{:type :invoke, :f :write, :process 1, :value 3, :register 4, :time 5872164700}
{:type :ok, :f :write, :process 1, :value 3, :register 4, :time 5883704300}
```
### Check
Each register needs to be checked separately, since Knossos does not understand the concept of registers and expects all operations to be run against one register:
```
# for reg_id in {1..5}; do grep register\ $reg_id cas-test.edn > cas-test-r$reg_id.edn; done
```

The elle-cli application will use the Knossos checker for the cas-register model:
```
# for edn_file in cas-test-r*; do echo "Validating $edn_file:"; java -jar target/elle-cli-0.1.6-standalone.jar --model cas-register --anomalies G0 --consistency-models strict-serializable --directory out_anomalies --verbose $edn_file; echo ""; done

Validating cas-test-r1.edn:
{"valid?":true, ...}
...
Validating cas-test-r5.edn:
{"valid?":true, ...}
```


## rw-register
This model is very similar to the cas-register model.
The schema for rw-register is:
```
CREATE TABLE IF NOT EXISTS accord.rw_registers
(
id int PRIMARY KEY,
contents bigint
);
```
The difference is in the output format tailored for the Elle checker:
```
{:type :invoke, :f txn, :process 2, :value [[:w 3 251]], :tid 1, :step 1, :time 3919942600}
```
This output means a write operation tries to update the value of the register with id “3” to “251”. Values are unique across all threads, as required by the Elle checker specification.


The next line runs two operations within a single transaction:
```
{:type :invoke, :f txn, :process 20, :value [[:r 1 nil] [:w 5 45]], :tid 0, :step 47, :time 40934090200}
```
It reads the value from the register with id “1” and updates the value of the register with id “5” to “45”. The “tid” parameter stands for “transaction id” and “step” is the cycle this thread is executing.

### Workload
These are the workloads run against the rw-register model, randomly picked for each iteration:
- single read
```
BEGIN TRANSACTION
LET row = (SELECT * FROM rw_registers WHERE id = ?);
SELECT row.contents;
COMMIT TRANSACTION;
```
- single write
```
BEGIN TRANSACTION
LET row = (SELECT * FROM rw_registers WHERE id=0);
SELECT row.contents;
IF row IS NULL THEN
INSERT INTO rw_registers(id, contents) VALUES (?, ?);
END IF
COMMIT TRANSACTION;
```
- double read
```
BEGIN TRANSACTION
LET row1 = (SELECT * FROM rw_registers WHERE id=?);
LET row2 = (SELECT * FROM rw_registers WHERE id=?);
SELECT row1.contents,row2.contents;
COMMIT TRANSACTION;
```
- write after read
```
BEGIN TRANSACTION
LET row = (SELECT * FROM rw_registers WHERE id=?);
SELECT row.contents;
INSERT INTO rw_registers(id, contents) VALUES (?, ?);
COMMIT TRANSACTION;
```
- double write
```
BEGIN TRANSACTION
LET row = (SELECT * FROM rw_registers WHERE id=0);
SELECT row.contents;
INSERT INTO rw_registers(id, contents) VALUES (?, ?);
INSERT INTO rw_registers(id, contents) VALUES (?, ?);
COMMIT TRANSACTION;
```

### Execution
2 threads, 100 cycles (50 cycles per thread), register ids = {1,2,3,4,5}
```
# cqlsh -e "truncate accord.rw_registers"
# lein run --rw-register -t 2 -n 100 -r 1,2,3,4,5 -s 1000 > rw-test.edn
```
### Output (excerpt)
```
{:type :invoke, :f txn, :process 1, :value [[:w 1 1]], :tid 0, :step 1, :time 3913334400}
{:type :invoke, :f txn, :process 2, :value [[:w 3 251]], :tid 1, :step 1, :time 3919942600}
{:type :ok, :f txn, :process 1, :value [[:w 1 1]], :tid 0, :step 1, :time 8861703100}
{:type :invoke, :f txn, :process 1, :value [[:w 1 2] [:w 1 3]], :tid 0, :step 2, :time 8862245900}
{:type :info, :f txn, :process 2, :value [[:w 3 251]], :tid 1, :step 1, :time 10941719500, :cause :write-timed-out}
{:type :invoke, :f txn, :process 3, :value [[:w 2 252]], :tid 1, :step 2, :time 10942204900}
...
{:type :ok, :f txn, :process 21, :value [[:r 5 45]], :tid 1, :step 47, :time 41640045800}
{:type :invoke, :f txn, :process 21, :value [[:r 1 nil] [:r 4 nil]], :tid 1, :step 48, :time 41640366300}
{:type :ok, :f txn, :process 21, :value [[:r 1 287] [:r 4 46]], :tid 1, :step 48, :time 41690583200}
{:type :invoke, :f txn, :process 21, :value [[:w 4 289]], :tid 1, :step 49, :time 41690857500}
{:type :ok, :f txn, :process 21, :value [[:w 4 289]], :tid 1, :step 49, :time 42051760500}
{:type :invoke, :f txn, :process 21, :value [[:r 2 nil]], :tid 1, :step 50, :time 42051967600}
{:type :ok, :f txn, :process 21, :value [[:r 2 43]], :tid 1, :step 50, :time 42085080900}
```
### Check
The elle-cli application will use the Elle checker for the rw-register model:
```
# java -jar target/elle-cli-0.1.6-standalone.jar --model rw-register --anomalies G0 --consistency-models strict-serializable --directory out_anomalies --verbose rw-test.edn

{"valid?":true}
```

## list-append
According to the documentation:
_The append test models the database as a collection of named lists, and performs transactions comprised of read and append operations. A read returns the value of a particular list, and an append adds a single unique element to the end of a particular list. We derive ordering dependencies between these transactions, and search for cycles in that dependency graph to identify consistency anomalies._

_In terms of Elle values in operation are lists of integers. Each operation performs a transaction, comprised of micro-operations which are either reads of some value (returning the entire list) or appends (adding a single number to whatever the present value of the given list is). We detect cycles in these transactions using Elle's cycle-detection system._

The schema for list-append is:
```
CREATE TABLE IF NOT EXISTS accord.list_append
(
id int PRIMARY KEY,
contents LIST<bigint>
);
```

### Workload
These are the workloads run against the list_append model, randomly picked for each iteration:
- single read
```
BEGIN TRANSACTION
LET row = (SELECT * FROM list_append WHERE id = ?);
SELECT row.contents;
COMMIT TRANSACTION;
```
- single write
```
BEGIN TRANSACTION
LET row = (SELECT * FROM list_append WHERE id=0);
SELECT row.contents;
IF row IS NULL THEN
UPDATE list_append SET contents += [%d] WHERE id = %d;
END IF
COMMIT TRANSACTION;
```

### Execution
2 threads, 100 cycles (50 cycles per thread), register ids = {1,2,3,4,5}
```
# cqlsh -e "truncate accord.list_append"
# lein run --list-append -t 2 -n 100 -r 1,2,3,4,5 -s 1000 > la-test.edn
```

### Output (excerpt)
```
{:type :invoke, :process 1, :value [[:r 3 nil]], :tid 0, :n 1, :time 3781122300}
{:type :invoke, :process 2, :value [[:append 4 51]], :tid 1, :n 1, :time 3788663200}
{:type :ok, :process 1, :value [[:r 3 []]], :tid 0, :n 1, :time 3856102700}
{:type :invoke, :process 1, :value [[:r 4 nil]], :tid 0, :n 2, :time 3856595100}
{:type :ok, :process 2, :value [[:append 4 51]], :tid 1, :n 1, :time 3932329600}
{:type :invoke, :process 2, :value [[:r 1 nil]], :tid 1, :n 2, :time 3932769400}
{:type :ok, :process 1, :value [[:r 4 [51]]], :tid 0, :n 2, :time 3952088500}
...
{:type :ok, :process 15, :value [[:append 1 48]], :tid 0, :n 48, :time 15835102500}
{:type :invoke, :process 15, :value [[:append 2 49]], :tid 0, :n 49, :time 15835356800}
{:type :ok, :process 15, :value [[:append 2 49]], :tid 0, :n 49, :time 15877012900}
{:type :invoke, :process 15, :value [[:r 4 nil]], :tid 0, :n 50, :time 15877259300}
{:type :ok, :process 15, :value [[:r 4 [51 58 70 74 79 95 34 46 47]]], :tid 0, :n 50, :time 15888631400}
```
### Check
The elle-cli application will use the Elle checker for the list-append model:
```
# java -jar target/elle-cli-0.1.6-standalone.jar --model list-append --anomalies G0 --consistency-models strict-serializable --directory out_anomalies --verbose la-test.edn

{"valid?":true}
```

### Anomalies
Currently, one anomaly found by the `list-append` test is investigated (see this [Jira ticket](https://issues.apache.org/jira/browse/CASSANDRA-18798)).