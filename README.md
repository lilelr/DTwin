# DTwin

DTwin is a SparkSQL generator that generates SQL queries by cloning the performance of production queries. 
First of all,  DTwin collects the physical plans without confidential information from the given queries. Subsequently, DTwin takes as input the information of physical plans and generate the queries based on the TPC-DS dataset.  
We use two sets of queries to evaluate DTwin. The first is 10 Huawei Trace queries and the other is 103 TPC-DS queries. 

### Prerequisite

- **python3**
    - pip install graphviz
    

### Launching

The main entries are two scripts. **trace_synthesize_sql_from_generated_plan_new.py** is for Huawei trace while **synthesize_sql_from_generated_plan_new.py** for TPC-DS queries. Both have four identical arguments as follows.  **DTwin would output the the generated queries in the current working director with a file with suffix ".sql".**

Note that the length of the critical path is set with 3 and edge density is set with 0.4 by default according to the trace analysis.


```bash
optional arguments:
  -h, --help       show this help message and exit
  --num NUM        number of generated queries
  --length LENGTH  the length of critical path
  --ed ED          the edge density between [0,1]

```

#### Generating the queries by cloning performance from Huawei Trace

For example, generating four queries by cloning performance from Huawei Trace with length of critical path of 3 and edge density of 0.4. 

```bash
python3 trace_synthesize_sql_from_generated_plan_new.py --num 4 --length 3 --ed 0.4
```

#### Generating the queries by cloning performance from 103 TPC-DS queries


For example, generating 10 queries by cloning performance from TPC-DS queries with length of critical path of 5 and edge density of 0.2. 

```bash
python3 synthesize_sql_from_generated_plan_new.py --num 10 --length 5 --ed 0.2
```


### Code description

**trace_parsePlan_lele_cp-tpcds-operators.py 
trace_generate_sql.py**

generating the sql according to a given dag from trace.


**tpc_ds_tables.py**

tpc-ds tables definitions

**database.py**

input table definitions

**synthesize_tables.py**

JoinTable definitions

**sythesize_nodes.py**

SynthesizedNode defintions

**lstm_sequences_generating.py**

LSTM train ad build the model. 

**random_behaviors_util.py**

common operations for join, aggregate.

**enum_operator.py**

type of physical operators in the physical plan

