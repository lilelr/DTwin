# DTwin

DTwin is a SparkSQL generator that generates SQL queries by cloning the performance of a target application. 
First of all,  DTwin collects the opertaor plans (i.e., physical plans without confidential information) from the target application, a production application from Huawei Cloud or TPC-DS benchmark. Subsequently, DTwin takes as input the opertaor plans and generate the queries based on the public TPC-DS dataset.  
We use two sets of queries to evaluate DTwin. The first is 10 Huawei Trace queries and the other is 103 TPC-DS queries. 

### Prerequisite

- **python3**
    - pip install numpy graphviz
    
### Data preparation 

Take the TPC-DS for example. We need to convert their physical plans to operator plans without confidential information. 

```bash
python3 parsePlan_lele_cp-tpcds-100G.py
````
After running the script, we output the operator plans of the TPC-DS in the subdirectory called **tpcds-99queries**.

Hence, DTwin will profile these operator plans to collect the distributions of branch count, operator sequences, and etc. 


### Query Generation

The main entries are two scripts. **trace_synthesize_sql_from_generated_plan_new.py** is used for generating queries cloning the performance for the Huawei production application while **synthesize_sql_from_generated_plan_new.py** for TPC-DS benchmark. Both have one important argument as follows.  **DTwin would output the the generated queries in the current working director with a file with suffix ".sql".**

Note that the length of the critical path is set with 3 and edge density is set with 0.4 by default according to the trace analysis.


```bash
optional arguments:
  -h, --help       show this help message and exit
  --num NUM        number of generated queries


```

#### Generating the queries by cloning performance from 103 TPC-DS queries


For example, generating 10 queries by cloning performance from TPC-DS queries. 

```bash
python3 synthesize_sql_from_generated_plan_new.py --num 8 
```

After that, DTwin will ouput 8 operator plans and 8 queries. We provide an example output listed in the subdirectoroy  **tpcds_generated_sql_plans**.

#### Generating the queries by cloning performance from Huawei Trace

For example, generating four queries by cloning performance from the Huawei prodcution application. 

```bash
python3 trace_synthesize_sql_from_generated_plan_new.py --num 4
```




### Code description

**trace_parsePlan_lele_cp-tpcds-operators.py 
trace_generate_sql.py**

generating the sql according to a given operator plan from the production application.


**tpc_ds_tables.py**

TPC-DS tables definitions

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

