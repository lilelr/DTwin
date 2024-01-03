## TPC-DS trace generation

### conditional_probability.py
generating the synthetic branches given a input length. 

### lele_dag_post_order.py
traversaling the physical plan of a query and output the branch coding of this query.
Output:    code_tree_TPC_DS_branch_number_4.txt

### lele_dag_sql_output_to_file.py
generating the critical paths encoded by letters like "a,b,c,d " according to the DAG structure of a query. 

### tpc_ds_tables.py
tpc-ds tables definitions

### database.py
input table definitions

### synthesize_tables.py
JoinTable definitions

### sythesize_nodes.py
SynthesizedNode defintions

#### lstm_sequences_generating.py
LSTM train ad build the model. 

### random_behaviors_util.py 
common operations for join, aggregate.

### synthesize_sql_from_generated_plan_new.py
generate the sql with varied number of tables in a single branch

### lstm_sequence_part_of_speech_tagging.ipynb
testing file






## Trace generator related
### core/dag_generation/trace_parsePlan_lele_cp-tpcds-operators.py 
### core/dag_generation/trace_generate_sql.py
generating the sql according to a given dag from trace.

### trace_lstm_sequences_generating.py 
generating the lstm model for physical plan
### trace_lstm_sequence_part_of_speech_tagging.ipynb 
is the corresponding juptyer notebook


## run sql
### veriton
/home/lemaker/open-source/datagrip_projects/hello_hive
./run_cbg_ben.sh qg

### slave4
~/open-source/BigBench-spark3/tpcds-run$ ./Tpcds_q_dag_100G.sh 
/home/lemaker/open-source/BigBench-spark3/querySamples/tpcds/q_dag.sql