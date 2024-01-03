import sys, os

from chain_pre_processing import generate_long_chain
from sythesize_nodes import SynthesizedNode
from core.enum_operator import Operator
import random
from database import Table, Field, Source
from tpc_ds_tables import store_returns, date_dim, store_sales, time_dim, item, customer, customer_demographics, \
    customer_address, household_demographics, store, promotion, reason, catalog_sales
from synthesize_tables import JoinTable
import copy
import time

import logging
import random_behaviors_util

table_index = 1


def get_with_table_index():
    global table_index
    index = table_index
    table_index = table_index + 1
    return str(index)


class TraceSQLGenerator:
    def __init__(self, top_syn_nodes: list[SynthesizedNode]):
        self.top_nodes = top_syn_nodes
        self.top_nodes_count = len(self.top_nodes)
        self.join_nodes = []

    def bind_tables_to_top_nodes(self):
        """
        handling the top le-level nodes until it meets the join nodes.
        :rtype: object
        """
        connected_tables_store_returns = [date_dim, time_dim, item, customer, customer_demographics,
                                          household_demographics, customer_address, reason, store_sales]
        main_table = store_returns
        random.shuffle(connected_tables_store_returns)
        actual_tables = connected_tables_store_returns[:self.top_nodes_count]
        previous_node = None
        for index, node in enumerate(self.top_nodes):
            while node:
                if node.operator == Operator.FileScan or node.operator == Operator.ReusedExchange:
                    # if node.operator == Operator.ReusedExchange:
                    #     print("debug ReusedExchange")
                    if index == 0:  # index=0 represent the long chain
                        node.input_table = main_table
                        node.input_table.with_table_name = "tb" + get_with_table_index()
                    else:
                        # handle the ReusedExchange operation, we regard it as an FileScan operation
                        node.input_table = actual_tables.pop(0)
                        node.input_table.with_table_name = "tb" + get_with_table_index()
                    previous_node = node
                elif node.operator == Operator.Filter:
                    node.input_table = previous_node.input_table
                    random.shuffle(node.input_table.where_fields)
                    previous_node = node
                elif node.operator == Operator.HashAggregate:
                    node.input_table = previous_node.input_table
                    print("hash_aggregate")
                    if not node.input_table.hash_agg:
                        random_behaviors_util.handle_aggregate_operator_input_table(node.input_table)
                    previous_node = node
                # elif node.operator == Operator.Project:
                #     print("Project")
                #     temp_len = len(current_join_table.t1_table.fields)
                #     random_number = random.randint(2, temp_len)
                #     tmp_pro_list = random.sample(current_join_table.t1_table.fields, random_number)
                #
                #     current_join_table.project_field = tmp_pro_list
                #     next_node.join_table = current_join_table
                #     previous_node = node
                elif node.operator == Operator.Window:
                    print("Window")
                    node.input_table = previous_node.input_table
                    pre_ancestor = previous_node.parent_node[0]
                    if pre_ancestor.operator == Operator.Exchange:

                        # if next_node.parent_node[0].parent_node[0]==Operator.Expand
                        window_fields = random_behaviors_util.handle_window_operator_input_table(node.input_table)
                        node.window_fields = window_fields
                    else:
                        last_field = node.window_fields[-1]
                        node.window_fields.append(last_field)

                    previous_node = node

                elif node.operator == Operator.BroadcastHashJoin or node.operator == Operator.SortMergeJoin:
                    # node.input_table = previous_node.input_table
                    # previous_node = node
                    if index == 0:
                        if node.join_table.t1_table is None and node.join_table.t2_table is None:
                            node.join_table = JoinTable(t1_table=previous_node.input_table, t2_table=None,
                                                        with_table_name=get_with_table_index())
                            self.join_nodes.append(node)  # append the node to the join nodes list
                        else:
                            node.join_table.t1_table = previous_node.input_table
                        node.join_table.relationships = node.join_table.t1_table.relationships
                        node.join_table.fields = node.join_table.t1_table.fields
                        node.join_table.join_field_str = node.join_table.t1_table.join_field_str
                    else:
                        if node.join_table.t1_table and node.join_table.t2_table is None:
                            node.join_table.t2_table = previous_node.input_table
                        elif node.join_table.t1_table is None and node.join_table.t2_table:
                            node.join_table.t1_table = previous_node.input_table
                        else:
                            # both node.join_table_t1_table and t2_table are none
                            # we create the new join table and prefer to bind t2_table
                            node.join_table = JoinTable(t1_table=None, t2_table=previous_node.input_table,
                                                        with_table_name=get_with_table_index())

                            self.join_nodes.append(node)  # append the node to the join nodes list
                    break  # break when encountering the join nodes
                elif node.operator == Operator.Union:
                    # if node.join_table.t1_table is None:
                    if index == 0:  # if it is the long chain,we bind the table. Otherwise, we do not bind any table
                        t1_table = previous_node.input_table
                        # t_2_table = copy.deepcopy(t1_table)
                        node.join_table = JoinTable(t1_table=t1_table, t2_table=t1_table,
                                                    with_table_name=get_with_table_index())
                        node.join_table.is_union = True  # mark this is a union node
                        node.join_table.relationships = t1_table.relationships
                        node.join_table.fields = t1_table.fields
                        node.join_table.join_field_str = t1_table.join_field_str
                        self.join_nodes.append(node)  # append the node to the join nodes list
                        node.join_table.union_count_tables += 1
                    else:
                        # node.join_table = JoinTable(t1_table=None, t2_table=None,
                        #                             with_table_name=get_with_table_index())
                        node.join_table.is_union = True  # mark this is a union node
                        node.join_table.union_count_tables += 1
                        if not node in self.join_nodes:
                            self.join_nodes.append(node)  # append the node to the join nodes list
                        # t2_table = previous_node.input_table
                        # t1_table = node.join_table.t1_table
                        # # keep t2_table have the same fields as t1_table so that "union all" can operate successfully
                        # random_behaviors_util.handle_union_operator(t1_table, t2_table)
                        # node.join_table.t2_table = t2_table
                    break
                else:
                    node.input_table = previous_node.input_table
                    previous_node = node
                node = node.child_node

    def generate_sql_from_join_nodes(self):
        final_sql = "use tpcds_10_parquet;  \n"
                    # " WITH "

        if len(self.join_nodes) ==1:
            return self.join_nodes[0], final_sql
        final_sql += " WITH "
        while self.join_nodes:
            current_node = self.join_nodes.pop(0)
            current_join_table = current_node.join_table  # save the current_node.join_table

            if current_join_table.t1_table is None or current_join_table.t2_table is None:
                self.join_nodes.append(current_node)
                continue
            else:
                if current_join_table.is_union:  # handle the union node
                    # continue
                    # final_sql += "\n" + current_join_table.print_sql(use_as=True) + " , "
                    final_sql += "\n"
                else:  # the normal hash_join or sort_merge_join

                    current_join_table.join_fields.append(current_join_table.t1_table.join_field_str)
                    current_join_table.join_fields.append(current_join_table.t2_table.join_field_str)
                    t1_sql = current_join_table.t1_table.print_sql(use_as=True)
                    t2_sql = current_join_table.t2_table.print_sql(use_as=True)


                    final_sql += "\n" + t1_sql + " , \n " + t2_sql + " , "

            next_node = current_node.child_node

            while True:
                if next_node is None or next_node.operator == Operator.TakeOrderedAndProject:
                    return current_node, final_sql  # result
                if next_node.operator == Operator.BroadcastHashJoin or next_node.operator == Operator.SortMergeJoin:
                    join_table = next_node.join_table
                    if join_table.t1_table is None:
                        join_table.t1_table = current_join_table
                        join_table.relationships = current_join_table.relationships
                        join_table.fields = current_join_table.fields
                        join_table.join_field_str = current_join_table.join_field_str
                        if join_table.t2_table is None:  # pretty new join node
                            self.join_nodes.append(next_node)
                            # this next_node has never been visited, add it to the join_ndoes

                    elif join_table.t2_table is None:  # has a table already and been the queue
                        join_table.t2_table = current_join_table

                    break  # jump up from the current loop since we encounter "Join"
                elif next_node.operator == Operator.HashAggregate:
                    print("hash_aggregate")
                    current_join_table = random_behaviors_util.handle_aggregate_operator(current_join_table)
                    next_node.join_table = current_join_table
                    # print(tmp_hash_list)
                elif next_node.operator == Operator.Project:
                    print("Project")
                    temp_len = len(current_join_table.t1_table.fields)
                    random_number = random.randint(2, temp_len)
                    tmp_pro_list = random.sample(current_join_table.t1_table.fields, random_number)

                    current_join_table.project_field = tmp_pro_list
                    next_node.join_table = current_join_table
                elif next_node.operator == Operator.Window:
                    print("Window")
                    pre_ancestor = next_node.parent_node[0].parent_node[0]
                    if pre_ancestor.operator == Operator.Exchange:

                        # if next_node.parent_node[0].parent_node[0]==Operator.Expand
                        window_fields = random_behaviors_util.handle_window_operator(current_join_table)
                        current_join_table.window_fields = window_fields
                    else:
                        last_field = current_join_table.window_fields[-1]
                        current_join_table.window_fields.append(last_field)
                elif next_node.operator == Operator.Union:
                    print("Union")
                    t1_table = current_join_table
                    # t_2_table = copy.deepcopy(t1_table)
                    next_node.join_table = JoinTable(t1_table=t1_table, t2_table=t1_table,
                                                     with_table_name=get_with_table_index())
                    next_node.join_table.is_union = True  # mark this is a union node
                    next_node.join_table.relationships = t1_table.relationships
                    next_node.join_table.fields = t1_table.fields
                    next_node.join_table.join_field_str = t1_table.join_field_str
                    # copy the t1_table to t2_table
                    # if next_node.join_table.t1_table and next_node.join_table.t2_table is None:
                    #     next_node.join_table.t2_table = next_node.join_table.t1_table
                    break
                next_node = next_node.child_node

    def print_final_sql(self, trace_name=""):
        self.bind_tables_to_top_nodes()
        last_join_node, generated_sql = self.generate_sql_from_join_nodes()
        # generated_sql = generate_sql(join_nodes)
        generated_sql = generated_sql[:-2]  # delete the last comma
        rest_sql = last_join_node.join_table.print_sql(use_as=False)
        generated_sql += rest_sql
        limit_sql = "\n LIMIT 100;"
        generated_sql += limit_sql
        print("--------------final generated sql ------------")

        print(generated_sql)

        sql_file_name = "trace_generated_sql-11-15.sql"
        result_dir = "./trace/generated_sql/"
        sql_file = os.path.join(result_dir, sql_file_name)
        with open(sql_file, 'a') as file:
            time_str = time.strftime('%Y-%m-%d %H:%M:%S', time.localtime())
            # file.write(time_str)
            file.write("\n---new sql--{}-----trace_name is {} \n".format(time_str, trace_name))
            file.write(generated_sql)
