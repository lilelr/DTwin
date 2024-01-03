import sys, os
import getopt
import argparse
from chain_pre_processing import  generate_long_chain_probability
from sythesize_nodes import SynthesizedNode
from enum_operator import Operator
import random
from database import Table, Field, Source
from tpc_ds_tables import store_returns, date_dim, store_sales, time_dim, item, customer, customer_demographics, \
    customer_address, household_demographics, store, promotion, reason, catalog_sales
from synthesize_tables import JoinTable
import copy
import time

import logging
import random_behaviors_util
import datetime
# from lstm_sequences_generating import LSTMWrapper, LSTMTagger

table_index = 1


def get_with_table_index():
    global table_index
    index = table_index
    table_index = table_index + 1
    return str(index)


def length_one_branch(id_index, table: Table):
    table = copy.deepcopy(table)
    start_node = SynthesizedNode(id_index, "FileScan parquetd")
    start_node.input_table = table  # fix the input table
    start_node.input_table.with_table_name = "tb" + get_with_table_index()
    id_index += 1

    second_node = SynthesizedNode(id_index, "Filter")
    id_index += 1
    second_node.parent_node.append(start_node)
    second_node.input_table = start_node.input_table
    start_node.child_node = second_node
    previous_node = second_node

    r = random.uniform(0, 1)
    if r < 0.5:
        third_node = SynthesizedNode(id_index, "Project")
        id_index += 1
        third_node.parent_node.append(second_node)
        third_node.input_table = second_node.input_table
        second_node.child_node = third_node
        third_node.input_table.random_project_fields()
        previous_node = third_node
    else:
        previous_node.input_table.where_fields = []

    return [start_node], [], previous_node, id_index


def length_two_branch(id_index, core_table, table_pop):
    core_table_copy = copy.deepcopy(core_table)
    table_pop_copy = copy.deepcopy(table_pop)
    r = random.uniform(0, 1)
    # r=0.2
    if r < 0.5:
        # start_node = SynthesizedNode(id_index, "FileScan parquetd")
        # # start_node.input_table = customer_copy  # fix the input table
        # start_node.input_table = core_table_copy  # fix the input table
        # start_node.input_table.with_table_name = "tb" + get_with_table_index()
        # id_index += 1
        #
        # related_table = copy.deepcopy(core_table_copy)
        # second_node = SynthesizedNode(id_index, "FileScan parquetd")
        # second_node.input_table = related_table  # choose a random table
        # second_node.input_table.with_table_name = "tb" + get_with_table_index()
        # id_index += 1
        #

        start_node = SynthesizedNode(id_index, "FileScan parquetf")
        # start_node.input_table = customer_copy  # fix the input table
        start_node.input_table = core_table_copy  # fix the input table
        start_node.input_table.with_table_name = "tb" + get_with_table_index()
        id_index += 1

        related_table = table_pop_copy
        second_node = SynthesizedNode(id_index, "FileScan parquetd")
        second_node.input_table = related_table  # choose a random table
        second_node.input_table.with_table_name = "tb" + get_with_table_index()
        id_index += 1

    else:
        customer_copy = copy.deepcopy(customer)
        customer_address_copy = copy.deepcopy(customer_address)
        customer_demographics_copy = copy.deepcopy(customer_demographics)
        household_demographics_copy = copy.deepcopy(household_demographics)
        date_dim_copy = copy.deepcopy(date_dim)
        related_tables = [customer_address_copy, customer_demographics_copy, household_demographics_copy, date_dim_copy]

        start_node = SynthesizedNode(id_index, "FileScan parquetd")
        # start_node.input_table = customer_copy  # fix the input table
        start_node.input_table = customer_copy  # fix the input table
        start_node.input_table.with_table_name = "tb" + get_with_table_index()
        id_index += 1

        random_index = random.randint(0, 3)
        related_table = related_tables[random_index]
        second_node = SynthesizedNode(id_index, "FileScan parquetd")
        second_node.input_table = related_table  # choose a random table
        second_node.input_table.with_table_name = "tb" + get_with_table_index()
        id_index += 1

    end_node = SynthesizedNode(id_index, "BroadcastHashJoin")
    # date_dim_copy = copy.deepcopy(date_dim)
    # table_copy = copy.deepcopy(store_returns)

    end_node.join_table = JoinTable(t1_table=start_node.input_table,
                                    t2_table=second_node.input_table,
                                    with_table_name=get_with_table_index())

    end_node.join_table.relationships = end_node.join_table.t1_table.relationships
    end_node.join_table.fields = end_node.join_table.t1_table.fields
    end_node.join_table.join_field_str = end_node.join_table.t1_table.join_field_str

    end_node.parent_node.append(start_node)
    end_node.parent_node.append(second_node)
    start_node.child_node = end_node
    second_node.child_node = end_node
    return [start_node, second_node], end_node


def length_n_branch(id_index, target_len, main_table):
    """
    :param id_index:
    :param target_len: 2<= target_len <= 4
    :param main_table: the main table of the main chain
    :return: start_node_list,join_node_list, previous_node
    """
    main_table = copy.deepcopy(main_table)
    store_sale_copy = copy.deepcopy(store_sales)
    store_returns_copy = copy.deepcopy(store_returns)
    catalog_sales_copy = copy.deepcopy(catalog_sales)
    store_sales_related = [copy.deepcopy(table) for table in [date_dim, time_dim, item, customer, customer_demographics,
                                                              household_demographics, customer_address, store,
                                                              promotion]]
    store_returns_related = [copy.deepcopy(table) for table in
                             [date_dim, time_dim, item, customer, customer_demographics]]

    # catalog_sales_related = [copy.deepcopy(table) for table in [store_returns, store_sales,  customer, customer_demographics]]
    catalog_sales_related = [copy.deepcopy(table) for table in
                             [customer, customer_demographics, date_dim, time_dim, item]]
    customer_copy = copy.deepcopy(customer)
    customer_address_copy = copy.deepcopy(customer_address)
    customer_demographics_copy = copy.deepcopy(customer_demographics)
    household_demographics_copy = copy.deepcopy(household_demographics)
    date_dim_copy = copy.deepcopy(date_dim)
    customer_related_tables = [customer_address_copy, customer_demographics_copy, household_demographics_copy,
                               date_dim_copy]

    r = random.uniform(0, 4)

    related_tables = store_sales_related
    core_table = store_sale_copy
    if r <= 1:
        related_tables = store_returns_related
        core_table = store_returns_copy
    elif r <= 2:
        related_tables = customer_related_tables
        core_table = customer_copy
    elif r <= 3:
        related_tables = catalog_sales_related
        core_table = catalog_sales_copy

    random.shuffle(related_tables)
    true_connected_tables = related_tables[:target_len]
    true_connected_tables.append(main_table)
    core_table = random_behaviors_util.random_chose_fields(core_table, true_connected_tables)

    first_node = SynthesizedNode(id_index, "FileScan parquetf")
    first_node.input_table = core_table  # fix the input table
    first_node.input_table.with_table_name = "tb" + get_with_table_index()
    id_index += 1
    previous_node = first_node

    # result
    start_node_list = []
    join_node_list = []
    start_node_list.append(first_node)

    target_len -= 1
    while target_len > 0:
        table_pop = related_tables.pop(0)
        new_start_node = SynthesizedNode(id_index, "FileScan parquetd")

        new_start_node.input_table = table_pop
        new_start_node.input_table.with_table_name = "tb" + get_with_table_index()
        id_index += 1
        start_node_list.append(new_start_node)

        join_node = SynthesizedNode(id_index, "BroadcastHashJoin")
        id_index += 1
        # date_dim_copy = copy.deepcopy(date_dim)
        # table_copy = copy.deepcopy(store_returns)

        t1_table_temp = previous_node.join_table if previous_node.join_table.t1_table else previous_node.input_table

        join_node.join_table = JoinTable(t1_table=t1_table_temp,
                                         t2_table=new_start_node.input_table,
                                         with_table_name=get_with_table_index())

        join_tab = join_node.join_table
        join_tab.relationships = join_tab.t1_table.relationships
        join_tab.fields = join_tab.t1_table.fields
        join_tab.join_field_str = join_tab.t1_table.join_field_str
        join_tab.name = core_table.name  # the name of all join_tables is fixed at "Customer"

        # handle the hash_aggregation
        join_node.join_table = random_behaviors_util.handle_aggregate_operator(join_tab)

        join_node.parent_node.append(previous_node)
        join_node.parent_node.append(new_start_node)
        previous_node.child_node = join_node
        new_start_node.child_node = join_node

        join_node_list.append(join_node)
        previous_node = join_node
        target_len -= 1
    return start_node_list, join_node_list, previous_node, id_index


def build_tree(long_chain, short_branch_pro=0.7):
    chain_len = len(long_chain)
    top_level_nodes = []
    id_index = 1
    previous_node = None
    start_node = None
    join_index = 0
    join_node_list = []
    actuaL_tables = None
    # 要在list中存很多表 第一个是根表 别的表都要跟这个表有连接。现在要存是那个field跟什么表连接(field1, field2, table2)
    # len = 9
    connected_tables_store_sales = [store_sales, date_dim, time_dim, item, customer, customer_demographics,
                                    household_demographics, customer_address, store, promotion]
    connected_tables_store_returns = [store_returns, date_dim, time_dim, item, customer, customer_demographics,
                                      household_demographics, customer_address, reason, store_sales]

    # length = 8

    actual_tables = []
    r = random.uniform(0, 1)
    main_table = store_sales
    if r < 0.5:
        actual_tables = connected_tables_store_sales[:chain_len]
    else:
        main_table = store_returns
        actual_tables = connected_tables_store_returns[:chain_len]

    for index, op in enumerate(long_chain):
        if op == 'FileScan FactTable':
            node = SynthesizedNode(id_index, "FileScan FactTable")
            table_new = actual_tables.pop(0)
            node.input_table = table_new  # fix the input table

            node.input_table.with_table_name = "tb" + get_with_table_index()
            id_index += 1
            previous_node = node
            start_node = node
            top_level_nodes.append(start_node)  # the node indexed at 0 is equivalent to the start_node

        elif op == 'FileScan DimTable':
            node = SynthesizedNode(id_index, "FileScan DimTable")
            node.input_table = date_dim  # fix the input table
            node.input_table.with_table_name = "tb" + get_with_table_index()
            id_index += 1
            previous_node = node
            start_node = node
            top_level_nodes.append(start_node)  # the node indexed at 0 is equivalent to the start_node

        elif op == 'Filter':
            node = SynthesizedNode(id_index, "Filter")
            node.input_table = previous_node.input_table
            node.join_table = previous_node.join_table
            previous_node.child_node = node
            id_index += 1
            node.parent_node.append(previous_node)
            previous_node = node

        elif op == 'Project':
            node = SynthesizedNode(id_index, "Project")
            node.input_table = previous_node.input_table
            node.join_table = previous_node.join_table
            previous_node.child_node = node
            node.parent_node.append(previous_node)
            id_index += 1
            previous_node = node

        elif op == 'HashAggregate':
            node = SynthesizedNode(id_index, "HashAggregate")
            previous_node.child_node = node
            node.input_table = previous_node.input_table
            node.join_table = previous_node.join_table
            previous_node = node
            id_index += 1

        elif op == 'Window':
            print("Window")
            node = SynthesizedNode(id_index, "Window")
            node.input_table = previous_node.input_table
            node.join_table = previous_node.join_table
            previous_node.child_node = node
            node.parent_node.append(previous_node)

            # previous_node
            if len(previous_node.parent_node) > 0:
                if len(previous_node.parent_node[0].parent_node) > 0:
                    pre_ancestor = previous_node.parent_node[0].parent_node[0]
                    if pre_ancestor.operator == Operator.Exchange or (
                            node.input_table.window_fields is None or len(node.input_table.window_fields) == 0):
                        # if next_node.parent_node[0].parent_node[0]==Operator.Expand
                        window_fields = random_behaviors_util.handle_window_operator_input_table(node.input_table)
                        node.input_table.window_fields = window_fields
                    else:
                        last_field = node.input_table.window_fields[-1]
                        node.input_table.window_fields.append(last_field)
                else:
                    window_fields = random_behaviors_util.handle_window_operator_input_table(node.input_table)
                    node.input_table.window_fields = window_fields
            else:
                window_fields = random_behaviors_util.handle_window_operator_input_table(node.input_table)
                node.input_table.window_fields = window_fields

            id_index += 1
            previous_node = node

        elif op == 'Union':
            # if node.join_table.t1_table is None:
            node = SynthesizedNode(id_index, "Union")
            previous_node.child_node = node
            node.input_table = previous_node.input_table
            node.parent_node.append(previous_node)
            # if it is the long chain,we bind the table. Otherwise, we do not bind any table
            t1_table = previous_node.input_table
            # t_2_table = copy.deepcopy(t1_table)
            node.join_table = JoinTable(t1_table=t1_table, t2_table=t1_table,
                                        with_table_name=get_with_table_index())
            node.join_table.is_union = True  # mark this is a union node
            node.join_table.relationships = t1_table.relationships
            node.join_table.fields = t1_table.fields
            node.join_table.join_field_str = t1_table.join_field_str
            # self.join_nodes.append(node)  # append the node to the join nodes list
            node.join_table.union_count_tables += 1
            join_node_list.append(node)
            previous_node = node
            id_index += 1

        elif op == 'Sort':
            node = SynthesizedNode(id_index, "Sort")
            previous_node.child_node = node
            node.input_table = previous_node.input_table
            node.join_table = previous_node.join_table
            previous_node = node
            id_index += 1

        elif op == "TakeOrderedAndProject":
            node = SynthesizedNode(id_index, "TakeOrderedAndProject")
            previous_node.child_node = node
            node.input_table = previous_node.input_table
            node.join_table = previous_node.join_table
            previous_node = node
            id_index += 1

        elif op == 'BroadcastHashJoin' or op == 'SortMergeJoin':
            name_str = "BroadcastHashJoin" if op == "BroadcastHashJoin" else "SortMergeJoin"
            node = SynthesizedNode(id_index, name_str)
            id_index += 1
            # date_dim_copy = copy.deepcopy(date_dim)
            # table_copy = copy.deepcopy(store_returns)
            # start_nodes_branch, end_node_one_branch = length_one_branch(id_index, table_copy)
            # start_nodes_branch, end_node_one_branch = length_two_branch(id_index,core_table, table_pop)

            if random_behaviors_util.random_flag(short_branch_pro):
                table_pop = actual_tables.pop(0)
                br_start_nodes_list, br_join_node_list, end_node_one_branch, id_index = length_one_branch(id_index,
                                                                                                          table_pop)
            else:
                branch_length = random.randint(2, 3)
                br_start_nodes_list, br_join_node_list, end_node_one_branch, id_index = length_n_branch(id_index,
                                                                                                        branch_length,
                                                                                                        main_table)
            # top_level_nodes.append(start_node_branch)
            if br_start_nodes_list:
                top_level_nodes.extend(br_start_nodes_list)
            if br_join_node_list:
                join_node_list.extend(br_join_node_list)

            temp_t2_table = end_node_one_branch.join_table if end_node_one_branch.join_table.t1_table else end_node_one_branch.input_table
            if join_index >= 1:

                node.join_table = JoinTable(t1_table=previous_node.join_table, t2_table=temp_t2_table,
                                            with_table_name=get_with_table_index())

            else:
                node.join_table = JoinTable(t1_table=previous_node.input_table,
                                            t2_table=temp_t2_table,
                                            with_table_name=get_with_table_index())

            node.join_table.relationships = node.join_table.t1_table.relationships
            node.join_table.fields = node.join_table.t1_table.fields
            # assign the fields of t1_tables to the
            # join_table.fields
            node.join_table.join_field_str = node.join_table.t1_table.join_field_str

            node.parent_node.append(previous_node)
            node.parent_node.append(end_node_one_branch)
            previous_node.child_node = node
            end_node_one_branch.child_node = node
            previous_node = node
            # join_node = node
            join_node_list.append(node)
            join_index += 1

    return start_node, id_index, join_node_list, top_level_nodes


def generate_sql_from_tree(start_node, join_nodes, top_level_nodes):
    final_sql = "WITH "
    while join_nodes:
        current_node = join_nodes.pop(0)
        current_join_table = current_node.join_table

        if current_join_table.t1_table is None or current_join_table.t2_table is None:
            join_nodes.append(current_node)
            continue
        else:  # we can print the t1_sql, and t_2_sql if both t1_table and t2_table are not none
            current_join_table.join_fields.append(current_join_table.t1_table.join_field_str)
            current_join_table.join_fields.append(current_join_table.t2_table.join_field_str)
            t1_sql = current_join_table.t1_table.print_sql(use_as=True)
            t2_sql = current_join_table.t2_table.print_sql(use_as=True)
            final_sql += "\n" + t1_sql + " , \n " + t2_sql + " , "

        next_node = current_node.child_node

        while True:
            if next_node is None or next_node.operator == Operator.TakeOrderedAndProject:
                return current_node, final_sql
            if next_node.operator == Operator.BroadcastHashJoin or next_node.operator == Operator.SortMergeJoin:
                join_table = next_node.join_table
                if join_table.t1_table is None:  # pretty new node
                    join_table.t1_table = current_join_table
                    join_table.t2_table = None
                    join_nodes.append(next_node)  # this next_node has not been visited, add it to the join_ndoes
                elif join_table.t2_table is None:  # has a table already and been the queue
                    join_table.t2_table = current_join_table

                break  # jump up from the current loop since we encounter "HashJoin"
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
                if pre_ancestor.operator == Operator.Exchange or len(current_join_table.window_fields) == 0:

                    # if next_node.parent_node[0].parent_node[0]==Operator.Expand
                    window_fields = random_behaviors_util.handle_window_operator(current_join_table)
                    current_join_table.window_fields = window_fields
                else:
                    last_field = current_join_table.window_fields[-1]
                    current_join_table.window_fields.append(last_field)
                next_node.join_table = current_join_table
            next_node = next_node.child_node


if __name__ == "__main__":
    # ---------swx------------

    parser = argparse.ArgumentParser(description='Usage of DTwin')

    parser.add_argument("--num", help="number of generated queries", type=int)
    parser.add_argument("--length", help="the length of critical path", type=int)
    parser.add_argument("--ed", help="the edge density between [0,1]", type=float)
    args = parser.parse_args()

    number_sql = 2
    critical_path_len = 3

    short_branch_pro = 0.4

    if args.num:
        number_sql = args.num

    if args.length:
        critical_path_len = args.length

    if args.ed:
        short_branch_pro = args.ed

    # 获取当前日期
    now = datetime.datetime.now()

    year = now.year  # 年份
    month = now.month  # 月份
    day = now.day  # 日
    sql_file_name = "tpcds-synthesized-sql-date-{}-{}-{}.sql".format(year, month, day)
    result_dir = "./"
    sql_file = os.path.join(result_dir, sql_file_name)
    target_num = number_sql
    while number_sql > 0:
        try:

            # long_chain = generate_long_chain(a)
            long_chain = generate_long_chain_probability(critical_path_len)
            # long_chain = ['FileScan parquetf', 'Filter', 'BroadcastHashJoin', 'Project', 'HashAggregate', 'Sort', 'Window', 'Project',
            #  'sort', 'SortMergeJoin', 'Filter', 'Project', 'TakeOrderedAndProject', '']
            start_node, id_final, join_node_list, top_level_nodes = build_tree(long_chain, short_branch_pro)
            random_behaviors_util.draw_sythesized_physical_plan(top_level_nodes)
            # current_node = start_node
            # while current_node:
            #     print(current_node.name)
            #     current_node = current_node.child_node

            print("------------top-level nodes---------------")
            # for index, node in enumerate(top_level_nodes):
            #     print("index is {}, node.name is {}".format(index, node.name))
            #     current_node = node
            #     while current_node:
            #         print(current_node.name)
            #         current_node = current_node.child_node

            last_join_node, generated_sql = generate_sql_from_tree(start_node, join_node_list, top_level_nodes)
            # generated_sql = generate_sql(join_nodes)
            generated_sql = generated_sql[:-2]  # delete the last comma
            rest_sql = last_join_node.join_table.print_sql(use_as=False)
            generated_sql += rest_sql
            limit_sql = "\n LIMIT 100;"
            generated_sql += limit_sql
            print("--------------final generated sql ------------")

            print(generated_sql)

            # # sql_file_name = "branch_length2_synthesized_sql.sql"
            # # sql_file_name = "branch_length2_synthesized_sql_10_23.sql"
            # # sql_file_name = "tpc-ds-synthesized-sql-12-7.sql"
            # sql_file_name = "tpc-ds-synthesized-sql-12-18.sql"
            # result_dir = "../../results/generated_sqls/"
            # sql_file = os.path.join(result_dir, sql_file_name)


            with open(sql_file, 'a') as file:
                time_str = time.strftime('%Y-%m-%d %H:%M:%S', time.localtime())
                # file.write(time_str)
                file.write("\n---new sql--{}-----\n".format(time_str))
                file.write("long_chain is {}\n".format(long_chain))
                file.write(generated_sql)

            # planList = parsePhysicalPlan(fileName)
            # # print(planList)
            # # top_level_nodes, nodes_list, adj_next = generate_adj(planList, qname=fileName + "_operators")
            # top_level_nodes=None
            # join_nodes = handle_file_scan_nodes(top_level_nodes, None, None)
            # print(len(join_nodes))
            # print(join_nodes)
            # final_node, final_sql = handle_join_nodes(join_nodes, None, None)
            # print("final_sql is -----------------------------")
            #
            # if not final_node:
            #     print("error")
            # else:
            #     final_sql = final_sql[:-2]  # delete the comma
            #     final_sql += final_node.join_table.printSQL(use_as=False)
            #     final_sql += "LIMIT 100\n"
            # print(final_sql)

            # sql_file_name = 'generated_sql_{}.sql'.format(queryName)
            # result_dir = "../results/generated_sqls/"
            # sql_file = os.path.join(result_dir, sql_file_name)
            # with open(sql_file, 'w') as file:
            #     file.write(final_sql)

            number_sql = number_sql - 1
        except Exception as e:
            logging.exception("An error occurred:")
            print(e)

        print("Result: number of generated queries is {}, the length of critical path is {}, edge density is {}".format(
            target_num, critical_path_len, short_branch_pro))
        print("output path is {}".format(sql_file))
