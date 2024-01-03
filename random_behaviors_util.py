import sys, os

# from chain_pre_processing import generate_long_chain
from sythesize_nodes import SynthesizedNode
import random
from graphviz import Digraph

from database import Table, Field, Source
from tpc_ds_tables import store_returns, date_dim, store_sales, time_dim, item, customer, customer_demographics, \
    customer_address, household_demographics, store, promotion, reason, catalog_sales
from synthesize_tables import JoinTable
import copy
import time


def random_flag(flag=0.5):
    """
    whether a generated number is less the default threshold = 0.5
    :param flag:
    :return:
    """
    r = random.uniform(0, 1)
    if r < flag:
        return True
    return False


def random_chose_fields(tab: Table, related_tables: list[Table]) -> Table:
    """
    Saving the fields of the core table tab that relate to each table in the related_tables
    :param tab: the core table. e.g., store_returns
    :param related_tables: the list of tables that relate to the core tables, e.g. [date_dim, time_dim]
    :return: tab
    """
    resulting_fields = []
    for field in tab.fields:
        if "identifier" not in field.data_type:
            # print("r is {}".format(r))
            if random_flag():  # choose field by the default probability = 0.5
                resulting_fields.append(field)

    for i, temp_table in enumerate(related_tables):

        temp_tab_name = temp_table.name
        if temp_tab_name in tab.relationships:
            tuple_list = tab.relationships[temp_tab_name]
            for tuple in tuple_list:
                saved_key = tuple[0]
                for field in tab.fields:
                    if saved_key == field.name:  # only save the fields that relate the current select table
                        resulting_fields.append(field)
    tab.fields = resulting_fields  # change the fields of the tab
    return tab


def handle_sort_aggregate_operator_join_table(join_tab: JoinTable) -> JoinTable:
    """
         handling the sort aggregate operation of the join_tab
        :param join_tab:
        :return:
        """
    chosen_fields_len = random.randint(1, 3)
    chosen_fields = []
    join_tab.fields = []
    for i, temp_field in enumerate(join_tab.t1_table.fields):

        if "identifier" in temp_field.data_type:
            copy_temp_field = copy.deepcopy(temp_field)
            copy_temp_field.source_table = join_tab.t1_table.with_table_name  # give the field the new name
            join_tab.group_by_fields.append(copy_temp_field)  # group_by fields
            join_tab.fields.append(copy_temp_field)
        if chosen_fields_len > 0:
            if "integer" in temp_field.data_type or "decimal" in temp_field.data_type:
                chosen_fields.append(temp_field)
                chosen_fields_len -= 1
                join_tab.sort_agg.append(temp_field)
                agg_f = "MAX"
                join_tab.sort_agg_functions.append(agg_f)
                new_field = copy.deepcopy(temp_field)
                new_field.data_type = "varchar(100)"
                new_field.name = temp_field.name + "_" + agg_f
                join_tab.fields.append(new_field)

    return join_tab


def handle_sort_aggregate_operator_input_table(input_tab: Table) -> Table:
    """
         handling the sort aggregate operation of the join_tab
        :param join_tab:
        :return:
        """
    chosen_fields_len = random.randint(1, 3)
    chosen_fields = []
    copy_fields = copy.deepcopy(input_tab.fields)
    input_tab.fields = []
    for i, temp_field in enumerate(copy_fields):

        if "identifier" in temp_field.data_type:
            copy_temp_field = copy.deepcopy(temp_field)
            copy_temp_field.source_table = input_tab.with_table_name  # give the field the new name
            input_tab.group_by_fields.append(copy_temp_field)  # group_by fields
            input_tab.fields.append(copy_temp_field)
        if chosen_fields_len > 0:
            if "integer" in temp_field.data_type or "decimal" in temp_field.data_type:
                chosen_fields.append(temp_field)
                chosen_fields_len -= 1
                input_tab.sort_agg.append(temp_field)
                agg_f = "MAX"
                input_tab.sort_agg_functions.append(agg_f)
                new_field = copy.deepcopy(temp_field)
                new_field.data_type = "varchar(100)"
                new_field.name = temp_field.name + "_" + agg_f
                input_tab.fields.append(new_field)

    return input_tab


def handle_aggregate_operator(join_tab: JoinTable) -> JoinTable:
    """
     handling the aggregate operation of the join_tab
    :param join_tab:
    :return:
    """
    chosen_fields_len = random.randint(1, 3)
    chosen_fields = []
    join_tab.fields = []  # empty the original fields
    for i, temp_field in enumerate(join_tab.t1_table.fields):

        if "identifier" in temp_field.data_type:
            copy_temp_field = copy.deepcopy(temp_field)
            copy_temp_field.source_table = join_tab.t1_table.with_table_name  # give the field the new name
            join_tab.group_by_fields.append(copy_temp_field)  # group_by fields
            join_tab.fields.append(copy_temp_field)
        if chosen_fields_len > 0:
            if "integer" in temp_field.data_type or "decimal" in temp_field.data_type:
                chosen_fields.append(temp_field)
                chosen_fields_len -= 1
                join_tab.hash_agg.append(temp_field)
                r = random.uniform(0, 1)
                agg_f = "MAX"

                if r < 0.648:
                    agg_f = "SUM"
                elif r < 0.748:
                    agg_f = "AVG"
                elif r < 0.848:
                    agg_f = "COUNT"
                join_tab.hash_agg_functions.append(agg_f)
                new_field = copy.deepcopy(temp_field)
                new_field.name = temp_field.name + "_" + agg_f
                join_tab.fields.append(new_field)

    dimensional_field_num = random.randint(1, 3)
    temp_count = 0
    for i, dimensional_field in enumerate(join_tab.t2_table.fields):
        if temp_count >= dimensional_field_num:
            break
        if "identifier" not in dimensional_field.data_type:
            r = random.uniform(0, 1)
            if r < 0.5:
                copy_dimensional_field = copy.deepcopy(dimensional_field)
                copy_dimensional_field.source_table = join_tab.t2_table.with_table_name
                join_tab.group_by_fields.append(copy_dimensional_field)
                temp_count += 1

    return join_tab


def handle_aggregate_operator_input_table(input_tab: Table):
    """
     handling the aggregate operation of the join_tab
    :param join_tab:
    :return:
    """
    chosen_fields_len = random.randint(1, 3)
    chosen_fields = []
    copy_fields = copy.deepcopy(input_tab.fields)

    input_tab.fields = []

    for i, temp_field in enumerate(copy_fields):

        if "identifier" in temp_field.data_type:
            copy_temp_field = copy.deepcopy(temp_field)
            copy_temp_field.source_table = input_tab.with_table_name  # give the field the new name

            input_tab.group_by_fields.append(copy_temp_field)  # group_by fields
            input_tab.fields.append(copy_temp_field)
        if chosen_fields_len > 0:
            if "integer" in temp_field.data_type or "decimal" in temp_field.data_type:
                if "MAX" in temp_field.name or "SUM" in temp_field.name or "AVG" in temp_field.name or "COUNT" in temp_field.name:
                    continue
                chosen_fields.append(temp_field)
                chosen_fields_len -= 1
                input_tab.hash_agg.append(temp_field)
                r = random.uniform(0, 1)
                agg_f = "MAX"
                if r < 0.648:
                    agg_f = "SUM"
                elif r < 0.748:
                    agg_f = "AVG"
                elif r < 0.848:
                    agg_f = "COUNT"
                input_tab.hash_agg_functions.append(agg_f)
                new_field = copy.deepcopy(temp_field)
                new_field.name = temp_field.name + "_" + agg_f
                input_tab.fields.append(new_field)


def random_aggregate_fields(tab: JoinTable) -> JoinTable:
    resulting_fields = []
    for field in tab.fields:
        if "identifier" not in field.data_type:
            if random_flag():
                resulting_fields.append(field)


def handle_window_operator_input_table(input_tab: Table) -> list:
    window_fields = input_tab.window_fields
    # for field in input_tab.fields:

    if input_tab.hash_agg is None:
        for field in input_tab.fields:
            # if "MAX" in field.name or "SUM" in field.name or "AVG" in field.name or "COUNT" in field.name:
            # continue
            # if "identifier" not in field.data_type:
            if "identifier" not in field.data_type:
                if not window_fields:  # first field
                    window_fields.append(field)
                    return window_fields
                else:
                    last_field = window_fields[-1]
                    if field.name == last_field.name:
                        continue
                    else:
                        # we need a new field
                        window_fields.append(field)

    if not window_fields: # still empty , we could add one identifier fields
        for field in input_tab.fields:

            if "identifier" in field.data_type:
                window_fields.append(field)
                return window_fields
    else:
        return window_fields


def handle_window_operator(join_tab: JoinTable) -> list:
    window_fields = join_tab.window_fields
    for field in join_tab.fields:
        if "identifier" not in field.data_type:
            if not window_fields:  # first field
                window_fields.append(field)
                return window_fields
            else:
                last_field = window_fields[-1]
                if field.name == last_field.name:
                    continue
                else:
                    # we need a new field
                    window_fields.append(field)

    if not window_fields: # still empty , we could add one identifier fields
        for field in join_tab.fields:

            if "identifier" in field.data_type:
                window_fields.append(field)
                return window_fields
    else:
        return window_fields


# def handle_window_operator_input_table(input_tab: Table) -> list:
#     window_fields = input_tab.window_fields
#     for field in input_tab.fields:
#         if "identifier" not in field.data_type:
#             if not window_fields:  # first field
#                 window_fields.append(field)
#                 return window_fields
#             else:
#                 last_field = window_fields[-1]
#                 if field.name == last_field.name:
#                     continue
#                 else:
#                     # we need a new field
#                     window_fields.append(field)
#                     return window_fields


def handle_union_operator(t1_table: Table, t2_table: Table):
    t2_table.fields = t1_table.fields
    t2_table.group_by_fields = t1_table.group_by_fields
    t2_table.hash_agg = t1_table.hash_agg
    t2_table.hash_agg_functions = t1_table.hash_agg_functions
    t2_table.name = t1_table.name
    # t2_table.where_fields = t1_table.where_fields # exclude the where_fields


def draw_sythesized_physical_plan(top_level_nodes: list[SynthesizedNode], filename="trace_sythesized_physical_plan"):
    """
    Drawing the synthesized physical plan and output it to a file named with the filename
    :param top_level_nodes:
    :param filename:
    """
    dot = Digraph(comment='The Round Table')
    # for i, name in enumerate(nodeLine):
    #     # if "ReusedExchange" not in name:
    #     dot.node(str(i), "nodeID(" + str(i) + ") " + name)
    # for i, nexti in enumerate(adj_next):
    #     for j in nexti:
    #         dot.edge(str(i), str(j))

    for top_node in top_level_nodes:
        current_node = top_node
        previous_node = None
        while current_node:
            dot.node(str(current_node.id), str(current_node.id) + "_" + current_node.name)
            if previous_node:
                dot.edge(str(previous_node.id), str(current_node.id))
            previous_node = current_node
            current_node = current_node.child_node

    # dot.save(save_path)
    dot.render(filename)
