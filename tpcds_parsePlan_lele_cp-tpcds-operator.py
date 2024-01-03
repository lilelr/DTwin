import sys, os
import re
from queue import Queue
from operator import attrgetter

from graphviz import Digraph
from node_lele import Node
from stage import FirstStage, BroadcastStage, NormalStage, Stage
from enum_operator import Operator
from sythesize_nodes import SynthesizedNode
from trace_generate_sql import TraceSQLGenerator
from synthesize_tables import JoinTable

from first_stage_performance_model import FirstStagePrediction

import numpy as np

import os

operatorList = [
    "ReusedExchange",
    "Scan parquet",
    "Scan",
    "Window",
    "ColumnarToRow",
    "Project",
    # SMJ
    "SortAggregate",
    "SortMergeJoin",
    "Sort",
    # BHJ
    "BroadcastHashJoin",
    "BroadcastExchange",
    "Exchange hashpartitioning",
    "Exchange SinglePartition",
    "HashAggregate",
    "Filter",
    "Subquery",
    "Expand",
    "Union",
    "Execute",
    "Exchange",  # for formatted / final plan
    "Generate"

]

resource_config = {"executor_cores": 2, "executor_mem": 8, "executor_instances": 4, "number_machines": 4}
executor_cores = 2
executor_mem = 8  # 8 GB
executor_instances = 6  # number of instances
number_machines = 6  # number of machines


def parsePhysicalPlan(fileName):
    fileHandler = open(fileName, "r", encoding='UTF-8')
    lineList = []
    while True:
        # Get next line from file
        line = fileHandler.readline()
        # If line is empty then end of file reached
        if not line:
            break
        # 去掉回车"\n"
        lineList.append(line.strip("\n"))
    fileHandler.close()
    return lineList


# 从计划中提取出物理操作符
def extractOperatorFromPlan(plan):
    # print(plan) # plan 表示当前行的内容 如:
    # +- FileScan parquet tpcds_100_parquet.date_dim[d_date_sk#24,d_year#30] Batched: true, DataFilter
    for op in operatorList:
        if op in plan:
            if op == "Scan":
                # 对FileScan类型特殊处理，找到数据源的表
                leftIndex = plan.find("Scan")
                rightIndex = plan.find("(")
                newOp = plan[leftIndex:rightIndex]

                fields_count = newOp.count("#")
                # print(fields_count)
                # return newOp, fields_count
                return "Scan", fields_count
            elif op == "Filter":
                fields_count = plan.count("#")
                return op, fields_count
            else:
                return op, 0

    print("Unsupported Operator found in line: {}".format(plan))
    return "error", 0


# 从计划中提取出根节点的名字
def extractRootOpFromPlan(plan):
    for op in operatorList:
        if op in plan:
            return op
    return "Error"


def predict_runtime(planList, qname="q", type="spark3"):
    n = len(planList) - 1
    # 名称; 所在图的层次（最后一个结点是0 或1;    stage号（op前的数字，无就-1）;  nextnode编号
    nodename, layer, stage, adj_next = [""], [0], [-1], [[] for i in range(n)]
    nodeLine = [""]  # 记录该node 对应的physical plan 原信息
    nodes_fileds_count = [0]
    planList = planList[1:] if type == "spark2" else (planList[2:] if type == "spark3" else planList[3:])
    # 1.1. Deal Root name  
    line = planList[0]
    # nodename[0] = extractOperatorFromPlan(line)
    nodename[0] = extractRootOpFromPlan(line)
    i0 = line.find("*(")
    stage[0] = int(line[i0 + 2: line.find(")")]) if i0 > 0 else -1
    # 1.2. Deal nonRoot node
    for id, line in enumerate(planList[1:], start=1):
        if line.strip() == "":
            break
        reuse_id = -1  # 重用哪个node
        # todo 注意q14b [i_item_sk#175, i_brand_id#182, i_class_id#184, i_category_id#186]
        # 注意 q23a 倒数第二个reuse   到底是用哪一个？
        # 注意data_id 有sum的情况
        if "ReusedExchange" in line:
            # op = line[line.find("],") + 3:].strip()
            # data_id = line[line.find("[") + 1: line.find("]")].strip()  # [d_date_sk#71] 这些,叫啥 啥意思？ todo
            # # 不能有[]符号，因为q14a 有Project [i_item_sk#26 AS ss_item_sk#18]
            # for ti, tline in enumerate(planList):
            #     if op in tline and data_id in planList[ti + 1]:  # 一定会找到吧?
            #         reuse_id = ti
            #         break
            # tmpname = "ReusedExchange_%s" % reuse_id
            tmpname = "FileScan"  # change the ReusedExchange to FileScan
            fields_count = 0
        else:
            tmpname, fields_count = extractOperatorFromPlan(line)
        i0 = line.find("-")
        if i0 < 0:  # 应该不会有这种情况吧
            # print(line)
            continue
        tmplayer = int(i0 / 3) + 1  # + 1, 因为根结点才是layer0
        i0 = line.find("*(")
        tmpstage = int(line[i0 + 2: line.find(")")]) if i0 >= 0 else -1
        # 2. Record

        nodename.append(tmpname)

        nodeLine.append(line)
        nodes_fileds_count.append(fields_count)
        layer.append(tmplayer)  # templayer=2
        stage.append(tmpstage)
        # 3. Build adj
        for i in range(id - 1, -1, -1):  # id =2
            if layer[i] < tmplayer:  # layer = [0,1,2]
                if reuse_id < 0:  # reuse_id = -1
                    adj_next[id].append(i)
                else:
                    adj_next[reuse_id].append(i)
                break
    # print(nodename)
    adj_next = adj_next[:len(nodename)]

    nodes_list = []
    top_level_nodes = []

    operators_dict = {op.name: 0 for op in Operator}  # key: Operator.type value: the numer of operators

    for i, nexti in enumerate(adj_next):
        current_depth, current_exchanges = dfs_lele_cp(i, 0, [], nodename, adj_next, False)
        if i > 0:
            next_node_id = adj_next[i][0]
            if "SortAggregate" in nodename[i]:
                print("SortAggregate")
            node = Node(i, nodename[i], current_depth, current_exchanges, next_node_id, nodes_fileds_count[i],
                        nodeLine[i])
            nodes_list.append(node)
            if node.name.find("Scan") >= 0:
                top_level_nodes.append(node)
        else:
            node = Node(i, nodename[i], 0, 0, -1, 0, nodeLine[i])
            nodes_list.append(node)
        # print("{} ".format(node))
        if node.operator != Operator.ColumnarToRow:
            operators_dict[node.operator.name] += 1
        # print("current_depth for {},{} is {} ".format(i,nodename[i],current_depth))
    # draw_adj(nodename, adj_next, filename=qname + ".adj")
    # sort the current top_level_nodes based on the number of exchanges first and depth second
    sorted_nodes = sorted(top_level_nodes, key=attrgetter('exchanges', 'depth'), reverse=True)
    # sorted_nodes = top_level_nodes

    temp_count = 0
    for key, value in operators_dict.items():
        print("Operator-{}, its count is {}".format(key, value))
        temp_count += value
    # print(temp_count)
    return 0, 0, 0, operators_dict, temp_count, sorted_nodes, nodes_list


def dfs_lele_cp(node_id, depth, exchanges, nodename, adj_next, flag_hash_aggregate):
    """
    node_ind: int
    depth:int
    exchange:[]
    nodename:string
    adjnext: []
    flag_hash_aggregate: bool
    """
    # if node_id==16:
    #     print("debug")
    if node_id == 0:
        return depth, exchanges
    else:
        depth = depth + 1
        # if str(nodename[node_id]).strip() == "Exchange":
        temp_line = str(nodename[node_id]).strip()
        if temp_line == "Exchange hashpartitioning" or temp_line == "Exchange SinglePartition":
            if flag_hash_aggregate:  # A previous Hash_aggregate operation has shown
                exchanges.append(1)  # since we have seen the Hash_aggregate before, we append 1 here
                flag_hash_aggregate = False
            else:
                exchanges.append(0)
        elif temp_line == "HashAggregate":
            flag_hash_aggregate = True

        next_node_id = adj_next[node_id][0]
        return dfs_lele_cp(next_node_id, depth, exchanges, nodename, adj_next, flag_hash_aggregate)


def draw_adj(nodeLine, adj_next, filename="onegraph"):
    dot = Digraph(comment='The Round Table')
    for i, name in enumerate(nodeLine):
        # if "ReusedExchange" not in name:
        dot.node(str(i), "nodeID(" + str(i) + ") " + name)
    for i, nexti in enumerate(adj_next):
        for j in nexti:
            dot.edge(str(i), str(j))
    # dot.save(save_path)
    dot.render(filename)


def plot_dag_sythesized_nodes(top_level_nodes: list[Node], nodes_list: list[Node]):
    dict_synthesized_nodes = {}
    top_syn_nodes = []
    for begin_node in top_level_nodes:
        current_node = begin_node
        previous_node = None
        flag = True

        while current_node:
            if current_node.id not in dict_synthesized_nodes:
                cur_syn_node = SynthesizedNode(current_node.id, current_node.name)
                # cur_syn_node.operator = current_node.operator

                dict_synthesized_nodes[current_node.id] = cur_syn_node
                if flag:
                    top_syn_nodes.append(cur_syn_node)
                    flag = False

            else:
                cur_syn_node = dict_synthesized_nodes[current_node.id]

            if previous_node:
                cur_syn_node.parent_node.append(previous_node)
                previous_node.child_node = cur_syn_node

            previous_node = cur_syn_node

            if current_node.nextnode_id == -1:
                current_node = None
            else:
                current_node = nodes_list[current_node.nextnode_id]
    return top_syn_nodes


def dfs_top_syn_nodes(top_syn_nodes: list[SynthesizedNode]):
    print("dfs_top_syn_nodes ------- ---------------")
    for top_node in top_syn_nodes:
        print("------------------------")
        current_node = top_node
        while current_node:
            print(repr(current_node))
            current_node = current_node.child_node


if __name__ == "__main__":
    # fileName = sys.argv[1]
    queryName = "tpcds-generated-sql1"
    prefix = "../../results/tpcds_generated_sql_plans/"
    file_names = [
        "tpcds_generated_sq1-12-21-plan.txt",
        "tpcds_generated_sql2-12-21-plan.txt",
        "tpcds_generated_sql3-12-21-plan.txt",
        "tpcds_generated_sql4-12-21-plan.txt",
        "tpcds_generated_sql5-12-21-plan.txt",
        "tpcds_generated_sql6-12-21-plan.txt",
        "tpcds_generated_sql7-12-21-plan.txt",
        "tpcds_generated_sql8-12-21-plan.txt"
    ]

    i = 1
    # particular_cases = [17,25,29,64,72]
    # particular_cases = [72]
    operators_head = ",".join([op.name for op in Operator])
    content = "file_name, dag_depth,dag_width,stage_count,operator_count,"
    content += operators_head
    content += "\n"

    # while i <= 99:
    #     # for i in particular_cases:
    #     print(i)
    #     # if i == 44 or i == 46:
    #     #     i += 1
    #     #     continue
    #     # fileName = "../results/q21-30/q"+str(i)+"_100.plan"
    #     file_name = ""
    #     if 1 <= i <= 10:
    #         file_name = "../../results/q1-10/q" + str(i) + "_500.plan"
    #     elif 11 <= i <= 20:
    #         if i == 14:
    #             file_name = "../../results/q11-20/q" + str(i) + "a_500.plan"
    #         else:
    #             file_name = "../../results/q11-20/q" + str(i) + "_500.plan"
    #     elif 21 <= i <= 30:
    #         if i == 23 or i==24:
    #             file_name = "../../results/q21-30/q" + str(i) + "a_500.plan"
    #         else:
    #             file_name = "../../results/q21-30/q" + str(i) + "_500.plan"
    #     elif 31 <= i <= 40:
    #         file_name = "../../results/q31-40/q" + str(i) + "_500.plan"
    #         if i == 39:
    #             file_name = "../../results/q31-40/q" + str(i) + "a_500.plan"
    #     elif 41 <= i <= 50:
    #         file_name = "../../results/q40-49/q" + str(i) + "_500.plan"
    #     elif 51 <= i <= 60:
    #         file_name = "../../results/q50-60/q" + str(i) + "_500.plan"
    #     elif 61 <= i <= 70:
    #         file_name = "../../results/q61-70/q" + str(i) + "_500.plan"
    #     elif 71 <= i <= 99:
    #         file_name = "../../results/q71-99/q" + str(i) + "_500.plan"
    #
    #     path = file_name
    #
    #     # try:
    #     planList = parsePhysicalPlan(path)
    #     depth, dag_width, stage_count, operators_dict, operator_count, top_level_nodes, nodes_list = predict_runtime(
    #         planList,
    #         qname=path + "_operators")  # 检查有无
    #     top_syn_nodes = plot_dag_sythesized_nodes(top_level_nodes, nodes_list)
    #     # traceSQLgenerator = TraceSQLGenerator(top_syn_nodes)
    #     # traceSQLgenerator.print_final_sql(file_name)
    #     # dfs_top_syn_nodes(top_syn_nodes)
    #     operators_str = ",".join([str(operators_dict[op.name]) for op in Operator])
    #     print(operator_count)
    #     new_line = '{},{},{},{},{},'.format(file_name, depth, dag_width, stage_count, operator_count)
    #     new_line += operators_str
    #     new_line += "\n"
    #     content += new_line
    #     i = i +1

    for file_name in file_names:
        path = prefix + file_name

        # try:

        planList = parsePhysicalPlan(path)
        depth, dag_width, stage_count, operators_dict, operator_count, top_level_nodes, nodes_list = predict_runtime(
                planList,
                qname=path + "_operators")  # 检查有无
        top_syn_nodes = plot_dag_sythesized_nodes(top_level_nodes, nodes_list)
        # traceSQLgenerator = TraceSQLGenerator(top_syn_nodes)
        # traceSQLgenerator.print_final_sql(file_name)
        # dfs_top_syn_nodes(top_syn_nodes)
        operators_str = ",".join([str(operators_dict[op.name]) for op in Operator])
        print(operator_count)
        new_line = '{},{},{},{},{},'.format(file_name, depth, dag_width, stage_count, operator_count)
        new_line += operators_str
        new_line += "\n"
        content += new_line

        # except Exception as e:
        #     print(e)
        # break
    #     # i = i + 1

    # output_file = '../../results/tpcds_generated_sql_plans/103-tpcds_stages_physical_operators.csv'
    output_file = '../../results/tpcds_generated_sql_plans/tpcds_generated_sql_physical_operators.csv'
    with open(output_file, 'w') as file:
        file.write(content)
