from trace_lstm_sequences_generating import LSTMWrapper, LSTMTagger
import copy
import time
import random
import logging

# table_dict = {"store_returns": store_returns, "date_dim": date_dim}

id_to_nodes = {"Af": "FileScan FactTable", "Ad": "FileScan DimTable", "B": "BroadcastHashJoin", "C": "SortMergeJoin",
               "D": "Union", "E": ""}
id_to_operators = {"": "", "1": "Filter", "2": "Project", "3": "HashAggregate", "4": "Sort",
                   "5": "TakeOrderedAndProject", "6": "Expand", "7": "Window", "8": "SortAggregate"}

db_name = "tpcds_100_parquet"


def generate_long_chain(given_len):
    b_pro = 0.19  # BHJ
    s_pro = 0.67  # SMJ
    union_pro = 0.13  # Union（这部分是估算的 圆饼图里没有显示）
    given_len = given_len - 1
    long_chain = ["Af"]

    while given_len > 0:

        # print(random.random())

        r = random.random()
        if r < b_pro:
            long_chain.append("C")
        elif r < b_pro + s_pro:
            long_chain.append("C")
        else:
            long_chain.append("D")
        given_len -= 1
    long_chain.append("E")
    result = ""
    temp_len = len(long_chain)
    for i in range(0, temp_len - 1):
        result += (long_chain[i] + long_chain[i + 1]) + " "
    result = result[:-1]
    print(result)
    print(long_chain)
    lstm_wrapper = LSTMWrapper()
    lstm_prediction = lstm_wrapper.predict(result)
    full_list = []
    for i in range(0, temp_len - 1):
        start_item = id_to_nodes[long_chain[i]]
        # end_item = id_to_nodes[long_chain[i+1]]
        full_list.append(start_item)
        for item in lstm_prediction[i]:
            full_list.append(item)
    full_list.append(id_to_nodes[long_chain[temp_len - 1]])
    print("full list is {}".format(full_list))

    return full_list
