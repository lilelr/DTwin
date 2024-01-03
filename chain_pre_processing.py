# from lstm_sequences_generating import LSTMWrapper, LSTMTagger
import copy
import time
import random
import logging


# table_dict = {"store_returns": store_returns, "date_dim": date_dim}

class FrequencyOperator():
    def __init__(self):
        # self.frequency_list = ["Filter->Project", "Filter"]
        # self.operator_list = [0.87, 0.13]

        self.frequency_list = []
        self.operator_list = []

    def random_get_operator(self):
        r = random.uniform(0, 1)
        cumulate = 0
        for i, fre in enumerate(self.frequency_list):
            if i == 0:
                cumulate = fre
            else:
                cumulate = cumulate + fre
            if fre >= r:
                return self.operator_list[i]
        return self.operator_list[-1]


id_to_nodes = {"Af": "FileScan FactTable", "Ad": "FileScan DimTable", "B": "BroadcastHashJoin", "C": "SortMergeJoin",
               "D": "Union", "E": ""}

# pair_frequency = {"AdB":{}}
# file_d_bhj = {}

id_to_operators = {"": "", "1": "Filter", "2": "Project", "3": "HashAggregate", "4": "Sort",
                   "5": "TakeOrderedAndProject", "6": "Expand", "7": "Window", "8": "SortAggregate"}

# id_to_operators = {"": "", "1": "Filter",
#                    "2": "Project",
#                    "3": "HashAggregate-HashAggregate",
#                    "4": "Sort", "5": "TakeOrderedAndProject",
#                    "6": "Expand", "7": "Window"}

db_name = "tpcds_100_parquet"


# def generate_long_chain(given_len):
#     b_pro = 10.7 / (10.7 + 3.75 + 0.4)  # BHJ
#     s_pro = 3.75 / (10.7 + 3.75 + 0.4)  # SMJ
#     union_pro = 0.4 / (10.7 + 3.75 + 0.4)  # Union（这部分是估算的 圆饼图里没有显示）
#     given_len = given_len - 1
#     long_chain = ["Af"]
#
#     while given_len > 0:
#
#         # print(random.random())
#
#         r = random.random()
#         if r < b_pro:
#             long_chain.append("B")
#         elif r < b_pro + s_pro:
#             long_chain.append("C")
#         else:
#             long_chain.append("D")
#         given_len -= 1
#     long_chain.append("E")
#     result = ""
#     temp_len = len(long_chain)
#     for i in range(0, temp_len - 1):
#         result += (long_chain[i] + long_chain[i + 1]) + " "
#     result = result[:-1]
#     print(result)
#     print(long_chain)
#     lstm_wrapper = LSTMWrapper()
#     lstm_prediction = lstm_wrapper.predict(result)
#     full_list = []
#     for i in range(0, temp_len - 1):
#         start_item = id_to_nodes[long_chain[i]]
#         # end_item = id_to_nodes[long_chain[i+1]]
#         full_list.append(start_item)
#         for item in lstm_prediction[i]:
#             full_list.append(item)
#     full_list.append(id_to_nodes[long_chain[temp_len - 1]])
#     print("full list is {}".format(full_list))
#
#     return full_list


def generate_long_chain_probability(given_len):
    b_pro = 10.7 / (10.7 + 3.75 + 0.4)  # BHJ
    s_pro = 3.75 / (10.7 + 3.75 + 0.4)  # SMJ
    union_pro = 0.4 / (10.7 + 3.75 + 0.4)  # Union（这部分是估算的 圆饼图里没有显示）
    given_len = given_len - 1
    long_chain = ["Af"]

    while given_len > 0:

        # print(random.random())

        r = random.random()
        if r < b_pro:
            long_chain.append("B")
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

    full_list = []
    scan_d_bhj = FrequencyOperator()
    scan_d_bhj.operator_list = ["Filter->Project", "Filter"]
    scan_d_bhj.frequency_list = [0.87, 0.13]

    scan_f_bhj = FrequencyOperator()
    scan_f_bhj.operator_list = ["Filter", "Filter->Project", "Filter->Project->HashAggregate->Filter->Project"]
    scan_f_bhj.frequency_list = [0.518, 0.444, 0.037]

    scan_f_smj = FrequencyOperator()
    scan_f_smj.operator_list = ["Filter->Sort"]
    scan_f_smj.frequency_list = [1]

    bhj_smj = FrequencyOperator()
    bhj_smj.operator_list = ["Project->Sort", "Project->HashAggregate->Sort", "Project->HashAggregate->Filter->Sort",
                             "Project->HashAggregate->HashAggregate->Filter->Sort",
                             "Project->HashAggregate->Sort->Window->Filter->Window->Filter->Project->Sort", "Sort",
                             "Project->HashAggregate->Sort->Window->Project->sort"]
    bhj_smj.frequency_list = [0.5, 0.20, 0.16, 0.045, 0.045, 0.0227, 0.0227]
    print("bhj_smj length and frequency list length are {} and {}".format(len(bhj_smj.operator_list),
                                                                          len(bhj_smj.frequency_list)))

    smj_bhj = FrequencyOperator()
    smj_bhj.operator_list = ["Project", "", "Filter->Project", "Project->HashAggregate"]
    smj_bhj.frequency_list = [0.78, 0.108, 0.081, 0.027]

    bhj_bhj = FrequencyOperator()
    bhj_bhj.operator_list = ["Project", "", "Project->HashAggregate", "Project->HashAggregate->Filter"]
    bhj_bhj.frequency_list = [0.935, 0.035, 0.023, 0.006]

    smj_smj = FrequencyOperator()
    smj_smj.operator_list = ["Project", "Project->sort", "", "Project->HashAggregate->Sort",
                             "Project->HashAggregate->Filter->Project->Sort", "Project->HashAggregate"]
    smj_smj.frequency_list = [0.444, 0.25, 0.222, 0.027, 0.027, 0.027]

    bhj_end = FrequencyOperator()
    bhj_end.operator_list = ["Project->HashAggregate->TakeOrderedAndProject", "Project->HashAggregate",
                             "Project->HashAggregate->HashAggregate",
                             "Project->HashAggregate->Sort->Window->Filter->Project>TakeOrderedAndProject",
                             "Project->HashAggregate->Sort->Window->Project",
                             "Project->Expand->HashAggregate->TakeOrderedAndProject",
                             "Project->HashAggregate->Project", "Project->TakeOrderedAndProject", "Project",
                             "Project->HashAggregate->Sort", "Project->Expand->HashAggregate",
                             "Project->HashAggregate->Filter",
                             "Project->Expand->HashAggregate->Sort->Window->Project->TakeOrderedAndProject",
                             "Filter->HashAggregate->Project", "Filter->Project->HashAggregate->TakeOrderedAndProject",
                             "Project->HashAggregate->HashAggregate->HashAggregate->HashAggregate->Project",
                             "Project->Expand->HashAggregate->Sort->Window->Filter",
                             "Project->Expand->HashAggregate->Sort->Window->Project",
                             "Expand->HashAggregate->Sort->Window->TakeOrderedAndProject",
                             "Project->HashAggregate->Sort->Window->Filter->Project", "TakeOrderedAndProject", ""]
    bhj_end.frequency_list = [0.259, 0.185, 0.0555, 0.0555, 0.037, 0.037, 0.037, 0.037, 0.037, 0.037, 0.0185, 0.0185,
                              0.0185, 0.0185, 0.0185, 0.0185, 0.0185, 0.0185, 0.0185, 0.0185, 0.0185, 0.0185]
    print("bhj_end length and frequency list length are {} and {}".format(len(bhj_end.operator_list),
                                                                          len(bhj_end.frequency_list)))

    smj_end = FrequencyOperator()
    smj_end.operator_list = ["Project->TakeOrderedAndProject", "Project->sort", "",
                             "Project->HashAggregate->TakeOrderedAndProject",
                             "Project->Hashaggregate", "Project", "Project->Project",
                             "Project->HashAggregate->HashAggregate->Filter->TakeOrderedAndProject",
                             "Project->Sort->Window->Filter->TakeOrderedAndProject",
                             "Filter->Project->TakeOrderedAndProject"]
    smj_end.frequency_list = [0.384, 0.192, 0.115, 0.115, 0.038, 0.038, 0.038, 0.038, 0.038, 0.038]

    scan_f_union = FrequencyOperator()
    scan_f_union.operator_list = ["Filter->Project"]
    scan_f_union.frequency_list = [1]

    union_bhj = FrequencyOperator()
    union_bhj.operator_list = [""]
    union_bhj.frequency_list = [1]

    smj_union = FrequencyOperator()
    smj_union.operator_list = ["Project"]
    smj_union.frequency_list = [1]

    print("smj_end length and frequency list length are {} and {}".format(len(smj_end.operator_list),
                                                                          len(smj_end.frequency_list)))

    bhj_union = FrequencyOperator()
    bhj_union.operator_list = ["Project->HashAggregate", "Project","Project->HashAggregate->Sort->Window->Sort"
                                                                   "->Window->Filter->Project"]
    bhj_union.frequency_list = [0.625, 0.25, 0.125]

    union_smj = FrequencyOperator()
    union_smj.operator_list = [""]
    union_smj.frequency_list = [1]

    union_end = FrequencyOperator()
    union_end.operator_list = ["HashAggregate->TakeOrderedAndProject", "HashAggregate->Project", "Expand->HashAggregate->TakeOrderedAndProject"]
    union_end.frequency_list = [0.625, 0.25, 0.125]


    for i in range(0, temp_len - 1):
        pair = long_chain[i] + long_chain[i + 1]
        left_item = id_to_nodes[long_chain[i]]
        # end_item = id_to_nodes[long_chain[i+1]]
        full_list.append(left_item)
        predict_operators = ""
        if pair == "AfB":
            predict_operators = scan_f_bhj.random_get_operator()
        elif pair == "AfC":
            predict_operators = scan_f_smj.random_get_operator()
        elif pair == "BB":
            predict_operators = bhj_bhj.random_get_operator()
        elif pair == "BC":
            predict_operators = bhj_smj.random_get_operator()
        elif pair == "CC":
            predict_operators = smj_smj.random_get_operator()
        elif pair == "BE":
            predict_operators = bhj_end.random_get_operator()
        elif pair == "CE":
            predict_operators = smj_end.random_get_operator()
        elif pair == "AfD":
            predict_operators = scan_f_union.random_get_operator()
        elif pair =="BD":
            predict_operators =bhj_union.random_get_operator()
        elif pair == "CD":
            predict_operators = smj_union.random_get_operator()
        elif pair =="DE":
            predict_operators = union_end.random_get_operator()

        if predict_operators == "":
            continue

        for item in predict_operators.split("->"):
            full_list.append(item)
        # for item in lstm_prediction[i]:
        #     full_list.append(item)
    full_list.append(id_to_nodes[long_chain[temp_len - 1]])
    print("full list is {}".format(full_list))
    return full_list


def trace_generate_long_chain_probability(given_len):
    b_pro = 10.7 / (10.7 + 3.75 + 0.4)  # BHJ
    s_pro = 3.75 / (10.7 + 3.75 + 0.4)  # SMJ
    union_pro = 0.4 / (10.7 + 3.75 + 0.4)  # Union（这部分是估算的 圆饼图里没有显示）
    given_len = given_len - 1
    long_chain = ["Af"]

    while given_len > 0:

        # print(random.random())

        r = random.random()
        if r < b_pro:
            long_chain.append("B")
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

    full_list = []

    scan_f_bhj = FrequencyOperator()
    scan_f_bhj.operator_list = ["Filter", "Filter->Project->Filter->Project", "Filter->Project->HashAggregate"]
    scan_f_bhj.frequency_list = [0.5, 0.25, 0.25]

    scan_f_smj = FrequencyOperator()
    scan_f_smj.operator_list = ["Filter->Project->HashAggregate->Sort","Filter->Project","Filter->Project->Sort->SortAggregate->Sort->SortAggregate->Filter->Project->Sort","Filter->Project->Sort->SortAggregate->Sort->SortAggregate","Filter->Project->Sort"]
    scan_f_smj.frequency_list = [0.33, 0.16, 0.16, 0.16,0.16]

    bhj_smj = FrequencyOperator()
    bhj_smj.operator_list = ["Filter->sort"]
    bhj_smj.frequency_list = [1]
    print("bhj_smj length and frequency list length are {} and {}".format(len(bhj_smj.operator_list),
                                                                          len(bhj_smj.frequency_list)))

    smj_bhj = FrequencyOperator()
    smj_bhj.operator_list = ["Project", "", "Filter->Project", "Project->HashAggregate"]
    smj_bhj.frequency_list = [0.78, 0.108, 0.081, 0.027]

    bhj_bhj = FrequencyOperator()
    bhj_bhj.operator_list = ["Project"]
    bhj_bhj.frequency_list = [1]

    smj_smj = FrequencyOperator()
    smj_smj.operator_list = ["Project->HashAggregate->Sort", "Project", ""]
    smj_smj.frequency_list = [0.5, 0.25, 0.25]

    bhj_end = FrequencyOperator()
    bhj_end.operator_list = ["Project",
                             "Project->Sort->Window->Sort->Window->Sort->Window->Sort->Window->Sort->Window->Sort->Window"]
    bhj_end.frequency_list = [0.67, 0.33]
    print("bhj_end length and frequency list length are {} and {}".format(len(bhj_end.operator_list),
                                                                          len(bhj_end.frequency_list)))

    smj_end = FrequencyOperator()
    smj_end.operator_list = ["Project->HashAggregate->HashAggregate", "Project", "Project->Sort->SortAggregate->Sort->SortAggregate"]
    smj_end.frequency_list = [0.34, 0.33, 0.33]

    scan_f_union = FrequencyOperator()
    scan_f_union.operator_list = ["Filter->Project","Filter->Project->Expand->HashAggregate->HashAggregate->Sort->Window->Filter->Project"]
    scan_f_union.frequency_list = [0.67, 0.33]

    union_bhj = FrequencyOperator()
    union_bhj.operator_list = [""]
    union_bhj.frequency_list = [1]

    smj_union = FrequencyOperator()
    smj_union.operator_list = [""]
    smj_union.frequency_list = [1]

    print("smj_end length and frequency list length are {} and {}".format(len(smj_end.operator_list),
                                                                          len(smj_end.frequency_list)))

    bhj_union = FrequencyOperator()
    bhj_union.operator_list = ["Filter"]
    bhj_union.frequency_list = [1]

    union_smj = FrequencyOperator()
    union_smj.operator_list = [""]
    union_smj.frequency_list = [1]

    union_end = FrequencyOperator()
    union_end.operator_list = [""]
    union_end.frequency_list = [1]


    for i in range(0, temp_len - 1):
        pair = long_chain[i] + long_chain[i + 1]
        left_item = id_to_nodes[long_chain[i]]
        # end_item = id_to_nodes[long_chain[i+1]]
        full_list.append(left_item)
        predict_operators = ""
        if pair == "AfB":
            predict_operators = scan_f_bhj.random_get_operator()
        elif pair == "AfC":
            predict_operators = scan_f_smj.random_get_operator()
        elif pair == "BB":
            predict_operators = bhj_bhj.random_get_operator()
        elif pair == "BC":
            predict_operators = bhj_smj.random_get_operator()
        elif pair == "CC":
            predict_operators = smj_smj.random_get_operator()
        elif pair == "BE":
            predict_operators = bhj_end.random_get_operator()
        elif pair == "CE":
            predict_operators = smj_end.random_get_operator()
        elif pair == "AfD":
            predict_operators = scan_f_union.random_get_operator()
        elif pair =="BD":
            predict_operators =bhj_union.random_get_operator()
        elif pair == "CD":
            predict_operators = smj_union.random_get_operator()
        elif pair =="DE":
            predict_operators = union_end.random_get_operator()

        if predict_operators == "":
            continue

        for item in predict_operators.split("->"):
            full_list.append(item)
        # for item in lstm_prediction[i]:
        #     full_list.append(item)
    full_list.append(id_to_nodes[long_chain[temp_len - 1]])
    print("full list is {}".format(full_list))
    return full_list


if __name__ == '__main__':
    given_length = 3
    # generate_long_chain_probability(given_length)
    trace_generate_long_chain_probability(given_length)
