
import sys, os
import sys
import getopt
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
from lstm_sequences_generating import LSTMWrapper, LSTMTagger


# my_list = [1, 2, 3, 4, 5, 6, 7, 8, 9, 10]
#
# # Randomly choose 2 elements from the list
# random_elements = random.sample(my_list, 2)
#
# print(random_elements)
#
# print(random.uniform(0, 1))

def store_sales_test():
    for field in store_sales.fields:
        print(field.name)

    store_sales.with_table_name="temp1"
    sql_generated = store_sales.print_sql()
    print(sql_generated)

def catalog_sales_test():
    for field in catalog_sales.fields:
        print(field.name)

    catalog_sales.with_table_name="temp1"
    sql_generated = catalog_sales.print_sql()
    print(sql_generated)


def parse_arguments(argv):
    input_file = ""
    output_file = ""
    opts, args = getopt.getopt(argv[1:], "hi:o:", ["help", "input_file=", "output_file="])

    for opt, arg in opts:
        if opt in ("-h", "--help"):
            print('script_2.py -i <input_file> -o <output_file>')
            print('or: test_arg.py --input_file=<input_file> --output_file=<output_file>')
            sys.exit()
        elif opt in ("-i", "--input_file"):
            input_file = arg
        elif opt in ("-o", "--output_file"):
            output_file = arg
    print('输入文件为：', input_file)
    print('输出文件为：', output_file)

    # 打印不含'-'或'--'的参数
    for i in range(0, len(args)):
        print('不含' - '或' - -'的参数 %s 为：%s' % (i + 1, args[i]))


if __name__ == '__main__':
    # lstm_wrapper = LSTMWrapper()
    # lstm_prediction = lstm_wrapper.predict("result")

    # for item in range(3):
    #     random.seed(time.time())
    #     r=random.uniform(0,1)
    #     print(r)
    parse_arguments(sys.argv)
    try:
        # catalog_sales_test()
        store_sales_test()
    except Exception as e:
        logging.exception("An error occurred:")
        print(e)