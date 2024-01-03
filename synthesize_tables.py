# from core.edit_distance import edit_dist, best_fit
#
# from tpc_ds_tables import store_returns, date_dim, store_sales, time_dim, item, customer, customer_demographics, \
#     customer_address, household_demographics, store, promotion, reason

import random
import time
import copy


# record the detailed
class TableField:
    def __init__(self, field_id=-2):
        self.field_id = field_id
        self.field_filters = []

    def add_table_field_filters(self, str_filter):
        self.field_filters.append(str_filter)

    def show_details_for_field(self):
        return str("field_id:" + str(self.field_id) + " " + "field_filters:" + str(self.field_filters))


class JoinTable:
    # class static members
    alias_fields = {}
    # the match between the renamed fields. renamed name  (key) ->  original_name (value)
    # e.g., ctr_store_sk -> sr_store_sk
    alias_fields_no_tbname = {}

    def __init__(self, t1="", t2="", with_table_name="", t1_table=None, t2_table=None):
        self.t1 = t1
        self.t2 = t2
        self.t1_table = t1_table
        self.t2_table = t2_table
        # self.table = [] # FROM
        # self.constraint = {} # WHERE:key->field_name (string); constraint (string)
        self.join_fields = []  # join[0] = tb1.field_name, join[1]=tb2.field_name
        # self.hash_agg = []  # WHERE
        self.fields = []
        self.project_field = []  # SELECT
        self.origin_field_names = {}  # key: alias_name , value: original_name
        # e.g., ws_sold_date_sk AS sold_date_sk, we will have sold_date_sk -> ws_sold_date_sk
        self.sort = []
        self.with_table_name = "tb" + with_table_name
        self.hash_agg = [] # save the original fields that are used to perform aggregation
        self.hash_agg_functions = [] # save the specific hashaggregate opertion, e.g. MAX, SUM

        self.sort_agg = [] # save the original fields that are used to peform sort aggregation
        self.sort_agg_functions = [] # save the specific sort hashaggregate opertion, e.g. MAX, SUM

        self.group_by_fields = []
        self.is_sort_merge_join = False
        self.is_broadcast_exchange = False
        self.is_union = False
        self.union_count_tables = 0  # record how many tables should be unioned together
        self.join_field_str = None  # 连接的字段名
        self.relationships = {}  # (table, string, string),(),...

        self.special_str_list = []
        self.where_fields = []
        self.window_fields = []  # [partition_field, partition_field]

    def is_project_field_empty(self):
        return len(self.project_field) == 0

    def add_field_filters(self, field_name, field_filter):
        if field_name not in self.fields:
            self.fields[field_name] = TableField()
        self.fields[field_name].add_table_field_filters(field_filter)

    def print_sql(self, use_as=True, union=False):
        as_sql = ""
        if use_as:
            as_sql = self.with_table_name + " AS ("

        if self.is_union:  # cope with the particularly union join node

            # sql2 = "\n" + self.t1_table.with_table_name + "  \n UNION ALL \n " + self.t2_table.with_table_name
            t1_table_sql = self.t1_table.print_sql(use_as=False, union=True)
            t2_table_sql = self.t2_table.print_sql(use_as=False, union=True)
            union_sql = "\n" + t1_table_sql + "  \n UNION ALL \n " + t2_table_sql
            temp_count = self.union_count_tables
            while temp_count > 2:  # if coming across multiple unions, we need to add more union operations
                union_sql += " \n UNION ALL \n" + t2_table_sql
                temp_count -= 1
            result_sql = as_sql + union_sql + "\n"
            if use_as:
                result_sql += ")"
            return result_sql

        # FROM
        from_sql = "\nFROM " + self.t1_table.with_table_name + " , " + self.t2_table.with_table_name
        where_sql = "\nWHERE "
        # first_field_join = self.t1_table.with_table_name + "." + self.t1_table.join_field_str
        # self.t1_table=store_sales
        # first_field_join = self.t1_table.with_table_name + "." + self.join_fields[0]
        # second_field_join = self.t2_table.with_table_name + "." + self.join_fields[1]
        # second_field_join = self.t2_table.with_table_name + "." + self.t2_table.join_field_str
        if type(self.t2_table) == JoinTable:
            join_fields_list = self.t1_table.relationships[self.t2_table.t1_table.name]
        else:
            join_fields_list = self.t1_table.relationships[self.t2_table.name]
        # (field1, filed2)  = self.t1_table.relationships[self.t2_table]
        random.shuffle(join_fields_list)
        join_array_str = []

        temp_len = len(join_fields_list)
        random.seed(time.time())
        r = random.uniform(0, 1)
        print("r is {}".format(r))

        for index, (field1_name, filed2_name) in enumerate(join_fields_list):
            first_field_join = self.t1_table.with_table_name + "." + field1_name
            second_field_join = self.t2_table.with_table_name + "." + filed2_name
            temp_connect_sql = (first_field_join + " = " + second_field_join)
            print(
                "self.t1_table.name is {}, first_field_join is {}, self.t2_table.name is {}, second_field_join is {}".format(
                    self.t1_table.with_table_name, first_field_join, self.t2_table.with_table_name, second_field_join))
            join_array_str.append(temp_connect_sql)

            # temp_max = "max( "+first_field_join+" )"
            # temp_max = " count(*)  "
            # max_selects.append(temp_max)
            # for field in self.t1_table.fields:
            #     if field.name == field1_name:
            #         self.hash_agg.append(field)

            if r < 0.5:  # only select one
                break

        where_sql += " and ".join(join_array_str)

        # dimensional table selects
        predicate_str = []

        if self.t2_table.where_fields and self.t2_table.group_by_fields is None :
            where_field_list = self.t2_table.where_fields
            random.shuffle(where_field_list)
            item_first = where_field_list[0]
            if "integer" in item_first.data_type or "decimal" in item_first.data_type:
                random.shuffle(item_first.numerical_range)
                temp_predicate = item_first.name + "=" + str(item_first.numerical_range[0])
                predicate_str.append(temp_predicate)
            else:
                random.shuffle(item_first.str_range)
                temp_predicate = item_first.name + "=\'" + item_first.str_range[0] + '\''
                predicate_str.append(temp_predicate)
        if predicate_str:
            where_sql += " and "
            where_sql += " and ".join(predicate_str)

        group_sql = "\n "
        if self.hash_agg:
            group_sql = "\n GROUP BY "

            group_str = ",".join(
                [field.source_table + "." + str(field.name) for field in self.group_by_fields])

            group_sql += group_str

        if self.sort_agg:
            group_sql = "\n GROUP BY "

            group_str = ",".join(
                [field.source_table + "." + str(field.name) for field in self.group_by_fields])

            group_sql += group_str

        # SELECT
        select_str = "\nSELECT "
        # temp_str = "  " + self.t1_table.with_table_name + ".* "
        # finally connect each field
        # 12-25 chrismas lele
        # t1_table_fields_str = ",".join([self.t1_table.with_table_name + "." + str(field.name) for field in self.t1_table.fields])
        t1_table_fields_str = ",".join([self.t1_table.with_table_name + "." + str(field.name) for field in self.t1_table.fields])
        temp_arr = []
        for field in self.t2_table.fields:
            temp_field_name = str(field.name)
            if temp_field_name not in t1_table_fields_str:
                temp_arr.append(self.t2_table.with_table_name + "."+temp_field_name)
        t2_table_fields_str = ",".join(temp_arr)
        temp_str = t1_table_fields_str + " , " + t2_table_fields_str

        if self.hash_agg:
            temp_str = " "

            for i, group_field in enumerate(self.group_by_fields):
                temp_str += group_field.source_table + "." + group_field.name + ", "

            t_len = len(self.hash_agg)
            for i, agg_field in enumerate(self.hash_agg):
                agg_fun_name = self.hash_agg_functions[i]
                new_field_name = agg_field.name + "_" + self.hash_agg_functions[i]
                temp_item_str = agg_fun_name + "(" + agg_field.name + ") AS " + new_field_name
                # self.fields.append(new_field)
                if i < t_len - 1:
                    temp_str += temp_item_str + ", "
                else:
                    temp_str += temp_item_str

        if self.sort_agg: # sort_aggregate fields
            temp_str = " "

            for i, group_field in enumerate(self.group_by_fields):
                temp_str += group_field.source_table + "." + group_field.name + ", "

            t_len = len(self.sort_agg)
            for i, agg_field in enumerate(self.sort_agg):
                agg_fun_name = self.sort_agg_functions[i]
                new_field_name = agg_field.name + "_" + self.sort_agg_functions[i]
                temp_item_str = agg_fun_name + "(cast(cast( " + agg_field.name + " as BIGINT)  as string)) AS " + new_field_name
                # self.fields.append(new_field)
                if i < t_len - 1:
                    temp_str += temp_item_str + ", "
                else:
                    temp_str += temp_item_str

        # window operation
        if self.window_fields:
            temp_str += ","
            window_str = []
            for i, window_field in enumerate(self.window_fields):
                random_field = random.choice(self.fields)
                item_str = "row_number() over (partition by {} order by {}) as r{}".format(window_field.name,
                                                                                           random_field.name, i)
                window_str.append(item_str)
            temp_str += ",".join(window_str)
        # if self.hash_agg and use_as == False:  # the final join sql
        #     temp_str = ",".join([self.t1_table.with_table_name + "." + str(field.name) for field in self.hash_agg])

        select_str += temp_str

        # from self.fields

        result_sql = as_sql + select_str + from_sql + where_sql + group_sql
        if use_as:
            result_sql += ")"
        return result_sql
