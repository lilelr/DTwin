import random
import copy

class Table:
    def __init__(self, name="", type=""):
        self.name = name  # string
        self.type = type  # string: f/d
        self.fields = []  # [Field]
        self.relationships = {}  # {key: table_name, value: [(main_table_key, other_table_key), (main_table_key, other_table_key)]}
        self.join_field_str = None  # connected field name
        self.with_table_name = None
        self.where_fields = []

        self.hash_agg = []
        self.hash_agg_functions = []

        self.sort_agg = []  # save the original fields that are used to peform sort aggregation
        self.sort_agg_functions = []  # save the specific sort hashaggregate opertion, e.g. MAX, SUM
        self.group_by_fields = []
        self.window_fields = []  # [partition_field, partition_field]
        self.is_union= False
        self.decimal_fields =[]

    def addRelationship(self, other_table_name, field1, field2):
        # other_table : other_table_name
        # e.g., store_returns.addRelationship("date_dim", "sr_returned_date_sk", "d_date_sk")
        if other_table_name in self.relationships:
            self.relationships[other_table_name].append((field1, field2))  # field2 is the join field of table2
        else:
            self.relationships[other_table_name] = []
            self.relationships[other_table_name].append((field1, field2))

    def random_save_fields(self):
        new_fields=[]
        for field in self.fields:
            if field in self.where_fields:
                new_fields.append(field)
            elif "identifier" in field.data_type:
                new_fields.append(field)
            else:
                r = random.uniform(0,1)
                if r < 0.3:
                    new_fields.append(field)
        self.fields = new_fields

    def random_project_fields(self):
        add_fields= []
        for field in self.fields:
            if "integer" in field.data_type or "decimal" in field.data_type:
                r = random.uniform(0,1)
                clone_field = copy.deepcopy(field)
                # clone_field.name = "("+field.name +" * 2 "+")"
                clone_field.name = field.name +" * 2 "
                if r < 0.5:
                    add_fields.append(clone_field)
        if add_fields:
            self.fields.extend(add_fields)

    def addField(self, field):
        self.fields.append(field)

    def print_sql(self, use_as=True, union=False):
        if use_as:
            as_sql = self.with_table_name + " AS ("
        else:
            as_sql = ""
        ## deprecated
        # select_sql = "\nSELECT "
        # temp_str = ",".join([str(field.name) for field in self.fields])
        # select_sql += temp_str

        from_sql = "\nFROM " + self.name

        predicate_str = []
        where_sql = " \n "

        if self.where_fields:
            where_field_list = self.where_fields
            random.shuffle(where_field_list) # shuffle where field list
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
            where_sql=" where "
            where_sql += " and ".join(predicate_str)

        # SELECT
        select_sql = "\nSELECT "
        # temp_str = "  " + self.t1_table.with_table_name + ".* "
        # finally connect each field
        temp_str = ",".join([self.name + "." + str(field.name) for field in self.fields])

        # if union:
        #     temp_str = ",".join([self.name + "." + str(field.name) for field in self.fields])
        # else:
        #     temp_str = ",".join([self.with_table_name + "." + str(field.name) for field in self.fields])
        if self.hash_agg:
            temp_str = ""

            for i, group_field in enumerate(self.group_by_fields):
                temp_str += self.name + "." + group_field.name + ", "

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
            temp_str = ""

            for i, group_field in enumerate(self.group_by_fields):
                temp_str += self.name + "." + group_field.name + ", "

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

        group_sql = "\n "
        if self.hash_agg:
            group_sql = "\n GROUP BY "

            group_str = ",".join(
                [self.name + "." + str(field.name) for field in self.group_by_fields])

            group_sql += group_str

        if self.sort_agg:
            group_sql = "\n GROUP BY "

            group_str = ",".join(
                [self.name + "." + str(field.name) for field in self.group_by_fields])

            group_sql += group_str

        # window operation
        if self.window_fields:
            temp_str += ","
            window_str = []
            for i, window_field in enumerate(self.window_fields):
                random_field = random.choice(self.fields)
                while "MAX" in random_field.name or "SUM" in random_field.name or "AVG" in random_field.name or "COUNT" in random_field.name:
                    random_field = random.choice(self.fields)
                item_str = "row_number() over (partition by {} order by {}) as r{}".format(window_field.name,
                                                                                               random_field.name, i)
                window_str.append(item_str)
            temp_str += ",".join(window_str)
            # if self.hash_agg and use_as == False:  # the final join sql
            #     temp_str = ",".join([self.t1_table.with_table_name + "." + str(field.name) for field in self.hash_agg])

        select_sql += temp_str

        result_sql = as_sql + select_sql + from_sql + where_sql+ group_sql+"\n"
        if use_as:
            result_sql += ")"
        # print(result_sql)
        return result_sql


class Field:
    def __init__(self, name, data_type, source_table, numerical_range=None, str_range=None):
        self.name = name  # string
        self.data_type = data_type  # string
        self.source_table = source_table
        self.target_table = None
        self.target_field = None
        self.numerical_range = numerical_range
        self.str_range = str_range

    def setRelationship(self, source_table, target_table, target_field):
        self.source_table = source_table
        self.target_table = target_table
        self.target_field = target_field


class Source:
    def __init__(self):
        self.tb1 = None  # Table
        self.tb2 = None  # Table
        self.join_field1 = []  # [Field]
        self.join_field2 = []  # [Field]
        self.all_fields1 = []  # [Field]
        self.all_fields2 = []  # [Field]
        self.isComplete = False
