import pandas as pd
import openpyxl
import os
import types
from pathlib import Path

VOLUME_TREND_LOG_FILE = r"data/VolumeTrendQuery.csv"
POST_SEARCH_LOG_FILE = r"data/PostSearchQuery.csv"
VOLUME_TREND_OUTPUT_FILE = r"output/volumeTrendQuery_products.csv"
POST_SEARCH_OUTPUT_FILE = r"output/PostSearchQuery_products.csv"
VOLUME_TREND_OUTPUT_FILE_OTHERS = r"output/volumeTrendQuery_others.xlsx"
POST_SEARCH_OUTPUT_FILE_OTHERS = r"output/PostSearchQuery_others.xlsx"

process_filenames = [[VOLUME_TREND_LOG_FILE, VOLUME_TREND_OUTPUT_FILE, VOLUME_TREND_OUTPUT_FILE_OTHERS], \
                     [POST_SEARCH_LOG_FILE, POST_SEARCH_OUTPUT_FILE, POST_SEARCH_OUTPUT_FILE_OTHERS]]
col_list = ["type", "id", "start", "end", "include_model", "keyword", "author", "timezone_offset", "page", "sort",
            "order"]


def log_parser(filename, output_file_others):
    try:
        series_ct = pd.read_csv(filename).iloc[:, 5]
    except FileNotFoundError:
        print(f"{filename} not found!!!")
        return False
    ct_list = series_ct.tolist()
    df_result = pd.DataFrame([], columns=col_list)
    for single_list in ct_list:
        list1_items = single_list.split("\n")
        # print(list1_items)
        row_list = [0, [], 0, 0, 0, 0, 0, 0, 0, 0, 0]
        for item in list1_items:
            sub_list = item.split(":")
            if len(sub_list) == 2:
                row_list[col_list.index(sub_list[0])] = sub_list[1]
            # print(len(sub_list))
        # print(row_list)
        row_series = pd.Series(row_list, index=df_result.columns)
        df_result = df_result.append(row_series, ignore_index=True)
    # print(df_result)

    # Usee groupby to modify this. study https://zhuanlan.zhihu.com/p/101284491   first
    # print(df_result.groupby(["type"]))
    print(df_result["type"].value_counts())
    series_type = df_result["type"].value_counts()
    df_type = pd.DataFrame({'type': series_type.index, 'counts': series_type.values})
    print(df_result["include_model"].value_counts())
    series_include_model = df_result["include_model"].value_counts()
    df_include_model = pd.DataFrame({'include_mode': series_include_model.index, 'counts': series_include_model.values})
    print(df_result["keyword"].value_counts())
    series_keyword = df_result["keyword"].value_counts()
    df_keyword = pd.DataFrame({'keyword': series_keyword.index, 'counts': series_keyword.values})
    print(df_result["author"].value_counts())
    series_author = df_result["author"].value_counts()
    df_author = pd.DataFrame({'author': series_author.index, 'counts': series_author.values})
    writer = pd.ExcelWriter(output_file_others)
    df_type.to_excel(writer, sheet_name="type")
    df_include_model.to_excel(writer, sheet_name="include_model")
    df_keyword.to_excel(writer, sheet_name="keyword")
    df_author.to_excel(writer, sheet_name="author")
    writer.save()
    return df_result


def handle_products(df_input, df_prods, out_filename):
    """ Get the product id list from the id column """
    id_series = df_input["id"]
    # print(id_series)
    id_lists = id_series.tolist()
    # print(id_lists)
    all_ids = []
    print("show string list")
    for id_l in id_lists:
        # print(type(str(id_l)))
        # print(str(id_l).split(","))
        hed_str = str(id_l).replace('[', '').replace(']', '').replace('\"', '')
        # print(hed_str)
        if len(hed_str) > 0:
            f_list = list(map(int, hed_str.split(",")))
            # print(f_list)
            all_ids.extend(f_list)
    # print(all_ids)

    id_counts = pd.value_counts(all_ids)
    # print(id_counts)
    # print(type(id_counts))

    product_ids = id_counts.index.tolist()
    print(product_ids)
    df_query_prods = df_prods.loc[product_ids]
    df_query_prods.insert(2, "counts", id_counts.tolist())
    print(df_query_prods.iloc[:, 0:3])
    df_query_prods.iloc[:, 0:3].to_csv(out_filename)


def remove_output_files():
    out_filelist = [VOLUME_TREND_OUTPUT_FILE, POST_SEARCH_OUTPUT_FILE, VOLUME_TREND_OUTPUT_FILE_OTHERS,
                    POST_SEARCH_OUTPUT_FILE_OTHERS]
    for old_output_file in out_filelist:
        if os.path.exists(old_output_file):
            os.remove(old_output_file)

if Path('./output').exists() == False:
    os.makedirs('./output')

remove_output_files()
df_products = pd.read_csv(r"data/products.csv", index_col=0)
for filenames in process_filenames:
    in_parse_result = log_parser(filenames[0], filenames[2])
    try:
        handle_products(in_parse_result, df_products, filenames[1])
    except:
        print("Parsing Result is Empty!")
