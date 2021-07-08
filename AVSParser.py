import pandas as pd

# series_ct = pd.read_csv(r"data\VolumeTrendQuery.csv").iloc[:, 5]
series_ct = pd.read_csv(r"data\PostSearchQuery.csv").iloc[:, 5]
# print(series_ct)
# print(type(series_ct))
ct_list = series_ct.tolist()
# print(ct_list)
col_list = ["type", "id", "start", "end", "include_model", "keyword", "author", "timezone_offset", "page", "sort", "order"]
df_result = pd.DataFrame([],
                         columns=["type", "id", "start", "end", "include_model", "keyword", "author", "timezone_offset",
                                  "page", "sort", "order"])
for single_list in ct_list:
    list1_items = single_list.split("\n")
    print(list1_items)
    row_list = [0, [], 0, 0, 0, 0, 0, 0, 0, 0, 0]
    for item in list1_items:
        sub_list = item.split(":")
        if len(sub_list) == 2:
            row_list[col_list.index(sub_list[0])] = sub_list[1]
        # print(len(sub_list))
    print(row_list)
    row_series = pd.Series(row_list, index=df_result.columns)
    df_result = df_result.append(row_series, ignore_index=True)
print(df_result)

print(df_result["type"].value_counts())
print(df_result["include_model"].value_counts())
print(df_result["keyword"].value_counts())
print(df_result["author"].value_counts())

#handle product id

id_series = df_result["id"]
# print(id_series)
id_lists = id_series.tolist()
# print(id_lists)
all_ids = []
print("show string list")
for id_l in id_lists:
    # print(type(str(id_l)))
    # print(str(id_l).split(","))
    hed_str = str(id_l).replace('[', '').replace(']', '').replace('\"', '')
    print(hed_str)
    if len(hed_str) > 0:
        f_list = list(map(int, hed_str.split(",")))
        print(f_list)
        all_ids.extend(f_list)
print(all_ids)

id_counts = pd.value_counts(all_ids)
print(id_counts)
print(type(id_counts))

product_ids = id_counts.index.tolist()
print(product_ids)


df = pd.read_csv(r"data\products.csv", index_col=0)
print(df)
products_df = df.loc[product_ids]
products_df.insert(2, "counts", id_counts.tolist())
print(products_df.iloc[:, 0:3])
# products_df.iloc[:, 0:3].to_csv(r"output\volumeTrendQuery_products.csv")
products_df.iloc[:, 0:3].to_csv(r"output\postSearchQuery_products.csv")
