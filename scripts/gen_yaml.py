#!/usr/bin/env python3
"""
Generate dbt schema.yml files from the “l3‑…” JSON descriptions.
Usage:

    python scripts/gen_yaml.py \
        --json-dir "C:/temp/developments/DA-D3 GenX NEU Core/src/config/curated_l3" \
        --models-dir "models/aadp/group_aadp_l3_l4_{env}.staging_l3"

"""

import argparse, json, os, glob
import yaml          # pip install pyyaml

search_string = [
'block',
'dom_minestar_raw_tum',
'dom_minestar_tum',
'drilling',
'hole',
'mining_proc_daily_summary_dom',
'mining_proc_daily_summary_minestar',
'phd_tag_daily_summary',
'phd_tag_processing',
'surface_manager_raw_tum',
'surface_manager_tum',
'tactical_mining_proc_daily_summary',
'tag_daily_summary',
]

search_stringx = [
'dim_block',
'dim_hole',
'fact_drilling',
'fact_mining_proc_daily_summary_minestar',
'fact_phd_tag_daily_summary',
'fact_phd_tag_processing_detail',
'fact_phd_tag_processing_shift',
'fact_tactical_mining_proc_daily_summary',
'fact_tag_daily_summary',
'fact_time_usage_dom_minestar_category_summary',
'fact_time_usage_dom_minestar_shift_summary',
'fact_time_usage_surface_manager_shift_category_summary',
'fact_time_usage_surface_manager_shift_summary',
]

search_stringx = [
'dom_time_usage_raw',
'minestar_cycle_raw',
'osipi_tag_daily_summary',
'osipi_tag_reading',
'phd_reduction_raw',
'phd_tag_reading',
'phd_tag_reduction_cleansed',
'rec_sord_data',
'sgm_qvc_mining_proc_data',
'surface_manager_drilling',
'surface_manager_hole',
]


def json_to_yaml(json_path: str, models_dir: str):
    name = os.path.splitext(os.path.basename(json_path))[0]
    #if name.startswith("l3-"):
    name = name[3:]
        

    with open(json_path) as f:
        jd = json.load(f)

    # split the “database.schema” value
    try:
        db, sch = jd['output_dataset'].get("l3_database", ".").split(".", 1)
    except ValueError:
        db, sch = "group_aadp_l3_l4_{env}.curated_l3", "staging_l3"

    db = db.replace("{env}", "{{var('env')}}")

    cols = []
    for c in jd['output_dataset'].get("schema", []):
        col = {"name": c.get("target_name")}
        if c.get("column_comment"):
            col["description"] = c["column_comment"]
        if c.get("target_type"):
            col["data_type"] = c["target_type"]

        tests = []
        if c.get("nullable") is False:
            tests.append("not_null")

         preserve pk/lc flags in meta
        meta = {'meta': {}}
        if c.get("is_pk"):
            meta['meta']["is_pk"] = True
            col["constraints"] = [{"type": "not_null"}]
            # tests.append("unique")
        if c.get("is_lc"):
            meta['meta']["is_lc"] = True
        if meta['meta']:
            col["config"] = meta

        if tests:
            col["tests"] = tests


        cols.append(col)

    # collect primary key columns at table level
    pk_cols = [f"{c.get('target_name')}" for c in jd['output_dataset'].get("schema", []) if c.get("is_pk")]

    model_entry = {
        "name": name,
        # "config": {"database": db, "schema": sch},
        "columns": cols,
    }

    # add table-level description from the JSON if present
    table_comment = jd['output_dataset'].get('table_comment')
    if table_comment:
        model_entry["description"] = table_comment

    if pk_cols:
        print(f"Found PK columns for {name}: {pk_cols}")
        # add table-level primary key constraint in dbt schema.yml format
        model_entry.setdefault("constraints", [])
        model_entry["constraints"].append({
            "type": "primary_key",
            "columns": pk_cols,
        })
        # add unique_combination_of_columns test for primary keys
        model_entry.setdefault("tests", [])
        model_entry['tests'].append({
            "dbt_utils.unique_combination_of_columns": {
                "arguments": {
                    "combination_of_columns": pk_cols
                }
            }
        })

    model_block = {"version": 2, "models": [model_entry]}

    out_path = os.path.join(models_dir, f"{name}.yml")
    with open(out_path, "w", newline="\n") as out:
        yaml.safe_dump(model_block, out, sort_keys=False)
    print("wrote", out_path)


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--json-dir", required=True,default="C:/temp/developments/DA-D3 GenX NEU Core/src/config/curated_l3",
                        help="directory containing the l3-*.json files")
    parser.add_argument("--models-dir", required=True,default="models/aadp/group_aadp_l3_l4_{env}.staging_l3",
                        help="dbt models directory where the .yml files should go")
    args = parser.parse_args()

    for path in glob.glob(os.path.join(args.json_dir, "l3-*.json")):
        if any(s in path for s in search_string):
            json_to_yaml(path, args.models_dir)


if __name__ == "__main__":
    main()

# l3 case --json-dir "C:\temp\developments\DA-D3 GenX NEU Core\src\config\curated_l3" --models-dir "models\aadp\group_aadp_l3_l4_{env}.curated_l3"    
# l4 case --json-dir "C:\temp\developments\DA-D3 GenX NEU Core\src\config\curated_l4" --models-dir "models\aadp\group_aadp_l3_l4_{env}.curated_l4"

#staging_l3 case --json-dir "C:\temp\developments\DA-D3 GenX NEU Core\src\config\curated_l3" --models-dir "models\aadp\group_aadp_l3_l4_{env}.staging_l3"