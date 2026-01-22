import json
import os
import shutil
import zipfile
from operator import methodcaller

import numpy as np
import pandas as pd
import sys

DISNEY_TO_MTV_DATE = '2025-01-01'

# Contancts tab
## Sex Age
def postprocessing_standard_tabcontacts_df_contact_sexage_abs_raw(path_tables, label_object, campaign_par):
    """

    :param str path_tables: path of the directory which the json files are stored
    :param label_object:
    :param campaign_par:
    :return:
    """

    # Read table
    df_contact_sexage = _extract_df_from_json_file(path_tables, 'impacts_by_sex_age')

    # Group data
    col_to_group = ['broadcaster', 'device_type', 'ad_type', 'sex', 'age_break']

    if df_contact_sexage.empty:
        df_contact_sexage[col_to_group + ['num_impacts']] = None

    df_contact_sexage = df_contact_sexage.groupby(col_to_group, as_index=False)['num_impacts'].sum()

    # Scaffolding
    df_scaffolding = _scaffolding_contacts(col_to_group, label_object)

    df_contact_sexage = df_scaffolding.merge(df_contact_sexage, how='left', on=col_to_group)
    df_contact_sexage['num_impacts'] = df_contact_sexage['num_impacts'].fillna(0)

    # Pivot Table
    df_contact_sexage = df_contact_sexage.pivot(columns=['sex', 'age_break'],
                                                index=['broadcaster', 'device_type', 'ad_type'],
                                                values='num_impacts').fillna(0)

    df_contact_sexage['_total'] = df_contact_sexage.sum(axis=1)

    df_contact_sexage = df_contact_sexage / 1000

    df_contact_sexage = df_contact_sexage.reset_index()

    # Add Custom labels
    _custom_labels(df_contact_sexage, label_object)

    # Rename columns
    df_contact_sexage = df_contact_sexage.rename(columns=label_object['replace']['sex'])
    df_contact_sexage = df_contact_sexage.rename(columns=label_object['replace']['age_break'])

    # Flat dataframe
    concat_index_columns = list(map(lambda x: ''.join(x), df_contact_sexage.columns.values))
    df_contact_sexage.columns = concat_index_columns

    # Format rows order
    df_contact_sexage = _format_rows(df_contact_sexage, label_object)
    df_contact_sexage = df_contact_sexage.drop_duplicates()

    # Format columns order
    col_sex_age = sorted(x for x in df_contact_sexage.columns if x.startswith("M") | x.startswith("W"))
    col_to_use = ['Type'] + col_sex_age + ['Total']
    df_contact_sexage = df_contact_sexage[col_to_use]

    return df_contact_sexage


def postprocessing_standard_tabcontacts_df_contact_sexage_abs_30eq(path_tables, label_object, campaign_par):
    df_contact_sexage = _extract_df_from_json_file(path_tables, 'impacts_by_sex_age')

    col_to_group = ['broadcaster', 'device_type', 'ad_type', 'sex', 'age_break']

    if df_contact_sexage.empty:
        df_contact_sexage[col_to_group + ['num_30sec_eq_impacts']] = None

    # Group data
    df_contact_sexage = df_contact_sexage.groupby(col_to_group, as_index=False)['num_30sec_eq_impacts'].sum()

    # Scaffolding
    df_scaffolding = _scaffolding_contacts(col_to_group, label_object)

    df_contact_sexage = df_scaffolding.merge(df_contact_sexage, how='left', on=col_to_group)
    df_contact_sexage['num_30sec_eq_impacts'] = df_contact_sexage['num_30sec_eq_impacts'].fillna(0)

    # Pivot Table
    df_contact_sexage = df_contact_sexage.pivot(columns=['sex', 'age_break'],
                                                index=['broadcaster', 'device_type', 'ad_type'],
                                                values='num_30sec_eq_impacts').fillna(0)

    df_contact_sexage['_total'] = df_contact_sexage.sum(axis=1)

    df_contact_sexage = df_contact_sexage / 1000

    df_contact_sexage = df_contact_sexage.reset_index()

    # Add Custom labels
    _custom_labels(df_contact_sexage, label_object)

    # Rename columns
    df_contact_sexage = df_contact_sexage.rename(columns=label_object['replace']['sex'])
    df_contact_sexage = df_contact_sexage.rename(columns=label_object['replace']['age_break'])

    # Flat dataframe
    concat_index_columns = list(map(lambda x: ''.join(x), df_contact_sexage.columns.values))
    df_contact_sexage.columns = concat_index_columns

    # Format rows order
    df_contact_sexage = _format_rows(df_contact_sexage, label_object)
    df_contact_sexage = df_contact_sexage.drop_duplicates()

    # Format columns order
    col_sex_age = sorted(x for x in df_contact_sexage.columns if x.startswith("M") | x.startswith("W"))
    col_to_use = ['Type'] + col_sex_age + ['Total']
    df_contact_sexage = df_contact_sexage[col_to_use]

    return df_contact_sexage


def postprocessing_standard_tabcontacts_df_contact_sexage_trp_raw(path_tables, label_object, campaign_par):
    """

    :param str path_tables: path of the directory which the json files are stored
    :param label_object:
    :param campaign_par:
    :return:
    """

    # Read table
    df_contact_sexage = _extract_df_from_json_file(path_tables, 'impacts_by_sex_age')
    df_universe_by_sexage = _extract_df_from_json_file(path_tables, 'universe_by_sex_age')
    df_universe_by_sexage = df_universe_by_sexage[df_universe_by_sexage['date'] == df_universe_by_sexage['date'].min()]

    col_to_group = ['broadcaster', 'device_type', 'ad_type', 'sex', 'age_break']

    if df_contact_sexage.empty:
        df_contact_sexage[col_to_group + ['num_impacts']] = None

    # Group data
    df_contact_sexage = df_contact_sexage.groupby(col_to_group, as_index=False)['num_impacts'].sum()

    # Compute TRPs
    df_contact_sexage = df_contact_sexage.merge(df_universe_by_sexage.drop(columns=['date']), on=['sex', 'age_break'])
    df_contact_sexage['num_trp'] = df_contact_sexage['num_impacts'] / df_contact_sexage['universe'] * 100
    df_contact_sexage = df_contact_sexage.drop(columns=['num_impacts', 'universe'])

    # Scaffolding
    df_scaffolding = _scaffolding_contacts(col_to_group, label_object)

    df_contact_sexage = df_scaffolding.merge(df_contact_sexage, how='left', on=col_to_group)
    df_contact_sexage['num_trp'] = df_contact_sexage['num_trp'].fillna(0)

    # Pivot Table
    df_contact_sexage = df_contact_sexage.pivot(columns=['sex', 'age_break'],
                                                index=['broadcaster', 'device_type', 'ad_type'],
                                                values='num_trp').fillna(0)

    df_contact_sexage['_total'] = df_contact_sexage.sum(axis=1)
    df_contact_sexage = df_contact_sexage.reset_index()

    # Add Custom labels
    _custom_labels(df_contact_sexage, label_object)

    # Rename columns
    df_contact_sexage = df_contact_sexage.rename(columns=label_object['replace']['sex'])
    df_contact_sexage = df_contact_sexage.rename(columns=label_object['replace']['age_break'])

    # Flat dataframe
    concat_index_columns = list(map(lambda x: ''.join(x), df_contact_sexage.columns.values))
    df_contact_sexage.columns = concat_index_columns

    # Format rows order
    df_contact_sexage = _format_rows(df_contact_sexage, label_object)
    df_contact_sexage = df_contact_sexage.drop_duplicates()

    # Format columns order
    col_sex_age = sorted(x for x in df_contact_sexage.columns if x.startswith("M") | x.startswith("W"))
    col_to_use = ['Type'] + col_sex_age + ['Total']
    df_contact_sexage = df_contact_sexage[col_to_use]

    return df_contact_sexage


def postprocessing_standard_tabcontacts_df_contact_sexage_trp_30eq(path_tables, label_object, campaign_par):
    # Read table
    df_contact_sexage = _extract_df_from_json_file(path_tables, 'impacts_by_sex_age')
    df_universe_by_sexage = _extract_df_from_json_file(path_tables, 'universe_by_sex_age')
    df_universe_by_sexage = df_universe_by_sexage[df_universe_by_sexage['date'] == df_universe_by_sexage['date'].min()]

    col_to_group = ['broadcaster', 'device_type', 'ad_type', 'sex', 'age_break']

    if df_contact_sexage.empty:
        df_contact_sexage[col_to_group + ['num_30sec_eq_impacts']] = None

    # Group data
    df_contact_sexage = df_contact_sexage.groupby(col_to_group, as_index=False)['num_30sec_eq_impacts'].sum()

    # Compute TRPs
    df_contact_sexage = df_contact_sexage.merge(df_universe_by_sexage.drop(columns=['date']), on=['sex', 'age_break'])
    df_contact_sexage['num_trp'] = df_contact_sexage['num_30sec_eq_impacts'] / df_contact_sexage['universe'] * 100
    df_contact_sexage = df_contact_sexage.drop(columns=['num_30sec_eq_impacts', 'universe'])

    # Scaffolding
    df_scaffolding = _scaffolding_contacts(col_to_group, label_object)

    df_contact_sexage = df_scaffolding.merge(df_contact_sexage, how='left', on=col_to_group)
    df_contact_sexage['num_trp'] = df_contact_sexage['num_trp'].fillna(0)

    # Pivot Table
    df_contact_sexage = df_contact_sexage.pivot(columns=['sex', 'age_break'],
                                                index=['broadcaster', 'device_type', 'ad_type'],
                                                values='num_trp').fillna(0)

    df_contact_sexage['_total'] = df_contact_sexage.sum(axis=1)
    df_contact_sexage = df_contact_sexage.reset_index()

    # Add Custom labels
    _custom_labels(df_contact_sexage, label_object)

    # Rename columns
    df_contact_sexage = df_contact_sexage.rename(columns=label_object['replace']['sex'])
    df_contact_sexage = df_contact_sexage.rename(columns=label_object['replace']['age_break'])

    # Flat dataframe
    concat_index_columns = list(map(lambda x: ''.join(x), df_contact_sexage.columns.values))
    df_contact_sexage.columns = concat_index_columns

    # Format rows order
    df_contact_sexage = _format_rows(df_contact_sexage, label_object)
    df_contact_sexage = df_contact_sexage.drop_duplicates()

    # Format columns order
    col_sex_age = sorted(x for x in df_contact_sexage.columns if x.startswith("M") | x.startswith("W"))
    col_to_use = ['Type'] + col_sex_age + ['Total']
    df_contact_sexage = df_contact_sexage[col_to_use]

    return df_contact_sexage


def postprocessing_standard_tabcontacts_df_contact_target_abs_raw(path_tables, label_object, campaign_par):
    """

    :param str path_tables: path of the directory which the json files are stored
    :param label_object:
    :param campaign_par:
    :return:
    """

    # Read campaign data
    target_list = campaign_par['target_name']

    # Read table
    df_contact_target = _extract_df_from_json_file(path_tables, 'impacts_in_target')

    col_to_group = ['broadcaster', 'device_type', 'ad_type', 'target_name']

    if df_contact_target.empty:
        df_contact_target[col_to_group + ['num_impacts']] = None

    # Group data
    df_contact_target = df_contact_target.groupby(col_to_group, as_index=False)['num_impacts'].sum()

    # Scaffolding
    df_scaffolding = _scaffolding_contacts(col_to_group, label_object, target_name=target_list)

    df_contact_target = df_scaffolding.merge(df_contact_target, how='left', on=col_to_group)
    df_contact_target['num_impacts'] = df_contact_target['num_impacts'].fillna(0)

    # Pivot Table
    df_contact_target = df_contact_target.pivot(columns=['target_name'],
                                                index=['broadcaster', 'device_type', 'ad_type'],
                                                values='num_impacts').fillna(0)

    df_contact_target['_total'] = df_contact_target.sum(axis=1)
    df_contact_target = df_contact_target / 1000

    df_contact_target = df_contact_target.reset_index()

    # Add Custom labels
    _custom_labels(df_contact_target, label_object)

    # Flat dataframe
    concat_index_columns = list(map(lambda x: ''.join(x), df_contact_target.columns.values))
    df_contact_target.columns = concat_index_columns

    # Format rows order
    df_contact_target = _format_rows(df_contact_target, label_object)

    # Remove column 'Total'
    df_contact_target = df_contact_target.drop(columns=['Total'])
    # Remove column 'A3+' if its sum is equal to 0
    if ('A3+' in df_contact_target):
        if df_contact_target['A3+'].sum() == 0:
            df_contact_target = df_contact_target.drop('A3+', axis=1)
            df_contact_target = df_contact_target.drop_duplicates()

    # Format columns order
    col_target = [x for x in df_contact_target.columns if not x.startswith("Type")]
    col_to_use = ['Type'] + col_target
    df_contact_target = df_contact_target[col_to_use]

    return df_contact_target


def postprocessing_standard_tabcontacts_df_contact_target_abs_30eq(path_tables, label_object, campaign_par):
    # Read campaign data
    target_list = campaign_par['target_name']

    # Read table
    df_contact_target = _extract_df_from_json_file(path_tables, 'impacts_in_target')

    col_to_group = ['broadcaster', 'device_type', 'ad_type', 'target_name']

    if df_contact_target.empty:
        df_contact_target[col_to_group + ['num_30sec_eq_impacts']] = None

    # Group data
    df_contact_target = df_contact_target.groupby(col_to_group, as_index=False)['num_30sec_eq_impacts'].sum()

    # Scaffolding
    df_scaffolding = _scaffolding_contacts(col_to_group, label_object, target_name=target_list)

    df_contact_target = df_scaffolding.merge(df_contact_target, how='left', on=col_to_group)
    df_contact_target['num_30sec_eq_impacts'] = df_contact_target['num_30sec_eq_impacts'].fillna(0)

    # Pivot Table
    df_contact_target = df_contact_target.pivot(columns=['target_name'],
                                                index=['broadcaster', 'device_type', 'ad_type'],
                                                values='num_30sec_eq_impacts').fillna(0)

    df_contact_target['_total'] = df_contact_target.sum(axis=1)
    df_contact_target = df_contact_target / 1000

    df_contact_target = df_contact_target.reset_index()

    # Add Custom labels
    _custom_labels(df_contact_target, label_object)

    # Flat dataframe
    concat_index_columns = list(map(lambda x: ''.join(x), df_contact_target.columns.values))
    df_contact_target.columns = concat_index_columns

    # Format rows order
    df_contact_target = _format_rows(df_contact_target, label_object)

    # Remove column 'Total'
    df_contact_target = df_contact_target.drop(columns=['Total'])
    # Remove column 'A3+' if its sum is equal to 0
    if ('A3+' in df_contact_target):
        if df_contact_target['A3+'].sum() == 0:
            df_contact_target = df_contact_target.drop('A3+', axis=1)
            df_contact_target = df_contact_target.drop_duplicates()

    # Format columns order
    col_target = [x for x in df_contact_target.columns if not x.startswith("Type")]
    col_to_use = ['Type'] + col_target
    df_contact_target = df_contact_target[col_to_use]

    return df_contact_target


def postprocessing_standard_tabcontacts_df_contact_target_trp_raw(path_tables, label_object, campaign_par):
    """

    :param str path_tables: path of the directory which the json files are stored
    :param label_object:
    :param campaign_par:
    :return:
    """

    # Read campaign data
    target_list = campaign_par['target_name']

    # Read table
    df_contact_target = _extract_df_from_json_file(path_tables, 'impacts_in_target')
    df_universe_by_target = _extract_df_from_json_file(path_tables, 'target_universe')
    df_universe_by_target = df_universe_by_target[df_universe_by_target['date'] == df_universe_by_target['date'].min()]

    col_to_group = ['broadcaster', 'device_type', 'ad_type', 'target_name']

    if df_contact_target.empty:
        df_contact_target[col_to_group + ['num_impacts']] = None

    # Group data
    df_contact_target = df_contact_target.groupby(col_to_group, as_index=False)['num_impacts'].sum()

    df_contact_target = df_contact_target.merge(df_universe_by_target.drop(columns=['date']), on=['target_name'])
    df_contact_target['num_trp'] = df_contact_target['num_impacts'] / df_contact_target['target_universe'] * 100
    df_contact_target = df_contact_target.drop(columns=['num_impacts', 'target_universe'])

    # Scaffolding
    df_scaffolding = _scaffolding_contacts(col_to_group, label_object, target_name=target_list)

    df_contact_target = df_scaffolding.merge(df_contact_target, how='left', on=col_to_group)
    df_contact_target['num_trp'] = df_contact_target['num_trp'].fillna(0)

    # Pivot Table
    df_contact_target = df_contact_target.pivot(columns=['target_name'],
                                                index=['broadcaster', 'device_type', 'ad_type'],
                                                values='num_trp').fillna(0)

    df_contact_target['_total'] = df_contact_target.sum(axis=1)
    df_contact_target = df_contact_target.reset_index()

    # Add Custom labels
    _custom_labels(df_contact_target, label_object)

    # Flat dataframe
    concat_index_columns = list(map(lambda x: ''.join(x), df_contact_target.columns.values))
    df_contact_target.columns = concat_index_columns

    # Format rows order
    df_contact_target = _format_rows(df_contact_target, label_object)

    # Remove column 'Total'
    df_contact_target = df_contact_target.drop(columns=['Total'])

    # Remove column 'A3+' if its sum is equal to 0 and drop duplicates
    if ('A3+' in df_contact_target):
        if df_contact_target['A3+'].sum() == 0:
            df_contact_target = df_contact_target.drop('A3+', axis=1)
            df_contact_target = df_contact_target.drop_duplicates()

    # Format columns order
    col_target = [x for x in df_contact_target.columns if not x.startswith("Type")]
    col_to_use = ['Type'] + col_target
    df_contact_target = df_contact_target[col_to_use]

    return df_contact_target


def postprocessing_standard_tabcontacts_df_contact_target_trp_30eq(path_tables, label_object, campaign_par):
    # Read campaign data
    target_list = campaign_par['target_name']

    # Read table
    df_contact_target = _extract_df_from_json_file(path_tables, 'impacts_in_target')
    df_universe_by_target = _extract_df_from_json_file(path_tables, 'target_universe')
    df_universe_by_target = df_universe_by_target[df_universe_by_target['date'] == df_universe_by_target['date'].min()]

    col_to_group = ['broadcaster', 'device_type', 'ad_type', 'target_name']

    if df_contact_target.empty:
        df_contact_target[col_to_group + ['num_30sec_eq_impacts']] = None

    # Group data
    df_contact_target = df_contact_target.groupby(col_to_group, as_index=False)['num_30sec_eq_impacts'].sum()

    df_contact_target = df_contact_target.merge(df_universe_by_target.drop(columns=['date']), on=['target_name'])
    df_contact_target['num_trp'] = df_contact_target['num_30sec_eq_impacts'] / df_contact_target[
        'target_universe'] * 100
    df_contact_target = df_contact_target.drop(columns=['num_30sec_eq_impacts', 'target_universe'])

    # Scaffolding
    df_scaffolding = _scaffolding_contacts(col_to_group, label_object, target_name=target_list)

    df_contact_target = df_scaffolding.merge(df_contact_target, how='left', on=col_to_group)
    df_contact_target['num_trp'] = df_contact_target['num_trp'].fillna(0)

    # Pivot Table
    df_contact_target = df_contact_target.pivot(columns=['target_name'],
                                                index=['broadcaster', 'device_type', 'ad_type'],
                                                values='num_trp').fillna(0)

    df_contact_target['_total'] = df_contact_target.sum(axis=1)
    df_contact_target = df_contact_target.reset_index()

    # Add Custom labels
    _custom_labels(df_contact_target, label_object)

    # Flat dataframe
    concat_index_columns = list(map(lambda x: ''.join(x), df_contact_target.columns.values))
    df_contact_target.columns = concat_index_columns

    # Format rows order
    df_contact_target = _format_rows(df_contact_target, label_object)

    # Remove column 'Total'
    df_contact_target = df_contact_target.drop(columns=['Total'])
    # Remove column 'A3+' if its sum is equal to 0
    if ('A3+' in df_contact_target):
        if df_contact_target['A3+'].sum() == 0:
            df_contact_target = df_contact_target.drop('A3+', axis=1)
            df_contact_target = df_contact_target.drop_duplicates()

    # Format columns order
    col_target = [x for x in df_contact_target.columns if not x.startswith("Type")]
    col_to_use = ['Type'] + col_target
    df_contact_target = df_contact_target[col_to_use]

    return df_contact_target


def postprocessing_standard_tabcontactsbu_df_contactcum_target_abs(path_tables, label_object, campaign_par):
    """

    :param str path_tables: path of the directory which the json files are stored
    :param label_object:
    :param campaign_par:
    :return:
    """

    # Read campaign data
    target_list = campaign_par['target_name']
    df_date_range = campaign_par['df_date_range']

    # Read table
    df_contact_target_bu = _extract_df_from_json_file(path_tables, 'impacts_in_target')

    col_to_group = ['date', 'broadcaster', 'device_type', 'ad_type', 'target_name']

    if df_contact_target_bu.empty:
        df_contact_target_bu[col_to_group + ['num_impacts']] = None

    # Group data
    df_contact_target_bu = df_contact_target_bu.groupby(col_to_group, as_index=False)['num_impacts'].sum()

    # Scaffolding
    df_scaffolding = _scaffolding_contacts(col_to_group, label_object, target_name=target_list,
                                           df_date_range=df_date_range)

    df_contact_target_bu = df_scaffolding.merge(df_contact_target_bu, how='left', on=col_to_group)
    df_contact_target_bu['num_impacts'] = df_contact_target_bu['num_impacts'].fillna(0)

    # Compute cumulative
    col_to_sort = ['date', 'broadcaster', 'device_type', 'ad_type', 'target_name']
    df_contact_target_bu = df_contact_target_bu.sort_values(col_to_sort)
    col_to_group = ['broadcaster', 'device_type', 'ad_type', 'target_name']
    df_contact_target_bu['num_impacts'] = df_contact_target_bu.groupby(col_to_group, as_index=False)[
        'num_impacts'].cumsum()

    # Pivot Table
    df_contact_target_bu = df_contact_target_bu.pivot(columns=['date'],
                                                      index=['target_name', 'broadcaster', 'device_type', 'ad_type'],
                                                      values='num_impacts').fillna(0)

    df_contact_target_bu = df_contact_target_bu / 1000

    df_contact_target_bu['_total'] = df_contact_target_bu.sum(axis=1)
    df_contact_target_bu = df_contact_target_bu.reset_index()

    # Add Custom labels
    _custom_labels(df_contact_target_bu, label_object)

    # Flat dataframe
    # concat_index_columns = list(map(lambda x: ''.join(x), df_contact_target_bu.columns.values))
    # df_contact_target_bu.columns = concat_index_columns

    # Format rows order
    df_contact_target_bu = _format_rows(df_contact_target_bu, label_object)
    df_contact_target_bu = df_contact_target_bu.drop_duplicates()

    # Format columns order
    df_contact_target_bu = df_contact_target_bu.rename(columns={"target_name": "Target name"})
    df_contact_target_bu = df_contact_target_bu.drop(columns=['Total'])
    col_date = sorted(x for x in df_contact_target_bu.columns if str(x) != "Type" and str(x) != "Target name")
    col_to_use = ['Type', 'Target name'] + col_date
    df_contact_target_bu = df_contact_target_bu[col_to_use]

    # Format date
    col_to_use = ['Type', 'Target name'] + [pd.to_datetime(x).strftime("%d-%m-%Y") for x in col_date]
    df_contact_target_bu.columns = col_to_use

    return df_contact_target_bu


def postprocessing_standard_tabcontactsbu_df_contactcum_target_trp(path_tables, label_object, campaign_par):
    """
    # DUBBIO! Usare sempre universo primo giorno?

    :param str path_tables: path of the directory which the json files are stored
    :param label_object:
    :param campaign_par:
    :return:
    """

    # Read campaign data
    target_list = campaign_par['target_name']
    df_date_range = campaign_par['df_date_range']

    # Read table
    df_contact_target_bu = _extract_df_from_json_file(path_tables, 'impacts_in_target')
    df_universe_by_target = _extract_df_from_json_file(path_tables, 'target_universe')
    df_universe_by_target = df_universe_by_target[df_universe_by_target['date'] == df_universe_by_target['date'].min()]

    col_to_group = ['date', 'broadcaster', 'device_type', 'ad_type', 'target_name']

    if df_contact_target_bu.empty:
        df_contact_target_bu[col_to_group + ['num_impacts']] = None

    # Group data
    df_contact_target_bu = df_contact_target_bu.groupby(col_to_group, as_index=False)['num_impacts'].sum()

    df_contact_target_bu = df_contact_target_bu.merge(df_universe_by_target.drop(columns=['date']), on=['target_name'])
    df_contact_target_bu['num_trp'] = df_contact_target_bu['num_impacts'] / df_contact_target_bu[
        'target_universe'] * 100
    df_contact_target_bu = df_contact_target_bu.drop(columns=['num_impacts', 'target_universe'])

    # Scaffolding
    df_scaffolding = _scaffolding_contacts(col_to_group, label_object, target_name=target_list,
                                           df_date_range=df_date_range)

    df_contact_target_bu = df_scaffolding.merge(df_contact_target_bu, how='left', on=col_to_group)
    df_contact_target_bu['num_trp'] = df_contact_target_bu['num_trp'].fillna(0)

    # Compute cumulative
    col_to_sort = ['date', 'broadcaster', 'device_type', 'ad_type', 'target_name']
    df_contact_target_bu = df_contact_target_bu.sort_values(col_to_sort)
    col_to_group = ['broadcaster', 'device_type', 'ad_type', 'target_name']
    df_contact_target_bu['num_trp'] = df_contact_target_bu.groupby(col_to_group, as_index=False)[
        'num_trp'].cumsum()

    # Pivot Table
    df_contact_target_bu = df_contact_target_bu.pivot(columns=['date'],
                                                      index=['target_name', 'broadcaster', 'device_type', 'ad_type'],
                                                      values='num_trp').fillna(0)

    df_contact_target_bu['_total'] = df_contact_target_bu.sum(axis=1)
    df_contact_target_bu = df_contact_target_bu.reset_index()

    # Add Custom labels
    _custom_labels(df_contact_target_bu, label_object)

    # Flat dataframe
    # concat_index_columns = list(map(lambda x: ''.join(x), df_contact_target_bu.columns.values))
    # df_contact_target_bu.columns = concat_index_columns

    # Format rows order
    df_contact_target_bu = _format_rows(df_contact_target_bu, label_object)
    df_contact_target_bu = df_contact_target_bu.drop_duplicates()

    # Format columns order
    df_contact_target_bu = df_contact_target_bu.rename(columns={"target_name": "Target name"})
    df_contact_target_bu = df_contact_target_bu.drop(columns=['Total'])
    col_date = sorted(x for x in df_contact_target_bu.columns if str(x) != "Type" and str(x) != "Target name")
    col_to_use = ['Type', 'Target name'] + col_date
    df_contact_target_bu = df_contact_target_bu[col_to_use]

    # df_contact_target_bu['_filter'] = df_contact_target_bu[col_date].sum(axis=1)
    # df_contact_target_bu = df_contact_target_bu[df_contact_target_bu['_filter'] > 0]
    # df_contact_target_bu = df_contact_target_bu.drop(columns=['_filter'])
    #

    # Format date
    col_to_use = ['Type', 'Target name'] + [pd.to_datetime(x).strftime("%d-%m-%Y") for x in col_date]
    df_contact_target_bu.columns = col_to_use

    return df_contact_target_bu


def postprocessing_standard_tabcontactsbu_df_contactdaily_target_abs(path_tables, label_object, campaign_par):
    """

    :param str path_tables: path of the directory which the json files are stored
    :param label_object:
    :param campaign_par:
    :return:
    """

    # Read campaign data
    target_list = campaign_par['target_name']
    df_date_range = campaign_par['df_date_range']

    # Read table
    df_contact_target_bu = _extract_df_from_json_file(path_tables, 'impacts_in_target')

    col_to_group = ['date', 'broadcaster', 'device_type', 'ad_type', 'target_name']

    if df_contact_target_bu.empty:
        df_contact_target_bu[col_to_group + ['num_impacts']] = None

    # Group data
    df_contact_target_bu = df_contact_target_bu.groupby(col_to_group, as_index=False)['num_impacts'].sum()

    # Scaffolding
    df_scaffolding = _scaffolding_contacts(col_to_group, label_object, target_name=target_list,
                                           df_date_range=df_date_range)

    df_contact_target_bu = df_scaffolding.merge(df_contact_target_bu, how='left', on=col_to_group)
    df_contact_target_bu['num_impacts'] = df_contact_target_bu['num_impacts'].fillna(0)

    # Pivot Table
    df_contact_target_bu = df_contact_target_bu.pivot(columns=['date'],
                                                      index=['target_name', 'broadcaster', 'device_type', 'ad_type'],
                                                      values='num_impacts').fillna(0)

    df_contact_target_bu = df_contact_target_bu / 1000

    df_contact_target_bu['_total'] = df_contact_target_bu.sum(axis=1)
    df_contact_target_bu = df_contact_target_bu.reset_index()

    # Add Custom labels
    _custom_labels(df_contact_target_bu, label_object)

    # Flat dataframe
    # concat_index_columns = list(map(lambda x: ''.join(x), df_contact_target_bu.columns.values))
    # df_contact_target_bu.columns = concat_index_columns

    # Format rows order
    df_contact_target_bu = _format_rows(df_contact_target_bu, label_object)
    df_contact_target_bu = df_contact_target_bu.drop_duplicates()

    # Format columns order
    df_contact_target_bu = df_contact_target_bu.rename(columns={"target_name": "Target name"})
    df_contact_target_bu = df_contact_target_bu.drop(columns=['Total'])
    col_date = sorted(x for x in df_contact_target_bu.columns if str(x) != "Type" and str(x) != "Target name")
    col_to_use = ['Type', 'Target name'] + col_date
    df_contact_target_bu = df_contact_target_bu[col_to_use]

    # Format date
    col_to_use = ['Type', 'Target name'] + [pd.to_datetime(x).strftime("%d-%m-%Y") for x in col_date]
    df_contact_target_bu.columns = col_to_use

    return df_contact_target_bu


def postprocessing_standard_tabcontactsbu_df_contactdaily_target_trp(path_tables, label_object, campaign_par):
    """

    :param str path_tables: path of the directory which the json files are stored
    :param label_object:
    :param campaign_par:
    :return:
    """

    # Read campaign data
    target_list = campaign_par['target_name']
    df_date_range = campaign_par['df_date_range']

    # Read table
    df_contact_target_bu = _extract_df_from_json_file(path_tables, 'impacts_in_target')
    df_universe_by_target = _extract_df_from_json_file(path_tables, 'target_universe')
    df_universe_by_target = df_universe_by_target[df_universe_by_target['date'] == df_universe_by_target['date'].min()]

    col_to_group = ['date', 'broadcaster', 'device_type', 'ad_type', 'target_name']

    if df_contact_target_bu.empty:
        df_contact_target_bu[col_to_group + ['num_impacts']] = None

    # Group data
    df_contact_target_bu = df_contact_target_bu.groupby(col_to_group, as_index=False)['num_impacts'].sum()

    df_contact_target_bu = df_contact_target_bu.merge(df_universe_by_target.drop(columns=['date']), on=['target_name'])
    df_contact_target_bu['num_trp'] = df_contact_target_bu['num_impacts'] / df_contact_target_bu[
        'target_universe'] * 100
    df_contact_target_bu = df_contact_target_bu.drop(columns=['num_impacts', 'target_universe'])

    # Scaffolding
    df_scaffolding = _scaffolding_contacts(col_to_group, label_object, target_name=target_list,
                                           df_date_range=df_date_range)

    df_contact_target_bu = df_scaffolding.merge(df_contact_target_bu, how='left', on=col_to_group)
    df_contact_target_bu['num_trp'] = df_contact_target_bu['num_trp'].fillna(0)

    # Pivot Table
    df_contact_target_bu = df_contact_target_bu.pivot(columns=['date'],
                                                      index=['target_name', 'broadcaster', 'device_type', 'ad_type'],
                                                      values='num_trp').fillna(0)

    df_contact_target_bu['_total'] = df_contact_target_bu.sum(axis=1)
    df_contact_target_bu = df_contact_target_bu.reset_index()

    # Add Custom labels
    _custom_labels(df_contact_target_bu, label_object)

    # Flat dataframe
    # concat_index_columns = list(map(lambda x: ''.join(x), df_contact_target_bu.columns.values))
    # df_contact_target_bu.columns = concat_index_columns

    # Format rows order
    df_contact_target_bu = _format_rows(df_contact_target_bu, label_object)
    df_contact_target_bu = df_contact_target_bu.drop_duplicates()

    # Format columns order
    df_contact_target_bu = df_contact_target_bu.rename(columns={"target_name": "Target name"})
    df_contact_target_bu = df_contact_target_bu.drop(columns=['Total'])
    col_date = sorted(x for x in df_contact_target_bu.columns if str(x) != "Type" and str(x) != "Target name")
    col_to_use = ['Type', 'Target name'] + col_date
    df_contact_target_bu = df_contact_target_bu[col_to_use]

    # Format date
    col_to_use = ['Type', 'Target name'] + [pd.to_datetime(x).strftime("%d-%m-%Y") for x in col_date]
    df_contact_target_bu.columns = col_to_use

    return df_contact_target_bu


def postprocessing_standard_tabr1_df_reach_target_abs(path_tables, label_object, campaign_par):
    """
    Usare universo target primo giorno?
    :param path_tables:
    :param label_object:
    :param campaign_par:
    :return:
    """
    # Read campaign data
    target_list = campaign_par['target_name']
    df_period_range = campaign_par['df_period_range']
    max_freq = campaign_par["max_freq"]

    # Read table
    df_reach_target = _extract_df_from_json_file(path_tables, 'r1plus_in_target_buildup')
    df_reach_target = df_reach_target[df_reach_target["end_date"] == df_reach_target["end_date"].max()]
    df_universe_by_target = _extract_df_from_json_file(path_tables, 'target_universe')
    df_universe_by_target = df_universe_by_target[df_universe_by_target['date'] == df_universe_by_target['date'].min()]
    df_reach_target = df_reach_target.merge(df_universe_by_target, on='target_name')
    df_reach_target['reach'] = df_reach_target['reach'] * df_reach_target['target_universe'] / 1000
    df_reach_target = df_reach_target.drop(columns=['target_universe', 'date'])

    # Scaffolding
    col_to_group = ['target_name', 'broadcaster', 'ad_type']
    df_scaffolding = _scaffolding_rf(col_to_group, label_object, target_name=target_list)
    df_reach_target = df_scaffolding.merge(df_reach_target, how='left', on=col_to_group)
    df_reach_target['reach'] = df_reach_target['reach'].fillna(0)

    # Order columns
    df_reach_target = _sort_labels(df_reach_target, label_object, "r1plus")

    if df_reach_target['reach'].sum():
        # Filter only rows whose values is not 0
        df_reach_target = df_reach_target[df_reach_target['reach'] > 0]
    else:
        # Remove rows whose date values are NA
        df_reach_target = df_reach_target.dropna(subset=['start_date', 'end_date'])

    # Pivot Table
    df_reach_target = df_reach_target.pivot(index=['target_name'],
                                            columns=['broadcaster', 'ad_type'],
                                            values='reach').fillna(0)

    df_reach_target = df_reach_target.reset_index()

    # Rename columns
    df_reach_target = df_reach_target.rename(columns=label_object['replace']['broadcaster'], level='broadcaster')
    df_reach_target = df_reach_target.rename(columns=label_object['replace']['ad_type'], level='ad_type')
    df_reach_target = df_reach_target.rename(columns={'target_name': 'Target name'})

    # Flat dataframe
    concat_index_columns = list(map(lambda x: (' '.join(x)).strip(), df_reach_target.columns.values))
    df_reach_target.columns = concat_index_columns

    # Format rows order
    df_reach_target = df_reach_target.sort_values(by='Target name')

    return df_reach_target


def postprocessing_standard_tabr1_df_reach_target_perc(path_tables, label_object, campaign_par):
    """
    Usare universo target primo giorno?
    :param path_tables:
    :param label_object:
    :param campaign_par:
    :return:
    """
    # Read campaign data
    target_list = campaign_par['target_name']
    df_period_range = campaign_par['df_period_range']
    max_freq = campaign_par["max_freq"]

    # Read table
    df_reach_target = _extract_df_from_json_file(path_tables, 'r1plus_in_target_buildup')
    df_reach_target = df_reach_target[df_reach_target["end_date"] == df_reach_target["end_date"].max()]

    df_reach_target['reach'] = df_reach_target['reach'] * 100

    # Scaffolding
    col_to_group = ['target_name', 'broadcaster', 'ad_type']
    df_scaffolding = _scaffolding_rf(col_to_group, label_object, target_name=target_list)
    df_reach_target = df_scaffolding.merge(df_reach_target, how='left', on=col_to_group)
    df_reach_target['reach'] = df_reach_target['reach'].fillna(0)

    # Order columns
    df_reach_target = _sort_labels(df_reach_target, label_object, "r1plus")

    if df_reach_target['reach'].sum():
        # Filter only rows whose values is not 0
        df_reach_target = df_reach_target[df_reach_target['reach'] > 0]
    else:
        # Remove rows whose date values are NA
        df_reach_target = df_reach_target.dropna(subset=['start_date', 'end_date'])

    # Pivot Table
    df_reach_target = df_reach_target.pivot(index=['target_name'],
                                            columns=['broadcaster', 'ad_type'],
                                            values='reach').fillna(0)

    df_reach_target = df_reach_target.reset_index()

    # Rename columns
    df_reach_target = df_reach_target.rename(columns=label_object['replace']['broadcaster'], level='broadcaster')
    df_reach_target = df_reach_target.rename(columns=label_object['replace']['ad_type'], level='ad_type')
    df_reach_target = df_reach_target.rename(columns={'target_name': 'Target name'})

    # Flat dataframe
    concat_index_columns = list(map(lambda x: (' '.join(x)).strip(), df_reach_target.columns.values))
    df_reach_target.columns = concat_index_columns

    # Format rows order
    df_reach_target = df_reach_target.sort_values(by='Target name')

    return df_reach_target


def postprocessing_standard_tabrf_df_reach_target_abs(path_tables, label_object, campaign_par):
    """
    Usare universo target primo giorno?
    :param path_tables:
    :param label_object:
    :param campaign_par:
    :return:
    """
    # Read campaign data
    target_list = campaign_par['target_name']
    max_freq = campaign_par["max_freq"]

    # Read table
    df_reach_target = _extract_df_from_json_file(path_tables, 'rf_in_target_overall')
    df_universe_by_target = _extract_df_from_json_file(path_tables, 'target_universe')
    df_universe_by_target = df_universe_by_target[df_universe_by_target['date'] == df_universe_by_target['date'].min()]

    # Calculate absolute reach by target
    df_reach_target = df_reach_target.merge(df_universe_by_target, on='target_name')
    df_reach_target['reach'] = df_reach_target['reach'] * df_reach_target['target_universe']
    df_reach_target = df_reach_target.drop(columns=['target_universe', 'date'])

    # Group data
    df_reach_target = df_reach_target[df_reach_target["frequency"] > 0]
    df_reach_target = df_reach_target.drop(columns=["start_date", "end_date"])

    # Scaffolding
    col_to_join = ['target_name', 'broadcaster', 'ad_type', 'frequency']
    df_scaffolding = _scaffolding_rf(col_to_join, label_object, target_name=target_list, max_freq=max_freq)
    df_scaffolding = df_scaffolding[df_scaffolding["frequency"] > 0]
    df_reach_target = df_scaffolding.merge(df_reach_target, how='left', on=col_to_join)
    df_reach_target['reach'] = df_reach_target['reach'].fillna(0)

    # Cumsum
    col_to_sort = ['target_name', 'broadcaster', 'ad_type', 'frequency']
    df_reach_target = df_reach_target.sort_values(col_to_sort, ascending=[True, True, True, False])
    col_to_group = ['target_name', 'broadcaster', 'ad_type']
    df_reach_target['reach'] = df_reach_target.groupby(col_to_group, as_index=False)['reach'].cumsum()

    # Pivot Table
    df_reach_target['frequency'] = df_reach_target['frequency'].astype(str)
    df_reach_target = df_reach_target.pivot(index=['target_name', 'broadcaster', 'ad_type'],
                                            columns=['frequency'],
                                            values='reach').fillna(0)

    df_reach_target['_total'] = df_reach_target.sum(axis=1)
    df_reach_target = df_reach_target.reset_index()

    # Add Custom labels
    _custom_labels(df_reach_target, label_object)

    df_reach_target = _format_rows(df_reach_target, label_object, tab_type='rf')

    # Rename columns
    map_col = [(str(x), f"{str(x)}+") for x in range(1, max_freq + 1)]
    rename_col = {x[0]: x[1] for x in map_col}
    sort_col = [x[1] for x in map_col]

    df_reach_target = df_reach_target.rename(columns={'target_name': 'Target name'})
    df_reach_target = df_reach_target.rename(columns=rename_col)

    # Add the average frequency as a column
    # Absolute contacts
    df_contacts_abs = _compute_contacts_abs_target(path_tables, label_object, campaign_par)

    # Join reach 1+ with absolute contacts
    df_merge_rf = df_reach_target[['Type', 'Target name', '1+']].merge(df_contacts_abs, how='left', on=['Type', 'Target name'])

    # Calculate the average frequency
    df_merge_rf['Average freq'] = (df_merge_rf["Contacts"].where(df_merge_rf["1+"] != 0, 0) /
                                   df_merge_rf["1+"].where(df_merge_rf["1+"] != 0, 1))

    # Join reach 1+ with average frequency
    df_reach_target = df_reach_target.merge(df_merge_rf[['Target name', 'Type', 'Average freq']], how='left', on=['Target name', 'Type'])
    col_to_exclude = ['Type', 'Target name', 'Average freq']
    df_reach_target.loc[:, ~df_reach_target.columns.isin(col_to_exclude)] = df_reach_target.loc[:,
                                                                                                ~df_reach_target
                                                                                                .columns
                                                                                                .isin(col_to_exclude)] / 1000

    # Sort columns
    col_order = ["Target name", "Type", "Average freq"] + sort_col
    df_reach_target = df_reach_target[col_order]

    return df_reach_target


def postprocessing_standard_tabrf_df_reach_target_perc(path_tables, label_object, campaign_par):
    """
    Usare universo target primo giorno?
    :param path_tables:
    :param label_object:
    :param campaign_par:
    :return:
    """
    # Read campaign data
    target_list = campaign_par['target_name']
    max_freq = campaign_par["max_freq"]

    # Read table
    df_reach_target = _extract_df_from_json_file(path_tables, 'rf_in_target_overall')

    # Group data
    df_reach_target = df_reach_target[df_reach_target["frequency"] > 0]
    df_reach_target = df_reach_target.drop(columns=["start_date", "end_date"])

    # Scaffolding
    col_to_join = ['target_name', 'broadcaster', 'ad_type', 'frequency']
    df_scaffolding = _scaffolding_rf(col_to_join, label_object, target_name=target_list, max_freq=max_freq)
    df_scaffolding = df_scaffolding[df_scaffolding["frequency"] > 0]
    df_reach_target = df_scaffolding.merge(df_reach_target, how='left', on=col_to_join)
    df_reach_target['reach'] = df_reach_target['reach'].fillna(0)

    # Cumsum
    col_to_sort = ['target_name', 'broadcaster', 'ad_type', 'frequency']
    df_reach_target = df_reach_target.sort_values(col_to_sort, ascending=[True, True, True, False])
    col_to_group = ['target_name', 'broadcaster', 'ad_type']
    df_reach_target['reach'] = df_reach_target.groupby(col_to_group, as_index=False)['reach'].cumsum()

    # Pivot Table
    df_reach_target['frequency'] = df_reach_target['frequency'].astype(str)
    df_reach_target = df_reach_target.pivot(index=['target_name', 'broadcaster', 'ad_type'],
                                            columns=['frequency'],
                                            values='reach').fillna(0)

    df_reach_target['_total'] = df_reach_target.sum(axis=1)
    df_reach_target = df_reach_target.reset_index()

    # Add Custom labels
    _custom_labels(df_reach_target, label_object)
    # Join Custom labels under the new "Type" column
    df_reach_target = _format_rows(df_reach_target, label_object, tab_type='rf')

    # Rename columns
    map_col = [(str(x), f"{str(x)}+") for x in range(1, max_freq + 1)]
    rename_col = {x[0]: x[1] for x in map_col}
    sort_col = [x[1] for x in map_col]

    df_reach_target = df_reach_target.rename(columns={'target_name': 'Target name'})
    df_reach_target = df_reach_target.rename(columns=rename_col)

    # Add the average frequency as a column
    # Absolute contacts
    df_contacts_abs = _compute_contacts_abs_target(path_tables, label_object, campaign_par)

    # Universe by target
    df_universe = _extract_df_from_json_file(path_tables, 'target_universe')
    df_universe = df_universe[['target_name', 'target_universe']].drop_duplicates()
    df_universe = df_universe.rename(columns={'target_name': 'Target name',
                                              'target_universe': 'Target universe'})

    # Join absolute contacts with universe by target
    df_merge_contacts = df_contacts_abs.merge(df_universe, how='inner', on='Target name')

    # Join reach 1+ with absolute contacts and universe by target
    df_merge_rf = df_reach_target[['Target name', 'Type', '1+']].merge(df_merge_contacts, how='left', on=['Type', 'Target name'])

    # Calculate the average frequency
    denominator = (df_merge_rf['1+'] * df_merge_rf['Target universe'])
    df_merge_rf['Average freq'] = df_merge_rf['Contacts'].where(denominator != 0, 0) / denominator.where(
        denominator != 0, 1)

    # Add the average frequency to the reach dataframe
    df_reach_target = df_reach_target.merge(df_merge_rf[['Type', 'Target name', 'Average freq']], how='left', on=['Type', 'Target name'])
    col_to_exclude = ['Type', 'Target name', 'Average freq']
    df_reach_target.loc[:, ~df_reach_target.columns.isin(col_to_exclude)] = df_reach_target.loc[:,
                                                                                                ~df_reach_target
                                                                                                .columns
                                                                                                .isin(col_to_exclude)] * 100

    # Sort columns
    col_order = ["Target name", "Type", 'Average freq'] + sort_col
    df_reach_target = df_reach_target[col_order]

    return df_reach_target


def postprocessing_standard_tabr1bu_df_reach_target_abs(path_tables, label_object, campaign_par):
    """
    Usare universo target primo giorno?
    :param path_tables:
    :param label_object:
    :param campaign_par:
    :return:
    """
    # Read campaign data
    target_list = campaign_par['target_name']
    df_period_range = campaign_par['df_period_range']

    # Read table
    df_reach_target = _extract_df_from_json_file(path_tables, 'r1plus_in_target_buildup')

    df_universe_by_target = _extract_df_from_json_file(path_tables, 'target_universe')
    df_universe_by_target = df_universe_by_target[df_universe_by_target['date'] == df_universe_by_target['date'].min()]
    df_reach_target = df_reach_target.merge(df_universe_by_target, on='target_name')
    df_reach_target['reach'] = df_reach_target['reach'] * df_reach_target['target_universe'] / 1000
    df_reach_target = df_reach_target.drop(columns=['target_universe', 'date'])

    # Scaffolding
    col_to_group = ['target_name', 'end_date', 'broadcaster', 'ad_type']
    df_scaffolding = _scaffolding_rf(col_to_group, label_object, target_name=target_list,
                                     df_period_range=df_period_range)
    df_reach_target = df_scaffolding.merge(df_reach_target, how='left', on=col_to_group)
    df_reach_target['reach'] = df_reach_target['reach'].fillna(0)

    # Pivot Table
    df_reach_target = df_reach_target.pivot(index=['target_name', 'broadcaster', 'ad_type'],
                                            columns=['end_date'],
                                            values='reach').fillna(0)

    df_reach_target['_total'] = df_reach_target.sum(axis=1)
    df_reach_target = df_reach_target.reset_index()

    # Add Custom labels
    _custom_labels(df_reach_target, label_object)

    # Format rows order
    df_reach_target = _format_rows(df_reach_target, label_object, tab_type='rf')

    # Format columns order
    df_reach_target = df_reach_target.rename(columns={"target_name": "Target name"})
    df_reach_target = df_reach_target.drop(columns=['Total'])
    col_date = sorted(x for x in df_reach_target.columns if x != "Type" and x != "Target name")
    col_to_use = ['Type', 'Target name'] + col_date
    df_reach_target = df_reach_target[col_to_use]

    # Format date
    col_to_use = ['Type', 'Target name'] + [pd.to_datetime(x).strftime("%d-%m-%Y") for x in col_date]
    df_reach_target.columns = col_to_use

    return df_reach_target


def postprocessing_standard_tabr1bu_df_reach_target_perc(path_tables, label_object, campaign_par):
    """
    Usare universo target primo giorno?
    :param path_tables:
    :param label_object:
    :param campaign_par:
    :return:
    """
    # Read campaign data
    target_list = campaign_par['target_name']
    df_period_range = campaign_par['df_period_range']

    # Read table
    df_reach_target = _extract_df_from_json_file(path_tables, 'r1plus_in_target_buildup')
    df_universe_by_target = _extract_df_from_json_file(path_tables, 'target_universe')
    df_universe_by_target = df_universe_by_target[df_universe_by_target['date'] == df_universe_by_target['date'].min()]
    df_reach_target = df_reach_target.merge(df_universe_by_target, on='target_name')
    df_reach_target['reach'] = df_reach_target['reach'] * 100
    df_reach_target = df_reach_target.drop(columns=['target_universe', 'date'])

    # Scaffolding
    col_to_group = ['target_name', 'end_date', 'broadcaster', 'ad_type']
    df_scaffolding = _scaffolding_rf(col_to_group, label_object, target_name=target_list,
                                     df_period_range=df_period_range)
    df_reach_target = df_scaffolding.merge(df_reach_target, how='left', on=col_to_group)
    df_reach_target['reach'] = df_reach_target['reach'].fillna(0)

    # Pivot Table
    df_reach_target = df_reach_target.pivot(index=['target_name', 'broadcaster', 'ad_type'],
                                            columns=['end_date'],
                                            values='reach').fillna(0)

    df_reach_target['_total'] = df_reach_target.sum(axis=1)
    df_reach_target = df_reach_target.reset_index()

    # Add Custom labels
    _custom_labels(df_reach_target, label_object)

    # Format rows order
    df_reach_target = _format_rows(df_reach_target, label_object, tab_type='rf')

    # Format columns order
    df_reach_target = df_reach_target.rename(columns={"target_name": "Target name"})
    df_reach_target = df_reach_target.drop(columns=['Total'])
    col_date = sorted(x for x in df_reach_target.columns if x != "Type" and x != "Target name")
    col_to_use = ['Type', 'Target name'] + col_date
    df_reach_target = df_reach_target[col_to_use]

    # Format date
    col_to_use = ['Type', 'Target name'] + [pd.to_datetime(x).strftime("%d-%m-%Y") for x in col_date]
    df_reach_target.columns = col_to_use

    return df_reach_target


def postprocessing_standard_tabsummary_df_contactreach_contact_target_abs_raw(path_tables, label_object, campaign_par):
    """

    :param str path_tables: path of the directory which the json files are stored
    :param label_object:
    :param campaign_par:
    :return:
    """

    # Read campaign data
    target_list = campaign_par['target_name']

    # Read table
    df_contact_target = _extract_df_from_json_file(path_tables, 'impacts_in_target')

    col_to_group = ['broadcaster', 'ad_type', 'target_name']

    if df_contact_target.empty:
        df_contact_target[col_to_group + ['num_impacts']] = None

    # Group data
    df_contact_target = df_contact_target.groupby(col_to_group, as_index=False)['num_impacts'].sum()

    # Compute Contacts Totals including Online video
    df_target_total_all_onlinevideo = df_contact_target.groupby(['target_name'], as_index=False)['num_impacts'].sum()
    df_target_total_all_onlinevideo['broadcaster'] = 'all'
    df_target_total_all_onlinevideo['ad_type'] = 'all_onlinevideo'

    # Compute Contacts Totals Linear and BVOD
    mask = df_contact_target['ad_type'].isin(['linear_static', 'dynamic'])
    df_target_total_all = df_contact_target[mask].groupby(['target_name'], as_index=False)['num_impacts'].sum()
    df_target_total_all['broadcaster'] = 'all'
    df_target_total_all['ad_type'] = 'all'

    # Compute Contacts Linear Total
    mask = df_contact_target['ad_type'] == 'linear_static'
    df_target_total_linear = df_contact_target[mask].groupby(['target_name'], as_index=False)['num_impacts'].sum()
    df_target_total_linear['broadcaster'] = 'all'
    df_target_total_linear['ad_type'] = 'linear_static'

    # Compute Contacts BVOD Total
    mask = df_contact_target['ad_type'] == 'dynamic'
    df_target_total_dynamic = df_contact_target[mask].groupby(['target_name'], as_index=False)['num_impacts'].sum()
    df_target_total_dynamic['broadcaster'] = 'all'
    df_target_total_dynamic['ad_type'] = 'dynamic'

    # Concat Totals
    df_contact_target_total = pd.concat(
        [df_contact_target, df_target_total_linear, df_target_total_dynamic, df_target_total_all,
         df_target_total_all_onlinevideo], ignore_index=True)

    # Scaffolding
    df_scaffolding = _scaffolding_rf(col_to_group, label_object, target_name=target_list)

    df_contact_target_total = df_scaffolding.merge(df_contact_target_total, how='left', on=col_to_group)
    df_contact_target_total['num_impacts'] = df_contact_target_total['num_impacts'].fillna(0)

    # Pivot Table
    # df_contact_target_total = df_contact_target_total.pivot(columns=['target_name'],
    #                                             index=['broadcaster', 'ad_type'],
    #                                             values='num_impacts').fillna(0)

    df_contact_target_total = df_contact_target_total.set_index(['target_name', 'broadcaster', 'ad_type'])

    df_contact_target_total['_total'] = df_contact_target_total.sum(axis=1)
    df_contact_target_total = df_contact_target_total.reset_index()

    # Add Custom labels
    _custom_labels(df_contact_target_total, label_object)

    # Flat dataframe
    concat_index_columns = list(map(lambda x: ''.join(x), df_contact_target_total.columns.values))
    df_contact_target_total.columns = concat_index_columns

    # Format rows order
    df_contact_target_total = _format_rows(df_contact_target_total, label_object, tab_type='summary')

    # Format columns order
    df_contact_target_total = df_contact_target_total.rename(
        columns={"target_name": "Target name", 'num_impacts': 'Contacts'})
    df_contact_target_total = df_contact_target_total.drop(columns=['Total'])
    col_target = [x for x in df_contact_target_total.columns if not x.startswith("Type")]
    col_to_use = ['Type'] + col_target
    df_contact_target_total = df_contact_target_total[col_to_use]

    return df_contact_target_total


def postprocessing_standard_tabsummary_df_contactreach_contact_target_perc_raw(path_tables, label_object, campaign_par):
    """

    :param str path_tables: path of the directory which the json files are stored
    :param label_object:
    :param campaign_par:
    :return:
    """

    # Read campaign data
    target_list = campaign_par['target_name']

    # Read table
    df_contact_target = _extract_df_from_json_file(path_tables, 'impacts_in_target')
    df_universe_by_target = _extract_df_from_json_file(path_tables, 'target_universe')
    df_universe_by_target = df_universe_by_target[df_universe_by_target['date'] == df_universe_by_target['date'].min()]

    col_to_group = ['broadcaster', 'ad_type', 'target_name']

    if df_contact_target.empty:
        df_contact_target[col_to_group + ['num_impacts']] = None

    # Group data
    df_contact_target = df_contact_target.groupby(col_to_group, as_index=False)['num_impacts'].sum()

    df_contact_target = df_contact_target.merge(df_universe_by_target.drop(columns=['date']), on=['target_name'])
    df_contact_target['num_trp'] = df_contact_target['num_impacts'] / df_contact_target['target_universe'] * 100
    df_contact_target = df_contact_target.drop(columns=['num_impacts', 'target_universe'])

    # Compute Contacts Totals including Online video
    df_target_total_all_onlinevideo = df_contact_target.groupby(['target_name'], as_index=False)['num_trp'].sum()
    df_target_total_all_onlinevideo['broadcaster'] = 'all'
    df_target_total_all_onlinevideo['ad_type'] = 'all_onlinevideo'

    # Compute Contacts Totals Linear and BVOD
    mask = df_contact_target['ad_type'].isin(['linear_static', 'dynamic'])
    df_target_total_all = df_contact_target[mask].groupby(['target_name'], as_index=False)['num_trp'].sum()
    df_target_total_all['broadcaster'] = 'all'
    df_target_total_all['ad_type'] = 'all'

    # Compute Contacts Linear Total
    mask = df_contact_target['ad_type'] == 'linear_static'
    df_target_total_linear = df_contact_target[mask].groupby(['target_name'], as_index=False)['num_trp'].sum()
    df_target_total_linear['broadcaster'] = 'all'
    df_target_total_linear['ad_type'] = 'linear_static'

    # Compute Contacts BVOD Total
    mask = df_contact_target['ad_type'] == 'dynamic'
    df_target_total_dynamic = df_contact_target[mask].groupby(['target_name'], as_index=False)['num_trp'].sum()
    df_target_total_dynamic['broadcaster'] = 'all'
    df_target_total_dynamic['ad_type'] = 'dynamic'

    # Concat Totals
    df_contact_target_total = pd.concat(
        [df_contact_target, df_target_total_linear, df_target_total_dynamic, df_target_total_all,
         df_target_total_all_onlinevideo], ignore_index=True)

    # Scaffolding
    df_scaffolding = _scaffolding_rf(col_to_group, label_object, target_name=target_list)

    df_contact_target_total = df_scaffolding.merge(df_contact_target_total, how='left', on=col_to_group)
    df_contact_target_total['num_trp'] = df_contact_target_total['num_trp'].fillna(0)

    # Pivot Table
    # df_contact_target_total = df_contact_target_total.pivot(columns=['target_name'],
    #                                             index=['broadcaster', 'ad_type'],
    #                                             values='num_trp').fillna(0)
    df_contact_target_total = df_contact_target_total.set_index(['target_name', 'broadcaster', 'ad_type'])
    df_contact_target_total = df_contact_target_total.round(1)

    df_contact_target_total['_total'] = df_contact_target_total.sum(axis=1)
    df_contact_target_total = df_contact_target_total.reset_index()

    # Add Custom labels
    _custom_labels(df_contact_target_total, label_object)

    # Flat dataframe
    concat_index_columns = list(map(lambda x: ''.join(x), df_contact_target_total.columns.values))
    df_contact_target_total.columns = concat_index_columns

    # Format rows order
    df_contact_target_total = _format_rows(df_contact_target_total, label_object, tab_type='summary')

    # Format columns order
    df_contact_target_total = df_contact_target_total.rename(columns={"target_name": "Target name", 'num_trp': 'TRPs'})
    df_contact_target_total = df_contact_target_total.drop(columns=['Total'])
    col_target = [x for x in df_contact_target_total.columns if not x.startswith("Type")]
    col_to_use = ['Type'] + col_target
    df_contact_target_total = df_contact_target_total[col_to_use]

    return df_contact_target_total


def postprocessing_standard_tabsummary_df_contactreach_reach_target_perc(path_tables, label_object, campaign_par):
    """
    Usare universo target primo giorno?
    :param path_tables:
    :param label_object:
    :param campaign_par:
    :return:
    """

    # Read campaign data
    target_list = campaign_par['target_name']
    max_freq = campaign_par["max_freq"]

    # Read table
    df_reach_target = _extract_df_from_json_file(path_tables, 'rf_in_target_overall')

    df_reach_target = df_reach_target.drop(columns=["start_date", "end_date"])

    # Scaffolding
    col_to_join = ['target_name', 'broadcaster', 'ad_type', 'frequency']
    df_scaffolding = _scaffolding_rf(col_to_join, label_object, target_name=target_list, max_freq=max_freq)
    df_scaffolding = df_scaffolding[df_scaffolding["frequency"] > 0]
    df_reach_target = df_scaffolding.merge(df_reach_target, how='left', on=col_to_join)
    df_reach_target['reach'] = df_reach_target['reach'].fillna(0)

    # Cumsum
    col_to_sort = ['target_name', 'broadcaster', 'ad_type', 'frequency']
    df_reach_target = df_reach_target.sort_values(col_to_sort, ascending=[True, True, True, False])
    col_to_group = ['target_name', 'broadcaster', 'ad_type']
    df_reach_target['reach'] = df_reach_target.groupby(col_to_group, as_index=False)['reach'].cumsum()

    # Pivot Table
    df_reach_target['frequency'] = df_reach_target['frequency'].astype(str)
    df_reach_target = df_reach_target.pivot(index=['target_name', 'broadcaster', 'ad_type'],
                                            columns=['frequency'],
                                            values='reach').fillna(0)
    df_reach_target['_total'] = df_reach_target.sum(axis=1)
    df_reach_target = df_reach_target.reset_index()

    # Add Custom labels
    _custom_labels(df_reach_target, label_object)

    df_reach_target = _format_rows(df_reach_target, label_object, tab_type='summary')

    # Rename columns
    map_col = [(str(x), f"{str(x)}+") for x in range(1, max_freq + 1)]
    rename_col = {x[0]: x[1] for x in map_col}
    sort_col = [x[1] for x in map_col if x[1] in ['1+', '2+', '3+']]

    df_reach_target = df_reach_target.rename(columns=rename_col)

    col_order = ["target_name", "Type"] + sort_col
    df_reach_target = df_reach_target[col_order]
    df_reach_target = df_reach_target.rename(columns={'target_name': 'Target name', '1+': 'Reach 1+ (%)',
                                                      '2+': 'Reach 2+ (%)', '3+': 'Reach 3+ (%)'})
    return df_reach_target


def postprocessing_standard_tabsummary_df_universe_target_abs(path_tables, label_object, campaign_par):
    # Read campaign data
    target_list = campaign_par['target_name']
    min_date = campaign_par['df_date_range']['date'].min()

    # Read table
    df_universe = _extract_df_from_json_file(path_tables, 'target_universe')

    df_universe = df_universe[df_universe['target_name'].isin(target_list) & (df_universe['date'] == min_date)]

    df_universe = df_universe.drop(columns=['date'])

    return df_universe


######################## TABLES & PLOTS ########################

def postprocessing_standard_tabcontacts_table_contact_sexage_abs_raw(path_tables, path_output_json, label_object,
                                                                     label_attribute_element,
                                                                     campaign_par, element_obj):
    df = postprocessing_standard_tabcontacts_df_contact_sexage_abs_raw(path_tables, label_object, campaign_par)
    df = _compute_total(df)

    # Round to the first decimal value
    df = df.round(1)

    p = _write_dict(df, path_output_json, label_attribute_element,
                    (element_obj['min_default_zoom'], element_obj['max_default_zoom']))
    return p


def postprocessing_standard_tabcontacts_plot_contact_sexage_abs_raw(path_tables, path_output_json, label_object,
                                                                    label_attribute_element,
                                                                    campaign_par, element_obj):
    df = postprocessing_standard_tabcontacts_df_contact_sexage_abs_raw(path_tables, label_object, campaign_par)
    df = df.drop(columns='Total')

    # Round to the first decimal value
    df = df.round(1)

    p = _write_dict(df, path_output_json, label_attribute_element,
                    (element_obj['min_default_zoom'], element_obj['max_default_zoom']))
    return p


def postprocessing_standard_tabcontacts_table_contact_sexage_trp_raw(path_tables, path_output_json, label_object,
                                                                     label_attribute_element,
                                                                     campaign_par, element_obj):
    df = postprocessing_standard_tabcontacts_df_contact_sexage_trp_raw(path_tables, label_object, campaign_par)

    # Add total A3+
    df_a3plus = _compute_contact_total_trp_raw(path_tables, label_object, campaign_par)

    df = df.drop(columns='Total')
    df = df.merge(df_a3plus, on='Type')
    df = _compute_total(df)

    # Round to the first decimal value
    df = df.round(1)

    p = _write_dict(df, path_output_json, label_attribute_element,
                    (element_obj['min_default_zoom'], element_obj['max_default_zoom']))
    return p


def postprocessing_standard_tabcontacts_plot_contact_sexage_trp_raw(path_tables, path_output_json, label_object,
                                                                    label_attribute_element,
                                                                    campaign_par, element_obj):
    df = postprocessing_standard_tabcontacts_df_contact_sexage_trp_raw(path_tables, label_object, campaign_par)
    df = df.drop(columns='Total')

    # Round to the first decimal value
    df = df.round(1)

    p = _write_dict(df, path_output_json, label_attribute_element,
                    (element_obj['min_default_zoom'], element_obj['max_default_zoom']))
    return p


def postprocessing_standard_tabcontacts_table_contact_sexage_abs_30eq(path_tables, path_output_json, label_object,
                                                                      label_attribute_element,
                                                                      campaign_par, element_obj):
    df = postprocessing_standard_tabcontacts_df_contact_sexage_abs_30eq(path_tables, label_object, campaign_par)
    df = _compute_total(df)

    # Round to the first decimal value
    df = df.round(1)

    p = _write_dict(df, path_output_json, label_attribute_element,
                    (element_obj['min_default_zoom'], element_obj['max_default_zoom']))
    return p


def postprocessing_standard_tabcontacts_table_contact_sexage_trp_30eq(path_tables, path_output_json, label_object,
                                                                      label_attribute_element,
                                                                      campaign_par, element_obj):
    df = postprocessing_standard_tabcontacts_df_contact_sexage_trp_30eq(path_tables, label_object, campaign_par)

    # Add total A3+
    df_a3plus = _compute_contact_total_trp_30eq(path_tables, label_object, campaign_par)

    df = df.drop(columns='Total')
    df = df.merge(df_a3plus, on='Type')
    df = _compute_total(df)

    # Round to the first decimal value
    df = df.round(1)

    p = _write_dict(df, path_output_json, label_attribute_element,
                    (element_obj['min_default_zoom'], element_obj['max_default_zoom']))
    return p


def postprocessing_standard_tabcontacts_plot_contact_sexage_abs_30eq(path_tables, path_output_json, label_object,
                                                                     label_attribute_element,
                                                                     campaign_par, element_obj):
    df = postprocessing_standard_tabcontacts_df_contact_sexage_abs_30eq(path_tables, label_object, campaign_par)
    df = df.drop(columns='Total')

    # Round to the first decimal value
    df = df.round(1)

    p = _write_dict(df, path_output_json, label_attribute_element,
                    (element_obj['min_default_zoom'], element_obj['max_default_zoom']))
    return p


def postprocessing_standard_tabcontacts_plot_contact_sexage_trp_30eq(path_tables, path_output_json, label_object,
                                                                     label_attribute_element,
                                                                     campaign_par, element_obj):
    df = postprocessing_standard_tabcontacts_df_contact_sexage_trp_30eq(path_tables, label_object, campaign_par)
    df = df.drop(columns='Total')

    # Round to the first decimal value
    df = df.round(1)

    p = _write_dict(df, path_output_json, label_attribute_element,
                    (element_obj['min_default_zoom'], element_obj['max_default_zoom']))
    return p


# Contancts tab
## Target
def postprocessing_standard_tabcontacts_table_contact_target_abs_raw(path_tables, path_output_json, label_object,
                                                                     label_attribute_element,
                                                                     campaign_par, element_obj):
    df = postprocessing_standard_tabcontacts_df_contact_target_abs_raw(path_tables, label_object, campaign_par)
    df = _compute_total(df)

    # Round to the first decimal value
    df = df.round(1)

    p = _write_dict(df, path_output_json, label_attribute_element,
                    (element_obj['min_default_zoom'], element_obj['max_default_zoom']))
    return p


def postprocessing_standard_tabcontacts_plot_contact_target_abs_raw(path_tables, path_output_json, label_object,
                                                                    label_attribute_element,
                                                                    campaign_par, element_obj):
    df = postprocessing_standard_tabcontacts_df_contact_target_abs_raw(path_tables, label_object, campaign_par)

    # Round to the first decimal value
    df = df.round(1)

    p = _write_dict(df, path_output_json, label_attribute_element,
                    (element_obj['min_default_zoom'], element_obj['max_default_zoom']))
    return p


def postprocessing_standard_tabcontacts_table_contact_target_trp_raw(path_tables, path_output_json, label_object,
                                                                     label_attribute_element,
                                                                     campaign_par, element_obj):
    df = postprocessing_standard_tabcontacts_df_contact_target_trp_raw(path_tables, label_object, campaign_par)
    df = _compute_total(df)

    # Round to the first decimal value
    df = df.round(1)

    p = _write_dict(df, path_output_json, label_attribute_element,
                    (element_obj['min_default_zoom'], element_obj['max_default_zoom']))
    return p


def postprocessing_standard_tabcontacts_plot_contact_target_trp_raw(path_tables, path_output_json, label_object,
                                                                    label_attribute_element,
                                                                    campaign_par, element_obj):
    df = postprocessing_standard_tabcontacts_df_contact_target_trp_raw(path_tables, label_object, campaign_par)

    # Round to the first decimal value
    df = df.round(1)

    p = _write_dict(df, path_output_json, label_attribute_element,
                    (element_obj['min_default_zoom'], element_obj['max_default_zoom']))
    return p


def postprocessing_standard_tabcontacts_table_contact_target_abs_30eq(path_tables, path_output_json, label_object,
                                                                      label_attribute_element,
                                                                      campaign_par, element_obj):
    df = postprocessing_standard_tabcontacts_df_contact_target_abs_30eq(path_tables, label_object, campaign_par)
    df = _compute_total(df)

    # Round to the first decimal value
    df = df.round(1)

    p = _write_dict(df, path_output_json, label_attribute_element,
                    (element_obj['min_default_zoom'], element_obj['max_default_zoom']))
    return p


def postprocessing_standard_tabcontacts_table_contact_target_trp_30eq(path_tables, path_output_json, label_object,
                                                                      label_attribute_element,
                                                                      campaign_par, element_obj):
    df = postprocessing_standard_tabcontacts_df_contact_target_trp_30eq(path_tables, label_object, campaign_par)
    df = _compute_total(df)

    # Round to the first decimal value
    df = df.round(1)

    p = _write_dict(df, path_output_json, label_attribute_element,
                    (element_obj['min_default_zoom'], element_obj['max_default_zoom']))
    return p


def postprocessing_standard_tabcontacts_plot_contact_target_abs_30eq(path_tables, path_output_json, label_object,
                                                                     label_attribute_element,
                                                                     campaign_par, element_obj):
    df = postprocessing_standard_tabcontacts_df_contact_target_abs_30eq(path_tables, label_object, campaign_par)

    # Round to the first decimal value
    df = df.round(1)

    p = _write_dict(df, path_output_json, label_attribute_element,
                    (element_obj['min_default_zoom'], element_obj['max_default_zoom']))
    return p


def postprocessing_standard_tabcontacts_plot_contact_target_trp_30eq(path_tables, path_output_json, label_object,
                                                                     label_attribute_element,
                                                                     campaign_par, element_obj):
    df = postprocessing_standard_tabcontacts_df_contact_target_trp_30eq(path_tables, label_object, campaign_par)

    # Round to the first decimal value
    df = df.round(1)

    p = _write_dict(df, path_output_json, label_attribute_element,
                    (element_obj['min_default_zoom'], element_obj['max_default_zoom']))
    return p


def postprocessing_standard_tabcontactsbu_table_contactcum_target_abs(path_tables, path_output_json, label_object,
                                                                      label_attribute_element, campaign_par,
                                                                      element_obj):
    df = postprocessing_standard_tabcontactsbu_df_contactcum_target_abs(path_tables, label_object, campaign_par)
    df = _compute_total_buildup(df)

    # Round to the first decimal value
    df = df.round(1)

    p = _write_dict(df, path_output_json, label_attribute_element,
                    (element_obj['min_default_zoom'], element_obj['max_default_zoom']))
    return p


def postprocessing_standard_tabcontactsbu_plot_contactcum_target_abs(path_tables, path_output_json, label_object,
                                                                     label_attribute_element, campaign_par,
                                                                     element_obj):
    df = postprocessing_standard_tabcontactsbu_df_contactcum_target_abs(path_tables, label_object, campaign_par)

    # Round to the first decimal value
    df = df.round(1)

    p = _write_dict(df, path_output_json, label_attribute_element,
                    (element_obj['min_default_zoom'], element_obj['max_default_zoom']))
    return p


def postprocessing_standard_tabcontactsbu_table_contactcum_target_trp(path_tables, path_output_json, label_object,
                                                                      label_attribute_element, campaign_par,
                                                                      element_obj):
    df = postprocessing_standard_tabcontactsbu_df_contactcum_target_trp(path_tables, label_object, campaign_par)
    df = _compute_total_buildup(df)

    # Round to the first decimal value
    df = df.round(1)

    p = _write_dict(df, path_output_json, label_attribute_element,
                    (element_obj['min_default_zoom'], element_obj['max_default_zoom']))
    return p


def postprocessing_standard_tabcontactsbu_plot_contactcum_target_trp(path_tables, path_output_json, label_object,
                                                                     label_attribute_element, campaign_par,
                                                                     element_obj):
    df = postprocessing_standard_tabcontactsbu_df_contactcum_target_trp(path_tables, label_object, campaign_par)

    # Round to the first decimal value
    df = df.round(1)

    p = _write_dict(df, path_output_json, label_attribute_element,
                    (element_obj['min_default_zoom'], element_obj['max_default_zoom']))
    return p


def postprocessing_standard_tabcontactsbu_table_contactdaily_target_abs(path_tables, path_output_json, label_object,
                                                                        label_attribute_element,
                                                                        campaign_par, element_obj):
    df = postprocessing_standard_tabcontactsbu_df_contactdaily_target_abs(path_tables, label_object, campaign_par)
    df = _compute_total_buildup(df)

    # Round to the first decimal value
    df = df.round(1)

    p = _write_dict(df, path_output_json, label_attribute_element,
                    (element_obj['min_default_zoom'], element_obj['max_default_zoom']))
    return p


def postprocessing_standard_tabcontactsbu_plot_contactdaily_target_abs(path_tables, path_output_json, label_object,
                                                                       label_attribute_element,
                                                                       campaign_par, element_obj):
    df = postprocessing_standard_tabcontactsbu_df_contactdaily_target_abs(path_tables, label_object, campaign_par)

    # Round to the first decimal value
    df = df.round(1)

    p = _write_dict(df, path_output_json, label_attribute_element,
                    (element_obj['min_default_zoom'], element_obj['max_default_zoom']))
    return p


def postprocessing_standard_tabcontactsbu_table_contactdaily_target_trp(path_tables, path_output_json, label_object,
                                                                        label_attribute_element,
                                                                        campaign_par, element_obj):
    df = postprocessing_standard_tabcontactsbu_df_contactdaily_target_trp(path_tables, label_object, campaign_par)
    df = _compute_total_buildup(df)

    # Round to the first decimal value
    df = df.round(1)

    p = _write_dict(df, path_output_json, label_attribute_element,
                    (element_obj['min_default_zoom'], element_obj['max_default_zoom']))
    return p


def postprocessing_standard_tabcontactsbu_plot_contactdaily_target_trp(path_tables, path_output_json, label_object,
                                                                       label_attribute_element,
                                                                       campaign_par, element_obj):
    df = postprocessing_standard_tabcontactsbu_df_contactdaily_target_trp(path_tables, label_object, campaign_par)

    # Round to the first decimal value
    df = df.round(1)

    p = _write_dict(df, path_output_json, label_attribute_element,
                    (element_obj['min_default_zoom'], element_obj['max_default_zoom']))
    return p


def postprocessing_standard_tabr1_table_reach_target_abs(path_tables, path_output_json, label_object,
                                                         label_attribute_element, campaign_par, element_obj):
    df = postprocessing_standard_tabr1_df_reach_target_abs(path_tables, label_object, campaign_par)

    # Round to the first decimal value
    df = df.round(1)

    p = _write_dict(df, path_output_json, label_attribute_element,
                    (element_obj['min_default_zoom'], element_obj['max_default_zoom']))
    return p


def postprocessing_standard_tabr1_plot_reach_target_abs(path_tables, path_output_json, label_object,
                                                        label_attribute_element, campaign_par, element_obj):
    df = postprocessing_standard_tabr1_df_reach_target_abs(path_tables, label_object, campaign_par)

    # Round to the first decimal value
    df = df.round(1)

    p = _write_dict(df, path_output_json, label_attribute_element,
                    (element_obj['min_default_zoom'], element_obj['max_default_zoom']))
    return p


def postprocessing_standard_tabr1_table_reach_target_perc(path_tables, path_output_json, label_object,
                                                          label_attribute_element, campaign_par, element_obj):
    df = postprocessing_standard_tabr1_df_reach_target_perc(path_tables, label_object, campaign_par)

    # Round to the first decimal value
    df = df.round(1)

    p = _write_dict(df, path_output_json, label_attribute_element,
                    (element_obj['min_default_zoom'], element_obj['max_default_zoom']))
    return p


def postprocessing_standard_tabr1_plot_reach_target_perc(path_tables, path_output_json, label_object,
                                                         label_attribute_element, campaign_par, element_obj):
    df = postprocessing_standard_tabr1_df_reach_target_perc(path_tables, label_object, campaign_par)

    # Round to the first decimal value
    df = df.round(1)

    p = _write_dict(df, path_output_json, label_attribute_element,
                    (element_obj['min_default_zoom'], element_obj['max_default_zoom']))
    return p


def postprocessing_standard_tabrf_plot_reach_target_abs(path_tables, path_output_json, label_object,
                                                        label_attribute_element, campaign_par, element_obj):
    df = postprocessing_standard_tabrf_df_reach_target_abs(path_tables, label_object, campaign_par)

    # Round to the first decimal value
    df = df.round(1)

    df = df.drop(columns='Average freq')
    p = _write_dict(df, path_output_json, label_attribute_element,
                    (element_obj['min_default_zoom'], element_obj['max_default_zoom']))
    return p


def postprocessing_standard_tabrf_table_reach_target_abs(path_tables, path_output_json, label_object,
                                                         label_attribute_element, campaign_par, element_obj):
    df = postprocessing_standard_tabrf_df_reach_target_abs(path_tables, label_object, campaign_par)

    # Round to the first decimal value
    df = df.round(1)

    p = _write_dict(df, path_output_json, label_attribute_element,
                    (element_obj['min_default_zoom'], element_obj['max_default_zoom']))
    return p


def postprocessing_standard_tabrf_table_reach_target_perc(path_tables, path_output_json, label_object,
                                                          label_attribute_element, campaign_par, element_obj):
    df = postprocessing_standard_tabrf_df_reach_target_perc(path_tables, label_object, campaign_par)

    # Round to the first decimal value
    df = df.round(1)

    p = _write_dict(df, path_output_json, label_attribute_element,
                    (element_obj['min_default_zoom'], element_obj['max_default_zoom']))
    return p


def postprocessing_standard_tabrf_plot_reach_target_perc(path_tables, path_output_json, label_object,
                                                         label_attribute_element, campaign_par, element_obj):
    df = postprocessing_standard_tabrf_df_reach_target_perc(path_tables, label_object, campaign_par)
    df = df.drop(columns='Average freq')

    # Round to the first decimal value
    df = df.round(1)

    p = _write_dict(df, path_output_json, label_attribute_element,
                    (element_obj['min_default_zoom'], element_obj['max_default_zoom']))
    return p


def postprocessing_standard_tabr1bu_table_reach_target_abs(path_tables, path_output_json, label_object,
                                                           label_attribute_element, campaign_par, element_obj):
    df = postprocessing_standard_tabr1bu_df_reach_target_abs(path_tables, label_object, campaign_par)

    # Round to the first decimal value
    df = df.round(1)

    p = _write_dict(df, path_output_json, label_attribute_element,
                    (element_obj['min_default_zoom'], element_obj['max_default_zoom']))
    return p


def postprocessing_standard_tabr1bu_plot_reach_target_abs(path_tables, path_output_json, label_object,
                                                          label_attribute_element, campaign_par, element_obj):
    df = postprocessing_standard_tabr1bu_df_reach_target_abs(path_tables, label_object, campaign_par)

    # Round to the first decimal value
    df = df.round(1)

    p = _write_dict(df, path_output_json, label_attribute_element,
                    (element_obj['min_default_zoom'], element_obj['max_default_zoom']))
    return p


def postprocessing_standard_tabr1bu_table_reach_target_perc(path_tables, path_output_json, label_object,
                                                            label_attribute_element, campaign_par, element_obj):
    df = postprocessing_standard_tabr1bu_df_reach_target_perc(path_tables, label_object, campaign_par)

    # Round to the first decimal value
    df = df.round(1)

    p = _write_dict(df, path_output_json, label_attribute_element,
                    (element_obj['min_default_zoom'], element_obj['max_default_zoom']))
    return p


def postprocessing_standard_tabr1bu_plot_reach_target_perc(path_tables, path_output_json, label_object,
                                                           label_attribute_element, campaign_par, element_obj):
    df = postprocessing_standard_tabr1bu_df_reach_target_perc(path_tables, label_object, campaign_par)

    # Round to the first decimal value
    df = df.round(1)

    p = _write_dict(df, path_output_json, label_attribute_element,
                    (element_obj['min_default_zoom'], element_obj['max_default_zoom']))
    return p


def postprocessing_standard_tabsummary_table_contactreach_target_abs(path_tables, path_output_json, label_object,
                                                                     label_attribute_element, campaign_par,
                                                                     element_obj):
    p = _compute_summary_table(campaign_par, element_obj, label_attribute_element, label_object, path_output_json,
                               path_tables)
    return p


def postprocessing_standard_tabsummary_table_contactreach_target_perc(path_tables, path_output_json, label_object,
                                                                      label_attribute_element, campaign_par,
                                                                      element_obj):
    p = _compute_summary_table(campaign_par, element_obj, label_attribute_element, label_object, path_output_json,
                               path_tables)
    return p


def postprocessing_standard_tabsummary_plot_contact_target_abs(path_tables, path_output_json, label_object,
                                                               label_attribute_element, campaign_par, element_obj):
    p = postprocessing_standard_tabcontacts_plot_contact_target_abs_raw(path_tables, path_output_json, label_object,
                                                                        label_attribute_element,
                                                                        campaign_par, element_obj)
    return p


def postprocessing_standard_tabsummary_plot_contact_target_perc(path_tables, path_output_json, label_object,
                                                                label_attribute_element, campaign_par, element_obj):
    p = postprocessing_standard_tabcontacts_plot_contact_target_trp_raw(path_tables, path_output_json, label_object,
                                                                        label_attribute_element,
                                                                        campaign_par, element_obj)
    return p


def postprocessing_standard_tabsummary_plot_contactcum_target_abs(path_tables, path_output_json, label_object,
                                                                  label_attribute_element, campaign_par, element_obj):
    p = postprocessing_standard_tabcontactsbu_plot_contactcum_target_abs(path_tables, path_output_json, label_object,
                                                                         label_attribute_element,
                                                                         campaign_par, element_obj)
    return p


def postprocessing_standard_tabsummary_plot_contactcum_target_perc(path_tables, path_output_json, label_object,
                                                                   label_attribute_element, campaign_par, element_obj):
    p = postprocessing_standard_tabcontactsbu_plot_contactcum_target_trp(path_tables, path_output_json, label_object,
                                                                         label_attribute_element,
                                                                         campaign_par, element_obj)
    return p


def postprocessing_standard_tabsummary_plot_reach1plus_target_abs(path_tables, path_output_json, label_object,
                                                                  label_attribute_element, campaign_par, element_obj):
    p = postprocessing_standard_tabr1_plot_reach_target_abs(path_tables, path_output_json, label_object,
                                                            label_attribute_element,
                                                            campaign_par, element_obj)
    return p


def postprocessing_standard_tabsummary_plot_reach1plus_target_perc(path_tables, path_output_json, label_object,
                                                                   label_attribute_element, campaign_par, element_obj):
    p = postprocessing_standard_tabr1_plot_reach_target_perc(path_tables, path_output_json, label_object,
                                                             label_attribute_element,
                                                             campaign_par, element_obj)
    return p


def postprocessing_standard_tabsummary_plot_reachfrequency_target_abs(path_tables, path_output_json, label_object,
                                                                      label_attribute_element, campaign_par,
                                                                      element_obj):
    p = postprocessing_standard_tabrf_plot_reach_target_abs(path_tables, path_output_json, label_object,
                                                            label_attribute_element,
                                                            campaign_par, element_obj)
    return p


def postprocessing_standard_tabsummary_plot_reachfrequency_target_perc(path_tables, path_output_json, label_object,
                                                                       label_attribute_element, campaign_par,
                                                                       element_obj):
    p = postprocessing_standard_tabrf_plot_reach_target_perc(path_tables, path_output_json, label_object,
                                                             label_attribute_element,
                                                             campaign_par, element_obj)
    return p


def postprocessing_standard_tabsummary_table_universe_target_abs(path_tables, path_output_json, label_object,
                                                                 label_attribute_element, campaign_par,
                                                                 element_obj):
    df = postprocessing_standard_tabsummary_df_universe_target_abs(path_tables, label_object, campaign_par)
    df = df.set_index('target_name')

    return_dict = df.to_dict(orient='dict')['target_universe']
    with open(path_output_json, 'w') as f:
        json.dump(return_dict, f, indent=3)

    return path_output_json


def generatefile_standard_resultexcel(path_tables, path_dir_output, label_object, campaign_par):
    children_request = _extract_child_id(path_tables)

    dict_to_write = dict()

    # Info Tab
    request_json = campaign_par['json_request']
    df_campaign_name = pd.DataFrame([request_json['name_campaign']], index=['Campaign name'])
    df_warning = pd.DataFrame(campaign_par['warning_desc'], index=['Warnings list'])
    df_target_name = pd.DataFrame([x['name_target'] for x in request_json['target']], columns=['Target name']).T
    df_sgcode = pd.DataFrame(request_json['sg_code'])
    df_sgcode = df_sgcode.rename(
        columns={'id': 'Sg code list', 'period_start': 'period start', 'period_end': 'period end'}).set_index(
        'Sg code list')

    min_sgcode_date = df_sgcode['period start'].min()
    max_sgcode_date = df_sgcode['period end'].max()

    df_broadcaster = _get_selected_broadcasters(request_json, label_object)
    df_channel = _get_selected_channels(request_json, label_object, min_sgcode_date, max_sgcode_date)
    df_device_type = _get_selected_device_types(request_json, label_object)
    df_online_video = _get_selected_online_video(request_json)

    # Lookup table in scope (added to all the Excel sheets with the only exception of the Info Tab)
    df_lookup_in_scope = _generate_df_lookup_in_scope()
    df_lookup_in_scope = _update_df_lookup_in_scope(path_tables, label_object, df_lookup_in_scope)

    # Contacts (000 and trp) sex age group
    df_contacts_by_sexage = _excel_contact_sexage(campaign_par, label_object, path_tables, None)
    df_contacts_by_sexage = _handle_child_reports(campaign_par, children_request, df_contacts_by_sexage,
                                                  label_object, path_tables, _excel_contact_sexage)
    child_request = None
    df_contacts_by_sexage = pd.merge(df_contacts_by_sexage, df_lookup_in_scope, how='left', on='type')

    dict_to_write['Contacts by sex & age'] = df_contacts_by_sexage

    # Contacts by target
    df_contacts_by_target = _excel_contact_target(campaign_par, label_object, path_tables, child_request)
    df_contacts_by_target = _handle_child_reports(campaign_par, children_request, df_contacts_by_target,
                                                  label_object, path_tables, _excel_contact_target)
    df_contacts_by_target = pd.merge(df_contacts_by_target, df_lookup_in_scope, how='left', on='type')

    dict_to_write['Contacts'] = df_contacts_by_target

    # Build-up Contacts
    df_contactsbu_daily = _excel_contact_bu_target(campaign_par, label_object, path_tables, child_request)
    df_contactsbu_daily = _handle_child_reports(campaign_par, children_request, df_contactsbu_daily,
                                                label_object, path_tables, _excel_contact_bu_target)
    df_contactsbu_daily = pd.merge(df_contactsbu_daily, df_lookup_in_scope, how='left', on='type')

    dict_to_write['Build-up Contacts'] = df_contactsbu_daily

    # RCH 1+
    df_reach_1plus = _excel_reach_1plus_target(campaign_par, label_object, path_tables, child_request)
    df_reach_1plus = _handle_child_reports(campaign_par, children_request, df_reach_1plus,
                                           label_object, path_tables, _excel_reach_1plus_target)
    df_reach_1plus = pd.merge(df_reach_1plus, df_lookup_in_scope, how='left', on='type')

    dict_to_write['RCH 1+'] = df_reach_1plus

    # Reach & Frequency
    df_rf = _excel_rf_target(campaign_par, label_object, path_tables, child_request)
    df_rf = _handle_child_reports(campaign_par, children_request, df_rf,
                                  label_object, path_tables, _excel_rf_target)
    df_rf = pd.merge(df_rf, df_lookup_in_scope, how='left', on='type')

    dict_to_write['Reach & Frequency'] = df_rf

    # Build-up RCH 1+
    df_reach_bu = _excel_reach_bu_1plus_target(campaign_par, label_object, path_tables, child_request)
    df_reach_bu = _handle_child_reports(campaign_par, children_request, df_reach_bu,
                                        label_object, path_tables, _excel_reach_bu_1plus_target)
    df_reach_bu = pd.merge(df_reach_bu, df_lookup_in_scope, how='left', on='type')

    dict_to_write['Build-up RCH 1+'] = df_reach_bu

    # Definitions
    df_definitions = _excel_definitions()
    dict_to_write['Definitions'] = df_definitions

    path_output_excel = os.path.join(path_dir_output, _normalize_campaign_name(campaign_par['name_campaign']) + '.xlsx')

    # Info - Note
    if children_request:
        sg_request = df_sgcode.index.tolist()  # Spotgate codes that are present in the request
        sg_no_impressions = set()  # Set which is designed to host spotgate codes with no impressions associated

        # Dataframes corresponding each to an Excel sheet containing numerical data
        df_to_parse = [df_contacts_by_sexage, df_contacts_by_target, df_contactsbu_daily, df_reach_1plus, df_rf,
                       df_reach_bu]

        for df in df_to_parse:
            # If there is any sg code which is not associated to impressions, update the set containing sg codes with no impressions
            sg_no_impressions = _get_sg_codes_no_impressions(df, sg_request, sg_no_impressions)

        if sg_no_impressions:  # At least one Spotgate code with no impressions associated
            sg_no_impressions_formatted = ', '.join(sg_no_impressions)
            df_note = pd.DataFrame({
                '0': [f'No impressions were found for the following Spotgate codes during the selected period: {sg_no_impressions_formatted}.']
            }, index=["Note"])
        else:  # No Spotgate codes with no impressions associated
            df_note = pd.DataFrame()

    else:
        if df_contacts_by_sexage.empty:
            df_note = pd.DataFrame({
                '0': [f'No impressions were found for all the selected Spotgate codes during the selected period.']
            }, index=["Note"])
        else:
            df_note = pd.DataFrame()

    with pd.ExcelWriter(path_output_excel, engine='xlsxwriter') as writer:

        # Write info tab
        i = 1
        df_campaign_name.to_excel(writer, sheet_name='Info', header=None, index=True, startcol=0, startrow=i)
        i = i + 2
        if not df_warning.empty:
            df_warning.to_excel(writer, sheet_name='Info', header=None, index=True, startcol=0, startrow=i)
            i = i + 2
        df_target_name.to_excel(writer, sheet_name='Info', header=None, index=True, startcol=0, startrow=i)
        i = i + 2
        df_sgcode.to_excel(writer, sheet_name='Info', header=True, index=True, startcol=0, startrow=i)
        i = i + len(df_sgcode) + 2
        df_broadcaster.to_excel(writer, sheet_name='Info', header=False, index=True, startcol=0, startrow=i)
        i = i + 1
        df_channel.to_excel(writer, sheet_name='Info', header=False, index=True, startcol=0, startrow=i)
        i = i + 1
        df_device_type.to_excel(writer, sheet_name='Info', header=False, index=True, startcol=0, startrow=i)
        i = i + 1
        df_online_video.to_excel(writer, sheet_name='Info', header=False, index=True, startcol=0, startrow=i)
        i = i + 2
        df_note.to_excel(writer, sheet_name='Info', header=False, index=True, startcol=0, startrow=i)

        # Access the worksheet and set column width
        worksheet = writer.sheets['Info']

        # Auto-fit column width based on max text length in each column
        for col_num, value in enumerate(df_campaign_name.columns.values):
            # Adjust to fit text, including index columns
            max_len = max(
                df_campaign_name[value].astype(str).map(len).max(),  # Find longest text in data
                len(str(value))  # Find longest text in headers
            )
            worksheet.set_column(col_num, col_num, max_len + 2)  # Set column width, adding a buffer for spacing

        for sheet_name, df in dict_to_write.items():
            df.to_excel(writer, sheet_name=sheet_name, index=False)
            worksheet = writer.sheets[sheet_name]
            format_num = writer.book.add_format({'num_format': '0.00'})

            for col_num, _ in enumerate(df.columns):
                worksheet.set_column(col_num, col_num, None, format_num)

    return path_dir_output


def _extract_child_id(path_tables):
    children_request = dict()
    for d in os.listdir(path_tables):
        if os.path.isdir(os.path.join(path_tables, d)):
            with open(os.path.join(path_tables, d, 'json_request.json')) as f:
                json_request = json.load(f)
            children_request[d] = json_request['sg_code'][0]
    return children_request


def _handle_child_reports(campaign_par, children_request, df, label_object, path_tables, table_to_produce):
    # If there is one or more child
    if children_request:
        df_cont = list()
        for child_dir_name, child_request in children_request.items():
            child_path_tables = f"{path_tables}/{child_dir_name}"

            # Generate the child table from the provided function
            df_child_table = table_to_produce(campaign_par, label_object, child_path_tables, child_request)

            # Sum all values in numerical columns and check if the sum is 0
            numerical_cols = df_child_table.select_dtypes(include=['number']).columns
            has_zero_sum_in_all_num_columns = df_child_table[numerical_cols].sum().eq(0).all()

            # Append child data only if there exists at least one non-zero value
            if not has_zero_sum_in_all_num_columns:
                df_cont.append(df_child_table)

        df = pd.concat([df] + df_cont, ignore_index=False)
    return df


def _excel_reach_bu_1plus_target(campaign_par, label_object, path_tables, child_request):
    df_reach_bu_abs = postprocessing_standard_tabr1bu_df_reach_target_abs(path_tables, label_object, campaign_par)
    df_reach_bu_perc = postprocessing_standard_tabr1bu_df_reach_target_perc(path_tables, label_object, campaign_par)
    df_reach_bu_abs = df_reach_bu_abs.melt(id_vars=["Target name", "Type"], value_name="num_reach",
                                           var_name=["date"])
    df_reach_bu_perc = df_reach_bu_perc.melt(id_vars=["Target name", "Type"], value_name="perc_reach",
                                             var_name=["date"])
    df_reach_bu = df_reach_bu_abs.merge(df_reach_bu_perc, on=['Type', 'Target name', 'date'])

    # If child_request is None -> parent request
    if not child_request:
        df_reach_bu['campaign level'] = 'overall campaign'
    else:  # if child_request is not None -> child request
        df_reach_bu['campaign level'] = child_request['id']

    col_order = ['campaign level', 'Target name', 'Type', 'date', 'perc_reach', 'num_reach']
    df_reach_bu = df_reach_bu[col_order]
    rename_col = {'Type': 'type', 'Target name': 'target name', 'perc_reach': 'reach (%)', 'num_reach': 'reach (000)'}
    df_reach_bu = df_reach_bu.rename(columns=rename_col)

    df_reach_bu = _sort_by_target_name(df_reach_bu, 'target name', 'reach bu')

    df_reach_bu = _drop_rows_if_no_impressions(df_reach_bu)

    # Round to the second decimal value
    df_reach_bu = df_reach_bu.round(2)

    return df_reach_bu


def _excel_rf_target(campaign_par, label_object, path_tables, child_request):
    df_rf_abs = postprocessing_standard_tabrf_df_reach_target_abs(path_tables, label_object, campaign_par)
    df_rf_abs = df_rf_abs.drop(columns='Average freq')
    df_rf_perc = postprocessing_standard_tabrf_df_reach_target_perc(path_tables, label_object, campaign_par)
    df_rf_perc = df_rf_perc.drop(columns='Average freq')
    df_rf_abs = df_rf_abs.melt(id_vars=["Target name", "Type"], value_name="num_reach",
                               var_name=["frequency"])
    df_rf_perc = df_rf_perc.melt(id_vars=["Target name", "Type"], value_name="perc_reach",
                                 var_name=["frequency"])
    df_rf = df_rf_abs.merge(df_rf_perc, on=['Type', 'Target name', 'frequency'])

    # If child_request is None -> parent request
    if not child_request:
        df_rf['campaign level'] = 'overall campaign'
    else:  # if child_request is not None -> child request
        df_rf['campaign level'] = child_request['id']

    col_order = ['campaign level', 'Target name', 'Type', 'frequency', 'perc_reach', 'num_reach']
    df_rf = df_rf[col_order]
    rename_col = {'Type': 'type', 'Target name': 'target name', 'perc_reach': 'reach (%)', 'num_reach': 'reach (000)'}
    df_rf = df_rf.rename(columns=rename_col)

    df_rf = _sort_by_target_name(df_rf, 'target name', 'rf')

    df_rf = _drop_rows_if_no_impressions(df_rf)

    # Round to the second decimal value
    df_rf = df_rf.round(2)

    return df_rf


def _excel_reach_1plus_target(campaign_par, label_object, path_tables, child_request):
    df_reach_1plus_abs = postprocessing_standard_tabr1_df_reach_target_abs(path_tables, label_object, campaign_par)
    df_reach_1plus_perc = postprocessing_standard_tabr1_df_reach_target_perc(path_tables, label_object, campaign_par)
    df_reach_1plus_abs = df_reach_1plus_abs.melt(id_vars=["Target name"], value_name="num_reach",
                                                 var_name=["type"])
    df_reach_1plus_perc = df_reach_1plus_perc.melt(id_vars=["Target name"], value_name="perc_reach",
                                                   var_name=["type"])
    df_reach_1plus = df_reach_1plus_abs.merge(df_reach_1plus_perc, on=['type', 'Target name']).sort_values('Target name')

    if not child_request:
        df_reach_1plus['campaign level'] = 'overall campaign'
    else:  # if child_request is not None -> child request
        df_reach_1plus['campaign level'] = child_request['id']

    col_order = ['campaign level', 'Target name', 'type', 'perc_reach', 'num_reach']
    df_reach_1plus = df_reach_1plus[col_order]
    rename_col = {'Type': 'type', 'Target name': 'target name', 'perc_reach': 'reach (%)', 'num_reach': 'reach (000)'}
    df_reach_1plus = df_reach_1plus.rename(columns=rename_col)

    df_reach_1plus = _sort_by_target_name(df_reach_1plus, 'target name', 'reach')

    df_reach_1plus = _drop_rows_if_no_impressions(df_reach_1plus)

    # Round to the second decimal value
    df_reach_1plus = df_reach_1plus.round(2)

    return df_reach_1plus


def _excel_contact_bu_target(campaign_par, label_object, path_tables, child_request):
    df_contactsbu_daily_abs = postprocessing_standard_tabcontactsbu_df_contactdaily_target_abs(path_tables,
                                                                                               label_object,
                                                                                               campaign_par)
    df_contactsbu_daily_trp = postprocessing_standard_tabcontactsbu_df_contactdaily_target_trp(path_tables,
                                                                                               label_object,
                                                                                               campaign_par)
    df_contactsbu_daily_abs = df_contactsbu_daily_abs.melt(id_vars=["Type", "Target name"], value_name="num_contact",
                                                           var_name=["date"])
    df_contactsbu_daily_trp = df_contactsbu_daily_trp.melt(id_vars=["Type", "Target name"], value_name="num_trp",
                                                           var_name=["date"])
    df_contactsbu_daily = df_contactsbu_daily_abs.merge(df_contactsbu_daily_trp, on=['Type', 'Target name', 'date'])
    if not child_request:
        df_contactsbu_daily['campaign level'] = 'overall campaign'
    else:  # if child_request is not None -> child request
        df_contactsbu_daily['campaign level'] = child_request['id']

    col_order = ['campaign level', 'Target name', 'Type', 'date', 'num_trp', 'num_contact']
    df_contactsbu_daily = df_contactsbu_daily[col_order]
    rename_col = {'Type': 'type', 'Target name': 'target name', 'num_contact': 'contacts (000)', 'num_trp': 'trps'}
    df_contactsbu_daily = df_contactsbu_daily.rename(columns=rename_col)

    df_contactsbu_daily = _sort_by_target_name(df_contactsbu_daily, 'target name', 'contact bu')

    df_contactsbu_daily = _drop_rows_if_no_impressions(df_contactsbu_daily)

    # Round to the second decimal value
    df_contactsbu_daily = df_contactsbu_daily.round(2)

    return df_contactsbu_daily


def _excel_contact_target(campaign_par, label_object, path_tables, child_request):
    df_contacts_by_target_abs = postprocessing_standard_tabcontacts_df_contact_target_abs_raw(path_tables, label_object,
                                                                                              campaign_par)
    df_contacts_by_target_trp = postprocessing_standard_tabcontacts_df_contact_target_trp_raw(path_tables, label_object,
                                                                                              campaign_par)
    df_contacts_by_target_abs = df_contacts_by_target_abs.melt(id_vars=["Type"], value_name="num_contact",
                                                               var_name=["target_name"])
    df_contacts_by_target_trp = df_contacts_by_target_trp.melt(id_vars=["Type"], value_name="num_trp",
                                                               var_name=["target_name"])
    df_contacts_by_target = df_contacts_by_target_abs.merge(df_contacts_by_target_trp, on=['Type', 'target_name'])

    if not child_request:
        df_contacts_by_target['campaign level'] = 'overall campaign'
    else:  # if child_request is not None -> child request
        df_contacts_by_target['campaign level'] = child_request['id']

    col_order = ['campaign level', 'target_name', 'Type', 'num_trp', 'num_contact']
    df_contacts_by_target = df_contacts_by_target[col_order]
    rename_col = {'Type': 'type', 'target_name': 'target name', 'num_contact': 'contacts (000)', 'num_trp': 'trps'}
    df_contacts_by_target = df_contacts_by_target.rename(columns=rename_col)

    df_contacts_by_target = _sort_by_target_name(df_contacts_by_target, 'target name', 'contact')

    df_contacts_by_target = _drop_rows_if_no_impressions(df_contacts_by_target)

    # Round to the second decimal value
    df_contacts_by_target = df_contacts_by_target.round(2)

    return df_contacts_by_target


def _excel_contact_sexage(campaign_par, label_object, path_tables, child_request):
    df_contacts_by_sexage_abs = postprocessing_standard_tabcontacts_df_contact_sexage_abs_raw(path_tables, label_object,
                                                                                              campaign_par)
    df_contacts_by_sexage_trp = postprocessing_standard_tabcontacts_df_contact_sexage_trp_raw(path_tables, label_object,
                                                                                              campaign_par)
    df_contacts_by_sexage_abs = df_contacts_by_sexage_abs.melt(id_vars=["Type"], value_name="num_contact",
                                                               var_name=["sexage_group"])
    df_contacts_by_sexage_trp = df_contacts_by_sexage_trp.melt(id_vars=["Type"], value_name="num_trp",
                                                               var_name=["sexage_group"])
    df_contacts_by_sexage = df_contacts_by_sexage_abs.merge(df_contacts_by_sexage_trp, on=['Type', 'sexage_group'])

    # If child_request is None -> parent request
    if not child_request:
        df_contacts_by_sexage['campaign level'] = 'overall campaign'
    else:  # if child_request is not None -> child request
        df_contacts_by_sexage['campaign level'] = child_request['id']

    col_order = ['campaign level', 'sexage_group', 'Type', 'num_trp', 'num_contact']
    df_contacts_by_sexage = df_contacts_by_sexage[col_order]
    rename_col = {'Type': 'type', 'sexage_group': 'sex age group', 'num_contact': 'contacts (000)', 'num_trp': 'trps'}
    df_contacts_by_sexage = df_contacts_by_sexage.rename(columns=rename_col)

    df_contacts_by_sexage = _drop_rows_if_no_impressions(df_contacts_by_sexage)

    # Round to the second decimal value
    df_contacts_by_sexage = df_contacts_by_sexage.round(2)

    return df_contacts_by_sexage


def _excel_definitions():
    definitions_data = {
        'Type': ['Contacts', 'TV', 'Streaming Services', 'Online Video', 'TRP', '30sec equivalent'],
        'Definition': [
            'The number of individuals watching an advertisement for its entire duration',
            'Ads on linear TV watched on TV sets, either live or recorded and played back',
            'VOD viewed on broadcaster players (and potentially other streaming services) plus linear channels streamed on devices other than TV sets',
            'Video ads streamed alongside video content viewed on websites',
            'TRP (Total Rating Points) The number of contacts in a given target group divided by the size of the target group multiplied by 100',
            'For campaigns that include spot length that are longer or less than 30 seconds the report is recalculated to the equivalent of 30 sec spots'
        ]
    }

    df = pd.DataFrame(definitions_data)

    return df


def _write_dict(df, path_output_json, label_attribute_element, zoom_par):
    return_dict = dict()
    return_dict['data'] = df.to_dict(orient='records')
    return_dict['label_metadata'] = label_attribute_element
    zoom_par = [0 if x is None else x for x in zoom_par]
    if sum(zoom_par) == 0:
        return_dict['zoom'] = None
    else:
        return_dict['zoom'] = {'min': zoom_par[0], 'max': zoom_par[1]}
    with open(path_output_json, 'w') as f:
        json.dump(return_dict, f, indent=3)

    return path_output_json


def _scaffolding_contacts(col_to_scaf, label_object, target_name=None, df_date_range=None):
    dict_col = {x: list(label_object['replace'][x].keys()) for x in label_object['replace']}
    df_cont = list()
    for x in col_to_scaf:
        if x not in ['target_name', 'date', 'start_date', 'end_date', 'frequency']:
            values = dict_col[x]
            if x in ['broadcaster', 'ad_type']:
                values.remove('all')
            df_cont.append(pd.DataFrame(dict_col[x], columns=[x]))

    if len(df_cont) == 1:
        return df_cont[0]

    df_out = df_cont[0]

    for t in df_cont[1:]:
        df_out = df_out.merge(t, how='cross')

    if target_name is not None:
        df_target = pd.DataFrame(target_name, columns=['target_name'])
        df_out = df_out.merge(df_target, how='cross')

    if df_date_range is not None:
        df_out = df_out.merge(df_date_range, how='cross')

    # Remove no sense combination
    if 'ad_type' in df_out.columns and 'broadcaster' in df_out.columns:
        remove_raw = -((df_out['ad_type'] == 'dynamic') & (df_out['broadcaster'] == 'other'))
        df_out = df_out[remove_raw]

    return df_out


def _scaffolding_rf(col_to_scaf, label_object, target_name=None, df_period_range=None, max_freq=None):
    dict_col = {x: list(label_object['replace'][x].keys()) for x in label_object['replace']}
    df_cont = list()
    for x in col_to_scaf:
        if x not in ['target_name', 'date', 'start_date', 'end_date', 'frequency']:
            df_cont.append(pd.DataFrame(dict_col[x], columns=[x]))

    if len(df_cont) == 1:
        return df_cont[0]

    df_out = df_cont[0]

    for t in df_cont[1:]:
        df_out = df_out.merge(t, how='cross')

    if target_name is not None:
        df_target = pd.DataFrame(target_name, columns=['target_name'])
        df_out = df_out.merge(df_target, how='cross')

    if df_period_range is not None:
        df_out = df_out.merge(df_period_range, how='cross')

    if max_freq is not None:
        df_freq = pd.DataFrame(list(range(0, max_freq + 1)), columns=['frequency'])
        df_out = df_out.merge(df_freq, how='cross')

    # Remove no sense combination
    if 'ad_type' in df_out.columns and 'broadcaster' in df_out.columns:
        remove_raw = -((df_out['ad_type'] == 'dynamic') & (df_out['broadcaster'] == 'other'))
        df_out = df_out[remove_raw]
        remove_raw = -((df_out['ad_type'] == 'all') & (df_out['broadcaster'] != 'all'))
        df_out = df_out[remove_raw]

    return df_out


def _generate_df_lookup_in_scope():
    data = {
        "type": [
            "MTV TV", "Sanoma TV", "WBD TV",
            "MTV TV screen Streaming services", "MTV Small screen Streaming services",
            "Sanoma TV screen Streaming services", "Sanoma Small screen Streaming services", "Sanoma Small screen Online video",
            "TV+Streaming services+Online video", "TV+Streaming services", "TV",
            "Streaming services", "MTV Streaming services", "Sanoma Streaming services", "Sanoma Online video"
        ],
        "MTV TV": [True, False, False, False, False, False, False, False, True, True, True, False, False, False, False],
        "Sanoma TV": [False, True, False, False, False, False, False, False, True, True, True, False, False, False, False],
        "WBD TV": [False, False, True, False, False, False, False, False, True, True, True, False, False, False, False],
        "MTV Streaming services": [False, False, False, True, True, False, False, False, True, True, False, True, True, False, False],
        "Sanoma Streaming services": [False, False, False, False, False, True, True, False, True, True, False, True, False, True, False],
        "Sanoma Online video": [False, False, False, False, False, False, False, True, True, False, False, False, False, False, True]
    }

    df = pd.DataFrame(data)

    return df


def _get_selected_broadcasters(request_json, label_object):
    if not request_json['filter']['broadcaster']:
        selected_streaming_services_broadcaster = ['MTV', 'Sanoma']
        df_broadcaster = pd.DataFrame([selected_streaming_services_broadcaster], index=['Selected streaming services broadcaster'])
    else:
        selected_streaming_services_broadcaster = request_json['filter']['broadcaster']
        df_broadcaster = pd.DataFrame(selected_streaming_services_broadcaster, index=['Selected streaming services broadcaster'])
        df_broadcaster = df_broadcaster.replace(label_object['replace']['broadcaster'])

    return df_broadcaster


def _get_selected_channels(request_json, label_object, min_sgcode_date, max_sgcode_date):
    """
    # On 2025-01-01, ownership of FOX (channel 72) and National Geographic (channel 80) changed
    # from Sanoma to MTV. For any date before 2025-01-01, these channels should be assigned
    # to Sanoma. For dates on or after 2025-01-01, they should be assigned to MTV.

    :param request_json: The JSON file containing the selection/non-selection of channels.
    :param label_object: The JSON file containing the mapping association between channel number and name.
    :param min_sgcode_date: The very first date expressed in the request (not used, but it is there for clarity).
    :param max_sgcode_date: The very last date expressed in the request.
    :return:
    """
    # Old campaigns
    if DISNEY_TO_MTV_DATE > max_sgcode_date:
        label_object['replace']['channel']['72'] = 'Sanoma - FOX'
        label_object['replace']['channel']['80'] = 'Sanoma - National Geographic'

        # All possible TV channels prior to the switchover, sorted by broadcaster and channel code
        channel_order = ['3', '12', '65', '5', '47', '68', '72', '80', '89', '13', '29', '51', '74', '91', '100']
        sorted_channels = {key: label_object['replace']['channel'][key] for key in channel_order if key in label_object['replace']['channel']}

        if not request_json['filter']['channel']:
            all_channels = list(sorted_channels.values())
            df_channel = pd.DataFrame([all_channels], index=['Selected TV channel'])
        else:
            # Retrieve only selected TV channel codes and sort them according to the old order
            selected_tv_channel_code = request_json['filter']['channel']
            sorted_selected_tv_channels = {str(channel_code): sorted_channels[channel_code] for
                                           channel_code in sorted_channels if
                                           channel_code in selected_tv_channel_code}

            selected_channels = list(sorted_selected_tv_channels.values())
            df_channel = pd.DataFrame([selected_channels], index=['Selected TV channel'])

    # Cross and new campaigns
    else:
        if not request_json['filter']['channel']:
            all_channels = list(label_object['replace']['channel'].values())
            df_channel = pd.DataFrame([all_channels], index=['Selected TV channel'])
        else:
            # Retrieve only selected TV channel codes and sort them according to the current order
            selected_tv_channel_code = request_json['filter']['channel']
            sorted_selected_tv_channels = {str(channel_code): label_object['replace']['channel'][channel_code] for
                                           channel_code in label_object['replace']['channel'] if
                                           channel_code in selected_tv_channel_code}

            selected_channels = list(sorted_selected_tv_channels.values())
            df_channel = pd.DataFrame([selected_channels], index=['Selected TV channel'])

    return df_channel


def _get_selected_device_types(request_json, label_object):
    if not request_json['filter']['device_type']:
        df_device_type = pd.DataFrame(label_object['replace']['device_type'].values(),
                                      columns=['Selected device type']).T
    else:
        df_device_type = pd.DataFrame(request_json['filter']['device_type'], columns=['Selected device type'])
        df_device_type = df_device_type.replace(label_object['replace']['device_type']).T

    return df_device_type


def _get_selected_online_video(request_json):
    if request_json['filter']['online_video']:
        df_online_video = pd.DataFrame(['Sanoma Online video'], index=['Selected online video'])
    else:
        df_online_video = pd.DataFrame([''], index=['Selected online video'])

    return df_online_video


def _update_df_lookup_in_scope(path_tables, label_object, df_lookup_in_scope):
    """
    Check the presence of MTV, Sanoma Streaming services and MTV, Sanoma, Other (broadcaster) TV data in the contacts table.
    Then, update the lookup dataframe by setting to False values in columns whose name corresponds to missing atomic
    elements (or groups of them).
    """
    df_contacts_by_target = _extract_df_from_json_file(path_tables, 'impacts_in_target')

    col_to_filter = ['broadcaster', 'device_type', 'ad_type']

    if df_contacts_by_target.empty:
        df_contacts_by_target[col_to_filter] = None

    df_atomic_elements = df_contacts_by_target[col_to_filter].drop_duplicates()

    df_atomic_elements['broadcaster'] = df_atomic_elements['broadcaster'].replace(
        label_object['replace']['broadcaster'])
    df_atomic_elements['device_type'] = df_atomic_elements['device_type'].replace(
        label_object['replace']['device_type'])
    df_atomic_elements['ad_type'] = df_atomic_elements['ad_type'].replace(
        label_object['replace']['ad_type'])

    # Add Type column to the atomic elements dataframe
    type_series = []
    for idx, element in df_atomic_elements.iterrows():
        if (element['ad_type'] == 'Streaming services') or (element['ad_type'] == 'Online video'):
            type_series.append(element['broadcaster'] + ' ' + element['device_type'] + ' ' + element['ad_type'])
        elif element['ad_type'] == 'TV':
            type_series.append(element['broadcaster'] + ' ' + element['ad_type'])

    df_atomic_elements['type'] = type_series

    # Retrieve the distinct atomic elements
    atomic_elements = [element for element in df_atomic_elements['type']]

    # Apply updates to the lookup dataframe depending on the presence of the atomic elements
    if not any(elem in ["MTV TV screen Streaming services", "MTV Small screen Streaming services"] for elem in atomic_elements):
        df_lookup_in_scope['MTV Streaming services'] = False
    if not any(elem in ["Sanoma TV screen Streaming services", "Sanoma Small screen Streaming services"] for elem in atomic_elements):
        df_lookup_in_scope['Sanoma Streaming services'] = False
    if 'MTV TV' not in atomic_elements:
        df_lookup_in_scope['MTV TV'] = False
    if 'Sanoma TV' not in atomic_elements:
        df_lookup_in_scope['Sanoma TV'] = False
    if 'WBD TV' not in atomic_elements:
        df_lookup_in_scope['WBD TV'] = False
    if 'Sanoma Small screen Online video' not in atomic_elements:
        df_lookup_in_scope['Sanoma Online video'] = False

    return df_lookup_in_scope


def _concat_labels(df_contact_sexage, col_to_use, sep):
    return df_contact_sexage[col_to_use].apply(lambda row: sep.join([x for x in row if x != '']), axis=1)


def _extract_df_from_json_file(path_tables, table_name):
    """
    Return the pandas dataframe from the report_table attribute of the json file from the API

    :param str path_tables: path of the directory which the json files are stored
    :return: pd.DataFrame
    """
    with open(os.path.join(path_tables, table_name + '.json')) as f:
        json_report = json.load(f)
    df_table = pd.DataFrame.from_dict(json_report['report_table'])
    if 'date' in df_table.columns:
        df_table['date'] = pd.to_datetime(df_table['date'])
    if 'start_date' in df_table.columns:
        df_table['start_date'] = pd.to_datetime(df_table['start_date'])
    if 'end_date' in df_table.columns:
        df_table['end_date'] = pd.to_datetime(df_table['end_date'])
    return df_table


def _format_rows(df, label_object, tab_type='contacts'):
    # Filter out rows with all 0s
    if '_total' in df.columns:
        if not (df['_total'] == 0).all():
            df = df[df["_total"] > 0]

    # Set rows order
    df = _sort_labels(df, label_object, tab_type)

    # Concat dimensions in single string
    if tab_type == 'contacts':
        col_to_use = ['_label_broadcaster', '_label_device_type', '_label_ad_type']
    elif tab_type in ('rf', 'summary'):
        col_to_use = ['_label_broadcaster', '_label_ad_type']
    else:
        raise ValueError("Specify a type of metric")

    # Concat labels
    df['_label_row'] = _concat_labels(df, col_to_use, " ")

    # Drop columns
    df = _drop_columns(df)

    # Filter only valid engagement types
    df = _filter_valid_engagement_type(df, label_object, ['Type'])

    return df


def _drop_columns(df):
    # Rename and drop columns
    df = df.rename(columns={'_label_row': 'Type', '_total': 'Total'})
    col_ = [x for x in ['broadcaster', 'device_type', 'ad_type'] if x in df.columns]
    col_to_drop = col_ + [x for x in df.columns if str(x).startswith('_')]
    df = df.drop(columns=col_to_drop)
    return df


def _filter_valid_engagement_type(df, label_object, col_to_join):
    df = pd.merge(
        df,
        pd.DataFrame(label_object['valid_engagement_type'], columns=col_to_join),
        how='inner',
        on=col_to_join
    )

    return df

def _normalize_campaign_name(name):
    name = (name
            .replace('//', ' ')
            .replace('/', ' '))
    return name


def _sort_labels(df, label_object, tab_type):
    col_to_sort = list()
    df = df.copy(deep=True)
    if 'broadcaster' in df.columns:
        df['_order_broadcaster'] = df['broadcaster'].apply(
            lambda x: label_object['order']['broadcaster'].index(x))
        col_to_sort.append('_order_broadcaster')
    if 'device_type' in df.columns:
        df['_order_device_type'] = df['device_type'].apply(
            lambda x: label_object['order']['device_type'].index(x))
        col_to_sort.append('_order_device_type')
    if 'ad_type' in df.columns:
        df['_order_ad_type'] = df['ad_type'].apply(
            lambda x: label_object['order']['ad_type'].index(x))
        col_to_sort.append('_order_ad_type')

    if tab_type == 'contacts':
        sorting_order = label_object['order']['tab_order_contacts']
    elif tab_type == 'rf':
        sorting_order = label_object['order']['tab_order_rf']
    elif tab_type == 'r1plus':
        sorting_order = label_object['order']['tab_order_r1plus']
    elif tab_type == 'summary':
        sorting_order = label_object['order']['tab_order_summary']
    else:
        sorting_order = list()

    df = df.sort_values([x for x in sorting_order if x in col_to_sort])
    return df


def _custom_labels(df, label_object):
    if 'broadcaster' in df.columns:
        df['_label_broadcaster'] = df['broadcaster'].replace(
            label_object['replace']['broadcaster'])

    if 'device_type' in df.columns:
        df['_label_device_type'] = df['device_type'].replace(
            label_object['replace']['device_type'])
        df.loc[df['ad_type'] == 'linear_static', '_label_device_type'] = ''

    if 'ad_type' in df.columns:
        df['_label_ad_type'] = df['ad_type'].replace(label_object['replace']['ad_type'])


def _compute_summary_table(campaign_par, element_obj, label_attribute_element, label_object, path_output_json,
                           path_tables):
    df_contacts_abs = postprocessing_standard_tabsummary_df_contactreach_contact_target_abs_raw(path_tables,
                                                                                                label_object,
                                                                                                campaign_par)
    df_contacts_trp = postprocessing_standard_tabsummary_df_contactreach_contact_target_perc_raw(path_tables,
                                                                                                 label_object,
                                                                                                 campaign_par)
    df_rf_perc = postprocessing_standard_tabsummary_df_contactreach_reach_target_perc(path_tables, label_object,
                                                                                      campaign_par)
    df_universe_by_target = _extract_df_from_json_file(path_tables, 'target_universe')
    df_universe_by_target = df_universe_by_target[['target_name', 'target_universe']].drop_duplicates()
    df_universe_by_target = df_universe_by_target.rename(columns={'target_name': 'Target name',
                                                                  'target_universe': 'Target universe'})

    # Put contacts (000), TRPs and reach values together
    df_merge_contacts = df_contacts_abs.merge(df_contacts_trp, on=['Type', 'Target name'])
    df_merge_rf = df_merge_contacts.merge(df_rf_perc, on=['Type', 'Target name'])

    # Add average frequency
    df_merge_rf = df_merge_rf.merge(df_universe_by_target, how='inner', on='Target name')

    denominator = (df_merge_rf['Reach 1+ (%)'] * df_merge_rf['Target universe'])
    df_merge_rf['Average freq'] = df_merge_rf['Contacts'].where(denominator != 0, 0) / denominator.where(
        denominator != 0, 1)

    # Convert Contacts into Contacts (000)
    df_merge_rf['Contacts'] = df_merge_rf['Contacts'] / 1000
    df_merge_rf = df_merge_rf.rename(columns={"Contacts": "Contacts (000)"})

    # Convert values under Reach columns
    df_merge_rf['Reach 1+ (%)'] = df_merge_rf['Reach 1+ (%)'] * 100
    df_merge_rf['Reach 2+ (%)'] = df_merge_rf['Reach 2+ (%)'] * 100
    df_merge_rf['Reach 3+ (%)'] = df_merge_rf['Reach 3+ (%)'] * 100

    col_order = ['Target name', 'Type', 'Contacts (000)', 'TRPs', 'Average freq', 'Reach 1+ (%)', 'Reach 2+ (%)', 'Reach 3+ (%)']
    df_merge_rf = df_merge_rf[col_order]
    df_merge_rf = df_merge_rf.round(1)

    # Write json
    p = _write_dict(df_merge_rf, path_output_json, label_attribute_element,
                    (element_obj['min_default_zoom'], element_obj['max_default_zoom']))
    return p


def _zip_files(folder_path, zip_name, file_to_zip, children_request):
    with zipfile.ZipFile(zip_name, 'w', zipfile.ZIP_DEFLATED) as zipf:
        for x in file_to_zip:
            zipf.write(os.path.join(folder_path, x), arcname=x)
        if children_request:
            for k, child_req in children_request.items():
                for x in file_to_zip:
                    zipf.write(os.path.join(folder_path, k, x), arcname=os.path.join(child_req['id'], x))

    return zip_name


def _compute_avg_frequency(df):
    df_average_freq = df[['target_name', 'broadcaster', 'ad_type', 'frequency', 'reach']]
    df_average_freq = df_average_freq.rename(columns={'frequency': 'num_frequency', 'reach': 'num_reach'})
    wm = lambda x: np.average(x, weights=df_average_freq.loc[x.index, "num_reach"])
    df_average_freq = df_average_freq.groupby(['target_name', 'broadcaster', 'ad_type'], as_index=False).agg(
        average_freq=("num_frequency", wm)).round(3)
    return df_average_freq


def _compute_contacts_abs_target(path_tables, label_object, campaign_par):
    # Read campaign data
    target_list = campaign_par['target_name']

    # Read table
    df_contact_target = _extract_df_from_json_file(path_tables, 'impacts_in_target')

    col_to_group = ['broadcaster', 'ad_type', 'target_name']

    if df_contact_target.empty:
        df_contact_target[col_to_group + ['num_impacts']] = None

    # Group data
    df_contact_target = df_contact_target.groupby(col_to_group, as_index=False)['num_impacts'].sum()

    # Compute Contacts Totals including Online video
    df_target_total_all_onlinevideo = df_contact_target.groupby(['target_name'], as_index=False)['num_impacts'].sum()
    df_target_total_all_onlinevideo['broadcaster'] = 'all'
    df_target_total_all_onlinevideo['ad_type'] = 'all_onlinevideo'

    # Compute Contacts Totals Linear and BVOD
    mask = df_contact_target['ad_type'].isin(['linear_static', 'dynamic'])
    df_target_total_all = df_contact_target[mask].groupby(['target_name'], as_index=False)['num_impacts'].sum()
    df_target_total_all['broadcaster'] = 'all'
    df_target_total_all['ad_type'] = 'all'

    # Compute Contacts Linear Total
    mask = df_contact_target['ad_type'] == 'linear_static'
    df_target_total_linear = df_contact_target[mask].groupby(['target_name'], as_index=False)['num_impacts'].sum()
    df_target_total_linear['broadcaster'] = 'all'
    df_target_total_linear['ad_type'] = 'linear_static'

    # Compute Contacts BVOD Total
    mask = df_contact_target['ad_type'] == 'dynamic'
    df_target_total_dynamic = df_contact_target[mask].groupby(['target_name'], as_index=False)['num_impacts'].sum()
    df_target_total_dynamic['broadcaster'] = 'all'
    df_target_total_dynamic['ad_type'] = 'dynamic'

    # Concat Totals
    df_contact_target_total = pd.concat(
        [df_contact_target, df_target_total_linear, df_target_total_dynamic, df_target_total_all,
         df_target_total_all_onlinevideo], ignore_index=True)

    # Scaffolding
    df_scaffolding = _scaffolding_rf(col_to_group, label_object, target_name=target_list)

    df_contact_target_total = df_scaffolding.merge(df_contact_target_total, how='left', on=col_to_group)
    df_contact_target_total['num_impacts'] = df_contact_target_total['num_impacts'].fillna(0)

    df_contact_target_total = df_contact_target_total.set_index(['target_name', 'broadcaster', 'ad_type'])

    df_contact_target_total['_total'] = df_contact_target_total.sum(axis=1)
    df_contact_target_total = df_contact_target_total.reset_index()

    # Add Custom labels
    _custom_labels(df_contact_target_total, label_object)

    # Flat dataframe
    concat_index_columns = list(map(lambda x: ''.join(x), df_contact_target_total.columns.values))
    df_contact_target_total.columns = concat_index_columns

    # Format rows order
    df_contact_target_total = _format_rows(df_contact_target_total, label_object, tab_type='summary')

    # Format columns order
    df_contact_target_total = df_contact_target_total.rename(
        columns={"target_name": "Target name", 'num_impacts': 'Contacts'})
    df_contact_target_total = df_contact_target_total.drop(columns=['Total'])
    col_target = [x for x in df_contact_target_total.columns if not x.startswith("Type")]
    col_to_use = ['Type'] + col_target
    df_contact_target_total = df_contact_target_total[col_to_use]

    return df_contact_target_total


def _compute_total(df):
    """
    Note that:
    - 'tv' stands for 'TV'
    - 'ss' stands for 'Streaming services'
    - 'ov' stands for 'Online video'

    :param df: The input dataframe
    :return: The input dataframe including totals
    """
    # Keep only rows whose Type is Streaming services or TV
    df_tv_ss = df[df['Type'].str.contains('Streaming services|TV')]

    # Compute Total TV, Streaming services and Online video
    df_total_tv_ss_ov = df.set_index('Type').sum().round(1).to_frame().T
    df_total_tv_ss_ov['Type'] = 'TV+Streaming services+Online video'

    is_online_video_present = df['Type'].str.contains('Online video').any()

    # Compute Total TV and Streaming services
    df_total_tv_ss = df_tv_ss.set_index('Type').sum().round(1).to_frame().T
    df_total_tv_ss['Type'] = 'TV+Streaming services'

    df = pd.concat([df, df_total_tv_ss, df_total_tv_ss_ov], ignore_index=True)

    if not is_online_video_present:
        df = df[df['Type']!='TV+Streaming services+Online video']

    return df


def _compute_total_buildup(df):
    """
    Note that:
    - 'tv' stands for 'TV'
    - 'ss' stands for 'Streaming services'
    - 'ov' stands for 'Online video'

    :param df: The input dataframe
    :return: The input dataframe including totals
    """
    # Keep only rows whose Type is Streaming services or TV
    df_tv_ss = df[df['Type'].str.contains('Streaming services|TV')]

    # Compute Total TV, Streaming services and Online video
    df_total_tv_ss_ov = df.drop(columns=['Type'])
    df_total_tv_ss_ov = df_total_tv_ss_ov.groupby(['Target name'], as_index=False).sum().round(1)
    df_total_tv_ss_ov['Type'] = 'TV+Streaming services+Online video'

    is_online_video_present = df['Type'].str.contains('Online video').any()

    # Compute Total TV and Streaming services
    df_total_tv_ss = df_tv_ss.drop(columns=['Type'])
    df_total_tv_ss = df_total_tv_ss.groupby(['Target name'], as_index=False).sum().round(1)
    df_total_tv_ss['Type'] = 'TV+Streaming services'

    df = pd.concat([df, df_total_tv_ss_ov, df_total_tv_ss], ignore_index=True)

    if not is_online_video_present:
        df = df[df['Type']!='TV+Streaming services+Online video']

    return df


def _sort_labels_totals_first(df):
    """
    Priority Types are defined by the user.

    In the case this function gets obsolete because priority Types are not aligned anymore with any label in the df,
    this function loses its effects. Hence, it might be either discarded from the parent function (make the code leaner)
    or labels might be changed (update becomes up-to-date).

    :param df:
    :return:
    """
    priority_types = ['TV+Streaming services+Online video', 'TV+Streaming services']
    df_total = df[df['Type'].isin(priority_types)]
    df_non_total = df[~df['Type'].isin(priority_types)]

    df_sorted = pd.concat([df_total, df_non_total]).reset_index(drop=True)
    return df_sorted


def _sort_by_target_name(df, col_name, tab_type):
    """
    Sort dataframe by a specific column.

    :param df:
    :param col_name:
    :param tab_type:
    :return:
    """
    # Preserve the original column 'type' order
    type_custom_order = df['type'].drop_duplicates().values
    sort_key = {value: index for index, value in enumerate(type_custom_order)}

    if (tab_type=='contact bu') or (tab_type=='reach bu'):
        df['date'] = pd.to_datetime(df['date'], format='%d-%m-%Y')
        df = df.sort_values(by=[col_name, 'date', 'type'],
                            key=lambda col: col.map(sort_key) if col.name == 'type' else col)
        df['date'] = df['date'].dt.strftime('%d-%m-%Y')
    elif tab_type=='rf':
        df['frequency'] = df['frequency'].str.extract(r'(\d+)').astype(int)
        df = df.sort_values(by=[col_name, 'frequency', 'type'],
                            key=lambda col: col.map(sort_key) if col.name == 'type' else col)
        df['frequency'] = df['frequency'].astype(str) + '+'
    else:
        df = df.sort_values(by=[col_name, 'type'],
                            key=lambda col: col.map(sort_key) if col.name == 'type' else col)

    return df


def _compute_contact_total_trp_raw(path_tables, label_object, campaign_par):
    """

    :param str path_tables: path of the directory which the json files are stored
    :param label_object:
    :param campaign_par:
    :return:
    """

    # Read table
    df_contact_sexage = _extract_df_from_json_file(path_tables, 'impacts_by_sex_age')
    df_universe_by_sexage = _extract_df_from_json_file(path_tables, 'universe_by_sex_age')
    df_universe_by_sexage = df_universe_by_sexage[df_universe_by_sexage['date'] == df_universe_by_sexage['date'].min()]

    col_to_group = ['broadcaster', 'device_type', 'ad_type']

    if df_contact_sexage.empty:
        df_contact_sexage[col_to_group + ['num_impacts']] = None

    # Group data
    df_contact_sexage = df_contact_sexage.groupby(col_to_group, as_index=False)['num_impacts'].sum()

    # Compute TRPs
    size_universe = df_universe_by_sexage['universe'].sum()
    df_contact_sexage['num_trp'] = df_contact_sexage['num_impacts'] / size_universe * 100
    df_contact_sexage = df_contact_sexage.drop(columns=['num_impacts'])

    # Scaffolding
    df_scaffolding = _scaffolding_contacts(col_to_group, label_object)

    df_contact_sexage = df_scaffolding.merge(df_contact_sexage, how='left', on=col_to_group)
    df_contact_sexage['num_trp'] = df_contact_sexage['num_trp'].fillna(0)

    df_contact_sexage = df_contact_sexage.set_index([x for x in df_contact_sexage.columns if x != 'num_trp'])

    df_contact_sexage['_total'] = df_contact_sexage.sum(axis=1)
    df_contact_sexage = df_contact_sexage.round(1)

    df_contact_sexage = df_contact_sexage.reset_index()

    # Add Custom labels
    _custom_labels(df_contact_sexage, label_object)

    # Format rows order
    df_contact_sexage = _format_rows(df_contact_sexage, label_object)
    df_contact_sexage = df_contact_sexage.drop_duplicates()

    # Format columns order
    col_to_use = ['Type', 'num_trp']
    df_contact_sexage = df_contact_sexage[col_to_use]

    df_contact_sexage = df_contact_sexage.rename(columns={'num_trp': 'Total'})

    return df_contact_sexage


def _compute_contact_total_trp_30eq(path_tables, label_object, campaign_par):
    """

    :param str path_tables: path of the directory which the json files are stored
    :param label_object:
    :param campaign_par:
    :return:
    """

    # Read table
    df_contact_sexage = _extract_df_from_json_file(path_tables, 'impacts_by_sex_age')
    df_universe_by_sexage = _extract_df_from_json_file(path_tables, 'universe_by_sex_age')
    df_universe_by_sexage = df_universe_by_sexage[df_universe_by_sexage['date'] == df_universe_by_sexage['date'].min()]

    col_to_group = ['broadcaster', 'device_type', 'ad_type']

    if df_contact_sexage.empty:
        df_contact_sexage[col_to_group + ['num_30sec_eq_impacts']] = None

    # Group data
    df_contact_sexage = df_contact_sexage.groupby(col_to_group, as_index=False)['num_30sec_eq_impacts'].sum()

    # Compute TRPs
    size_universe = df_universe_by_sexage['universe'].sum()
    df_contact_sexage['num_trp'] = df_contact_sexage['num_30sec_eq_impacts'] / size_universe * 100
    df_contact_sexage = df_contact_sexage.drop(columns=['num_30sec_eq_impacts'])

    # Scaffolding
    df_scaffolding = _scaffolding_contacts(col_to_group, label_object)

    df_contact_sexage = df_scaffolding.merge(df_contact_sexage, how='left', on=col_to_group)
    df_contact_sexage['num_trp'] = df_contact_sexage['num_trp'].fillna(0)

    df_contact_sexage = df_contact_sexage.set_index([x for x in df_contact_sexage.columns if x != 'num_trp'])

    df_contact_sexage['_total'] = df_contact_sexage.sum(axis=1)
    df_contact_sexage = df_contact_sexage.round(1)

    df_contact_sexage = df_contact_sexage.reset_index()

    # Add Custom labels
    _custom_labels(df_contact_sexage, label_object)

    # Format rows order
    df_contact_sexage = _format_rows(df_contact_sexage, label_object)
    df_contact_sexage = df_contact_sexage.drop_duplicates()

    # Format columns order
    col_to_use = ['Type', 'num_trp']
    df_contact_sexage = df_contact_sexage[col_to_use]

    df_contact_sexage = df_contact_sexage.rename(columns={'num_trp': 'Total'})

    return df_contact_sexage


def _drop_rows_if_no_impressions(df):
    # Sum all values in numerical columns and check if the sum is 0
    numerical_cols = df.select_dtypes(include=['number']).columns
    has_zero_sum_in_all_num_columns = df[numerical_cols].sum().eq(0).all()
    if has_zero_sum_in_all_num_columns:
        # Drop all rows, keeping only column headers
        df = df.drop(df.index)
    else:
        pass
    return df


def _get_sg_codes_no_impressions(df, sg_request, sg_no_impressions):
    # Consider children only
    processed_sg_codes = df[df['campaign level'] != 'overall campaign'][
        'campaign level'].drop_duplicates().tolist()

    # Spotgate codes with no impressions associated (all Excel tabs considered)
    sg_no_impressions_table = set(sg_request) - set(processed_sg_codes)

    if not sg_no_impressions_table.issubset(sg_no_impressions):
        sg_no_impressions.update(sg_no_impressions_table)

    return sg_no_impressions


def main_postprocess_request(path_tables, path_dir_output, element_config, label_attribute, label_object):
    """
    This function executes the post-processing functions found in the element_config.json file for producing the
    json files used as input to the graphic library.

    The python_element parsed from element_config.json file has the following naming convention:
    {webapp name}_{tab name}_{type of visualisation}_{kpi to show}_{aggregation level}_{first level metric}_{secoond level metric}
    standard_tabcontacts_table_contact_sexage_abs_raw

    Where:
    - webapp name: name of the specific type of the webapp
    - tab name: name of the specific tab of the webapp
    - type of visualisation: graphical element to display {"plot", "table"}
    - kpi to show: kpi to display {"contact", "contactcum", "contactdaily", "reach"}
    - aggregation level: type of aggregation to report in the plot {"sexage", "target"}
    - first level metric: metric to display {"abs", "trp", "perc"}
    - second level metric: additional metric to display (only for contact) {"raw", "30eq"}

    :param str path_tables: Path of the input directory where the json files from the API are stored.
    :param str path_dir_output: Path of the output directory where the json files will be stored.
    :param str element_config: Path of the element_config.json file used to retrieve the functions to launch.
    :param str label_attribute: Path of the label_attribute.json file used to associate specific metadata
        to each label of the graphical label.
    :param str label_object: Path of the label_object.json file used to handle label renaming and ordering.
    """

    # Read file containing the elements to run
    with open(element_config) as f:
        element_config = json.load(f)

    # Read file containing the metadata associated to each label in the element
    with open(label_attribute) as f:
        label_attribute = json.load(f)

    # Read file containing the mapping table for the replacement
    with open(label_object) as f:
        label_object = json.load(f)

    # Read raw request
    with open(os.path.join(path_tables, 'json_request.json')) as f:
        json_request = json.load(f)

    # Read universe table to understand the real period of the campaign
    with open(os.path.join(path_tables, 'target_universe.json')) as f:
        universe_json = json.load(f)

    # Add target name A3+ if not present in the request
    target_name = [x['name_target'] for x in json_request['target']]
    if 'A3+' not in target_name:
        target_name = target_name + ['A3+']

    # Compute the min and max date of the period of the campaign
    min_date = min([pd.to_datetime(x['date']) for x in universe_json['report_table']])
    max_date = max([pd.to_datetime(x['date']) for x in universe_json['report_table']])

    # Create a dataframe containing all the dates from min_date to max_date
    range_date = pd.date_range(min_date, max_date).tolist()
    df_date_range = pd.DataFrame(range_date, columns=['date'])

    # Create a dataframe containing all the period (combination of period_start and period_end)
    # from min_date to max_date
    data_map = {
        'start_date': [min_date] * len(range_date),
        'end_date': list(range_date)
    }
    df_period_range = pd.DataFrame(data_map)

    # Add the parameters of the campaign in the object to read
    campaign_par = {
        "target_name": target_name,
        "df_date_range": df_date_range,
        "df_period_range": df_period_range,
        "max_freq": 20
    }

    this_mod = sys.modules[__name__]

    # For each python_element found in element_config.json run the appropriate python function
    # If the python function is not present in the module methodcaller will raise an error
    # TODO: read the tables one time and send to each functions through campaign_par
    for element_obj in element_config.values():
        element_name = element_obj['python_element']
        path_output_json = os.path.join(path_dir_output, element_obj['file_name'])
        label_attribute_element = label_attribute[element_name]
        c = methodcaller(element_obj['python_function'],
                         path_tables, path_output_json, label_object,
                         label_attribute_element, campaign_par, element_obj)(this_mod)


def main_generator_file(path_tables, path_dir_output, export_file, label_object):
    """
      This function executes the functions found in the exportfile_config.json file for producing the files (ie excel,...)

      The python_element parsed from element_config.json file has the following naming convention:
      standard_resultexcel
      {webapp name}_{single element to generate}

      Where:
      - webapp name: name of the specific type of the webapp
      - single element to generate: name of file object to generate

      :param str path_tables: Path of the input directory where the json files from the API are stored.
      :param str path_dir_output: Path of the output directory where the file will be stored.
      :param str export_file: Path of the exportfile_config.json file used to retrieve the functions to launch.
      :param str label_object: Path of the label_object.json file used to handle label renaming and ordering.
    """

    # Read file containing the elements to run
    with open(export_file) as f:
        export_file = json.load(f)

    # Read file containing the mapping table for the replacement
    with open(label_object) as f:
        label_object = json.load(f)

    # Read raw request
    with open(os.path.join(path_tables, 'json_request.json')) as f:
        json_request = json.load(f)

    # Extract Warning
    with open(os.path.join(path_tables, 'target_universe.json')) as f:
        supp_file = json.load(f)
        warning_attr = supp_file['warning'] if 'warning' in supp_file else None

    # Add target name A3+ if not present in the request
    target_name = [x['name_target'] for x in json_request['target']]
    if 'A3+' not in target_name:
        target_name = target_name + ['A3+']

    # Compute the min and max date of the period of the campaign
    min_date = min([pd.to_datetime(x['period_start']) for x in json_request['sg_code']])
    max_date = max([pd.to_datetime(x['period_end']) for x in json_request['sg_code']])

    # Create a dataframe containing all the dates from min_date to max_date
    range_date = pd.date_range(min_date, max_date).tolist()
    df_date_range = pd.DataFrame(range_date, columns=['date'])

    # Create a dataframe containing all the period (combination of period_start and period_end)
    # from min_date to max_date
    data_map = {
        'start_date': [min_date] * len(range_date),
        'end_date': list(range_date)
    }
    df_period_range = pd.DataFrame(data_map)

    # Add the parameters of the campaign in the object to read
    campaign_par = {
        "target_name": target_name,
        "df_date_range": df_date_range,
        "df_period_range": df_period_range,
        "max_freq": 20,
        "name_campaign": json_request['name_campaign'],
        'json_request': json_request,
        'warning_desc': warning_attr
    }

    this_mod = sys.modules[__name__]

    # For each element found in exportfile_config.json run the appropriate python function
    # If the python function is not present in the module methodcaller will raise an error
    for element_obj in export_file.values():
        c = methodcaller(element_obj['python_function'],
                         path_tables, path_dir_output, label_object, campaign_par)(this_mod)


def main_generator_zip_json_report_to_download(path_tables, path_dir_output):
    """
      This function creates into the folder a zip file named raw_json_tables.zip contained the json table in output of the ApPI and the json request

      :param str path_tables: Path of the input directory where the json files from the API are stored.
      :param str path_dir_output: Path of the output directory where the file will be stored.
    """
    children_request = _extract_child_id(path_tables)

    # Read file containing the elements to run
    file_to_zip = ['impacts_by_sex_age.json',
                   'impacts_in_target.json',
                   'json_request.json',
                   'rf_in_target_overall.json',
                   'r1plus_in_target_buildup.json',
                   'target_universe.json',
                   'tv_spot_schedule.json',
                   'universe_by_sex_age.json']

    zip_name = os.path.join(path_dir_output, 'raw_json_tables.zip')

    _zip_files(path_tables, zip_name, file_to_zip, children_request)
