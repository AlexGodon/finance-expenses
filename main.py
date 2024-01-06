import pandas as pd
from datetime import datetime, timedelta
import yaml
import os


def generate_summary_csv(df, full_path_new_file):
    full_path_category_mapping = 'config/category_mappings.yml'


    with open(full_path_category_mapping, 'r') as f:
        configs = yaml.safe_load(f)

    category_configs = configs['category']
    person_configs = configs['person']

    # Add a new columns
    df['Category'] = None
    df['Type'] = None
    df['Person'] = 'Both'

    # Iterate through DataFrame
    for index, row in df.iterrows():
        description = row['Description'].lower().replace(' ', '-')

        # Check for each category
        assigned = False
        for category, keywords in category_configs.items():
            if any(keyword in description for keyword in keywords):
                df.at[index, 'Category'] = category.capitalize()
                assigned = True
                break  # Assuming one category per row
        # If no category is assigned, you might want to label it as 'Uncategorized' or similar
        if not assigned:
            df.at[index, 'Category'] = 'Uncategorized'

        df.at[index, 'Type'] = 'Expense'
        if int(row['Trans Amount']) <= 0:
            df.at[index, 'Type'] = 'Credit'

        # Check for each person
        for person, conditions in person_configs.items():
            keywords = conditions['keywords']
            if 'all_bmo' in keywords and 'bmo' in df.at[index, 'file_name']:
                df.at[index, 'Person'] = person.capitalize()
            elif 'all_nbc' in keywords and 'nbc' in df.at[index, 'file_name']:
                df.at[index, 'Person'] = person.capitalize()
            else:
                for keyword in keywords:
                    if keyword in description:
                        amounts = None
                        if conditions.get('amounts'):  # safety net, incase amount not defined.
                            amounts = conditions['amounts'].get(keyword)

                        if amounts:
                            if df.at[index, 'Trans Amount'] in amounts:
                                df.at[index, 'Person'] = person.capitalize()
                                break
                        else:
                            df.at[index, 'Person'] = person.capitalize()
                            break

            if df.at[index, 'Person'] != 'Both':
                break  # Assuming one person per row, both is set by default

    df.to_csv(full_path_new_file, index=False)

def read_file(file_path):
    column_names = []
    inverse_amount = False
    file_type = None
    skiprows = None
    separator = ','
    col_types = {'Expense': str, 'Credit': str}  # Define the data types

    if 'cibc' in file_path.lower():  # couple bank
        column_names = ["Transaction Date", "Description", "Expense", "Credit", "Balance"]
        skiprows = 1  # skip header, replaced by above names
        file_type = 'bank_account'
    elif 'bmo' in file_path.lower():
        column_names = ["First Bank Card", "Transaction Type", "Transaction Date", "Trans Amount", "Description"]
        skiprows = 6  # data on the csv file only start at row 7, skip first 6 rows.
        file_type = 'bank_account'
    elif 'nbc' in file_path.lower():  # national bank of canada
        column_names = ["Transaction Date", "Description", "Category", "Expense", "Credit", "Balance"]
        skiprows = 1  # skip header, replaced by above names
        separator = ';'
        file_type = 'bank_account'
    elif 'amex' in file_path.lower():  # scotia bank amex
        column_names = ["Transaction Date", "Description", "Trans Amount"]
        file_type = 'credit_card'
        inverse_amount = True
    elif 'visa' in file_path.lower():  # bmo visa
        column_names = ["Item #", "Card #", "Transaction Date", "Posting Date", "Trans Amount", "Description"]
        skiprows = 3  # data on the csv file only start at row 4, skip first 3 rows.
        file_type = 'credit_card'

    # Load file into DataFrame
    df = pd.read_csv(file_path, sep=separator, engine='python', names=column_names,
                     skip_blank_lines=True, skiprows=skiprows)

    # Cleaning and processing DataFrame
    df = df.dropna(how='all')  # Drop rows where all elements are NaN
    df.columns = df.columns.str.strip()  # Strip whitespace from column names
    # This is a safety to make sure, 'Expense' and 'Credit' are strings, converted later to int.
    for col in col_types.keys():
        if col in df.columns:
            df[col] = df[col].astype(col_types[col])

    if inverse_amount:
        df['Trans Amount'] = df['Trans Amount'] * -1

    return df, file_type


def standardize_date_format(df, date_column):
    # Convert start and end dates to datetime
    start_date = pd.to_datetime('2023-12-01')
    end_date = pd.to_datetime('2023-12-31')
    # Convert date_column to datetime, trying different formats
    for format in ['%d-%b-%y', '%Y%m%d', '%m/%d/%Y', '%Y-%m-%d']:
        try:
            df[date_column] = pd.to_datetime(df[date_column], format=format)
            break  # Exit the loop if conversion is successful
        except:
            pass

    # Filter the DataFrame and create an explicit copy
    filtered_df = df[(df[date_column] >= start_date) & (df[date_column] <= end_date)].copy()
    # Format the date column
    filtered_df[date_column] = filtered_df[date_column].dt.strftime('%Y-%m-%d')

    return filtered_df


# Define a function to insert subtotals within the DataFrame based on file_name changes
def insert_subtotals(df, group_by_column, subtotal_column):
    # Sort the DataFrame by the column we want to group by to ensure subtotals insert correctly
    df_sorted = df.sort_values(by=group_by_column).reset_index(drop=True)
    # Add a row for each change in file_name
    # Create a new column to detect changes in 'file_name'
    df_sorted['file_name_change'] = df_sorted[group_by_column].ne(df_sorted[group_by_column].shift())

    # Filter to get the indexes where change occurred
    change_indexes = df_sorted[df_sorted['file_name_change']].index
    iterations_cnt = len(change_indexes)

    # Create new rows to be inserted at the change_indexes
    new_rows = [{'Transaction Date': None, 'Description': 'Subtotal', 'Trans Amount': None,
                 'file_name': ''}] * iterations_cnt

    new_df = pd.DataFrame()
    last_index = df_sorted.index[-1]
    current_iteration = 0
    # Insert the new rows into the DataFrame
    for idx, new_row in zip(change_indexes, new_rows):
        idx += current_iteration
        if current_iteration == iterations_cnt - 1:  # Last iteration, -1 as iteration starts at 0
            next_idx = last_index + current_iteration
        else:
            next_idx = change_indexes[current_iteration+1] + current_iteration
            # -1 to have 1 row above next file_name change
            next_idx -= 1

        sum_trans_amount = df_sorted.loc[idx:next_idx, subtotal_column].sum()

        new_row[subtotal_column] = round(sum_trans_amount, 2)
        new_row['Description'] = 'Subtotal {}'.format(df_sorted.at[idx, group_by_column])
        df_sorted = (pd.concat([df_sorted.iloc[:next_idx+1], pd.DataFrame([new_row]), df_sorted.iloc[next_idx+1:]])
                     .reset_index(drop=True))

        current_iteration += 1

    # Drop the 'file_name_change' as it is no longer needed
    df_sorted.drop(columns=['file_name_change'], inplace=True)

    return df_sorted


def retain_specific_columns(df, file_type):
    # Specific columns to retain
    if file_type == 'bank_account':
        desired_columns = ["Transaction Date", "Description", "Trans Amount"]
    else:  # credit card
        desired_columns = ["Transaction Date", "Description", "Trans Amount"]

    # Filter the DataFrame to retain only the specified columns
    return df[desired_columns]


def process_chequing_transactions(df, file_type):
    if file_type == 'bank_account':
        if 'Expense' in df.columns and 'Credit' in df.columns:
            # Convert 'Expense' and 'Credit' columns to numeric, handling non-numeric entries
            df['Expense'] = pd.to_numeric(df['Expense'].str.replace('$', '').str.replace(',', '').str.replace('"', '').
                                          replace('Not applicable', '0'), errors='coerce').fillna(0)
            df['Credit'] = pd.to_numeric(df['Credit'].str.replace('$', '').str.replace(',', '').str.replace('"', '').
                                         replace('Not applicable', '0'), errors='coerce').fillna(0)

            # Subtract Expense from Credit to get the Trans Amount
            df['Trans Amount'] = df['Expense'] - df['Credit']
        else:  # bmo
            df['Trans Amount'] = df['Trans Amount'] * -1

    return df


def process_file(file_path):
    df, file_type = read_file(file_path)
    df = standardize_date_format(df, "Transaction Date")  # Assuming the first column is the date
    df = process_chequing_transactions(df, file_type)
    df = retain_specific_columns(df, file_type)
    df['file_name'] = os.path.basename(file_path)
    return df, file_type


def list_files_in_directory(directory):
    """
    List all files in the given directory and returns their paths.
    """
    file_paths = []
    for file in os.listdir(directory):
        if os.path.isfile(os.path.join(directory, file)) and file.endswith('.csv'):
            file_paths.append(os.path.join(directory, file))
    file_paths.sort()
    return file_paths


# Example usage
# Assuming the directory name is 'source_files'
source_directory = 'source_files'
destination_directory = 'dest_files'
file_paths = list_files_in_directory(source_directory)
all_credit_card_dfs = []
all_bank_account_dfs = []

for file_path in file_paths:
    df, file_type = process_file(file_path)
    if file_type == 'credit_card':
        all_credit_card_dfs.append(df)
    else:
        all_bank_account_dfs.append(df)

# Combining DataFrames
credit_card_df = pd.concat(all_credit_card_dfs, ignore_index=True)
bank_account_df = pd.concat(all_bank_account_dfs, ignore_index=True)

# Add Extra transactions not seen in bank accounts
first_day_previous_month = (datetime.today().replace(day=1) - timedelta(days=1)).replace(day=1).date()
new_row = pd.DataFrame([{'Transaction Date': first_day_previous_month,
                         'Description': 'Alayacare-insurance', 'Trans Amount': 70.00, 'file_name': 'cibc.csv'}])
bank_account_df = pd.concat([bank_account_df, new_row], ignore_index=True)

# Make sure directory where the files generated will be dropped / created.
os.makedirs(destination_directory, exist_ok=True)

# Add subtotals to each file_name changes
credit_card_df = insert_subtotals(credit_card_df, 'file_name', 'Trans Amount')
bank_account_df = insert_subtotals(bank_account_df, 'file_name', 'Trans Amount')

# Create a new calculation of the summary of all the data.
generate_summary_csv(credit_card_df,
                     os.path.join(destination_directory, 'credit_card_results.csv'))
generate_summary_csv(bank_account_df,
                     os.path.join(destination_directory, 'bank_account_results.csv'))
