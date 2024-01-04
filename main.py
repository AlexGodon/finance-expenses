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
        for person, keywords in person_configs.items():
            if any(keyword in description for keyword in keywords):
                df.at[index, 'Person'] = person.capitalize()
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
    for format in ['%d-%b-%y', '%Y%m%d', '%m/%d/%Y', '%Y-%m-%d']:
        try:
            df[date_column] = pd.to_datetime(df[date_column], format=format)
        except:
            pass
    df[date_column] = df[date_column].dt.strftime('%Y-%m-%d')
    return df


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
                         'Description': 'Alayacare-insurance', 'Trans Amount': 70.00, 'file_name': 'extra.csv'}])
bank_account_df = pd.concat([bank_account_df, new_row], ignore_index=True)

# Make sure directory where the files generated will be dropped / created.
os.makedirs(destination_directory, exist_ok=True)

# Create a new calculation of the summary of all the data.
generate_summary_csv(credit_card_df,
                     os.path.join(destination_directory, 'credit_card_results.csv'))
generate_summary_csv(bank_account_df,
                     os.path.join(destination_directory, 'bank_account_results.csv'))
