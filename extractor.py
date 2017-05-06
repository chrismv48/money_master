"""Extract financial information from the Plaid API"""
import csv
import json
from datetime import date
from collections import OrderedDict, defaultdict, Counter
from openpyxl import load_workbook
from plaid import Client

with open('config.json') as f:
    CONFIG = json.load(f)

plaid_client = Client(client_id=CONFIG['PLAID_CLIENT_ID'],
                      secret=CONFIG['PLAID_SECRET_ID'],
                      public_key=CONFIG['PLAID_PUBLIC_KEY'],
                      environment="development",
                      )


def load_existing_transactions():
    wb = load_workbook(CONFIG["MONEY_MASTER_EXCEL_PATH"])
    existing_transactions_raw = wb.get_sheet_by_name('Chase Transactions').rows
    header_row = [cell.value for cell in next(existing_transactions_raw)]
    existing_transactions = []
    for row in existing_transactions_raw:
        row_dict = OrderedDict()
        for i, cell in enumerate(row):
            row_dict[header_row[i]] = cell.value
        existing_transactions.append(row_dict)

    return existing_transactions


def group_transactions_by_category(transactions):
    """
    Group transactions by description and then by category. This will allow us to auto-categorize new transactions
    using the most commonly occurring description/category tuple.
    :param transactions:
    :return:
    """
    category_dict = defaultdict(Counter)
    for transaction in transactions:
        if transaction['category']:
            category_dict[transaction['description']][transaction['category']] += 1

    return category_dict


def query_plaid_transactions(plaid_client, start_date, end_date=None):
    """
    To ensure we don't pull duplicate transactions, we provide the gte date as the max date + 1 of the master
    dataset. I've seen situations where Plaid retroactively returns duplicate transactions with different plaid
    transaction id's, so we do this while also filtering out duplicate id's.
    :return:
    """
    if not end_date:
        end_date = date.today().strftime("%Y-%m-%d")

    #TODO: build in pagination in case transactions is > 500 by using the offset param.
    response = plaid_client.Transactions.get(CONFIG["PLAID_ACCESS_TOKEN"],
                                             start_date,
                                             end_date,
                                             count=500)
    return response


def build_account_details(account_response):
    # Get account information to add the account name and plaid_account_id etc to each transaction row
    account_dict = {}
    for account in account_response:
        account_id = account.get('account_id')
        account_dict[account_id] = {}
        account_dict[account_id]['bank_account_number'] = account.get('mask')
        account_dict[account_id]['account_name'] = CONFIG["BANK_NAME_MAPPING"].get(account.get('mask'))
        account_dict[account_id]['institution_type'] = account.get('type')
        account_dict[account_id]['account_type'] = account.get('type')
        account_dict[account_id]['account_subtype'] = account.get('subtype')

    return account_dict


def merge_transactions(existing_transactions, new_transactions, account_data):
    existing_transaction_ids = {row.get('transaction_id') for row in existing_transactions}
    merged_transactions = list(existing_transactions)

    for transaction in new_transactions:
        # Skip transactions already existing in Money Master.xlsx
        if transaction.get('transaction_id') in existing_transaction_ids:
            continue
        # Skip transactions from Kara's business account
        transaction_dict = OrderedDict()
        transaction_dict.update(account_data[transaction.get('account_id')])
        if transaction_dict.get('bank_account_number') == '7550':
            continue
        transaction_dict['date'] = transaction.get('date')
        transaction_dict['description'] = transaction.get('name')
        transaction_dict['amount'] = transaction.get('amount')
        transaction_dict['plaid_category'] = (', ').join(transaction.get('category') or [])
        transaction_dict['transaction_type'] = transaction.get('transaction_type')
        transaction_dict['address'] = transaction.get('location', {}).get('address', {})
        transaction_dict['city'] = transaction.get('location', {}).get('city')
        transaction_dict['state'] = transaction.get('location', {}).get('state')
        transaction_dict['zip'] = transaction.get('location', {}).get('zip')
        transaction_dict['country'] = transaction.get('location', {}).get('country')
        transaction_dict['pending'] = transaction.get('pending')
        transaction_dict['transaction_id'] = transaction.get('transaction_id')

        merged_transactions.append(transaction_dict)

    return merged_transactions


def apply_transaction_categories(transactions, category_data):
    for transaction in transactions:
        if not transaction.get('category'):
            transaction['category'] = category_data.get(transaction['description']).most_common()[0][0] if \
                category_data.get(transaction['description']) else None

    return transactions


def save_results_to_csv(transactions, csv_filename=CONFIG["RAW_DATA_CSV_FILENAME"]):
    assert len(transactions[0].keys()) == len(CONFIG["FIELDNAMES"])
    with open(csv_filename, 'w') as f:
        writer = csv.DictWriter(f, fieldnames=CONFIG["FIELDNAMES"], lineterminator='\n')
        writer.writeheader()
        writer.writerows(transactions)


if __name__ == '__main__':
    existing_transactions = load_existing_transactions()
    category_data = group_transactions_by_category(existing_transactions)

    start_date = max(existing_transactions, key=lambda x: x['date'])['date'].strftime("%Y-%m-%d")
    plaid_transactions_response = query_plaid_transactions(plaid_client, start_date=start_date)

    plaid_transactions = plaid_transactions_response['transactions']
    plaid_accounts = plaid_transactions_response['accounts']

    account_data = build_account_details(plaid_accounts)
    merged_transactions = merge_transactions(existing_transactions, plaid_transactions, account_data)
    merged_transactions = apply_transaction_categories(merged_transactions, category_data)

    save_results_to_csv(merged_transactions)
