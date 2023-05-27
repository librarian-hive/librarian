import pymssql
import streamlit as st
import datetime as dt 
import pandas as pd 
import numpy as np
from PIL import Image
from styles import style
import os

def hive_sql(SQLCommand, limit):
    db = os.environ['HIVESQL'].split()
    conn = pymssql.connect(server=db[0], user=db[1], password=db[2], database=db[3])
    cursor = conn.cursor()
    cursor.execute(SQLCommand)
    result = cursor.fetchmany(limit)
    conn.close()
    return result

def get_conversations(user1, user2):
    limit = 1000
    SQLCommand = '''
        SELECT body, 'https://hive.blog' + url, created
        FROM Comments
        WHERE author = '{}' 
        AND parent_author =  '{}'
        ORDER BY created DESC
    '''.format(user2, user1)
    result = hive_sql(SQLCommand, limit)
    count = 1
    total_res = len(result)
    total = '<div class="total_comments"> Total of {} responses from @{} to @{} on Hive Blockchain </div>'.format(str(total_res), user2, user1)
    st.markdown(total, unsafe_allow_html=True)
    for i in result:
        starting = '<div class = "comment">'
        count_string = '<span class = "count_string">' + str(count) + ' - ' + '</span>'
        users = '@' + user2 + ' to @' + user1 + ' on '
        date_string = '<span class = "date_string">' + str(i[2]) + '</span>'
        link_string = '   <a class = " link_string" href = "'+ i[1] +'"> hive.blog</a>'
        ending = '</div>'
        comment = starting + count_string + users+ date_string + link_string + ending
        txt = comment + '\n\n' + i[0]
        st.markdown(txt, unsafe_allow_html=True)
        count += 1

def get_rich_list(topss, asset):
    hive_per_vest = get_hive_per_vest()
    asset_dict = {'Owned HP': 'vesting_shares', 'Delegated HP': 'delegated_vesting_shares', 'Received HP': 'received_vesting_shares', 
                    'Total HP':'(vesting_shares - delegated_vesting_shares + received_vesting_shares)', 'Hive': 'balance',
                     'HBD': 'hbd_balance', 'Hive-Savings': 'savings_balance', 'HBD-Savings': 'savings_hbd_balance'}
    tops = int(topss[4:])
    SQLCommand = '''
    SELECT name, 
            CAST(ROUND(vesting_shares * {0},2,0) AS NUMERIC(10,2)),
            CAST(ROUND(delegated_vesting_shares * {0},2,0) AS NUMERIC(10,2)) * -1,
            CAST(ROUND(received_vesting_shares * {0},2,0) AS NUMERIC(10,2)),
            CAST(ROUND(vesting_shares * {0},2,0) AS NUMERIC(10,2)) - CAST(ROUND(delegated_vesting_shares * {0},2,0) AS NUMERIC(10,2)) + CAST(ROUND(received_vesting_shares * {0},2,0) AS NUMERIC(10,2)),
            balance, hbd_balance, savings_balance, savings_hbd_balance
    FROM Accounts
    ORDER BY {1} DESC;
    '''.format(hive_per_vest, asset_dict[asset])
    result = hive_sql(SQLCommand, tops)
    df = pd.DataFrame(result)
    df.rename(columns = {0: "Name", 1: "Owned HP", 2: "Delegated HP", 3: "Received HP", 4: "Total HP", 5: 'Hive', 6: 'HBD', 7: 'Hive Savings', 8: 'HBD Savings'}, inplace=True)
    df.index = df.index + 1
    text = '<h1>{} Hive Rich List Sorted by {}</h1>'.format(topss, asset)
    st.markdown(text, unsafe_allow_html=True)
    st.table(df)

def get_hive_per_vest():
    SQLCommand = '''
    SELECT hive_per_vest
    FROM DynamicGlobalProperties
    '''
    result = hive_sql(SQLCommand, 1)
    return result[0][0]

def get_delegations_all(delegator):
    hive_per_vest = get_hive_per_vest()
    limit = 1000
    SQLCommand = '''
    SELECT delegator, delegatee, vesting_shares, CAST(ROUND(vesting_shares * {},2,0) AS NUMERIC(10,2)), timestamp
    FROM TxDelegateVestingShares
    WHERE delegator = '{}'
    ORDER BY timestamp DESC
    '''.format(hive_per_vest, delegator)
    result = hive_sql(SQLCommand, limit)
    df = pd.DataFrame(result)
    txt = '<h1>All past and current delegations by @{}.</h1>'.format(delegator)
    st.markdown(txt, unsafe_allow_html=True)
    df.rename(columns = {0:'Delegator', 1:'Delegatee', 2:'Vesting Shares', 3:'Hive Power', 4:'Timestamp'}, inplace=True)
    df.index = df.index + 1
    st.table(df)

    SQLCommand = '''
    SELECT delegator, delegatee, vesting_shares, CAST(ROUND(vesting_shares * {},2,0) AS NUMERIC(10,2)), timestamp
    FROM TxDelegateVestingShares
    WHERE delegatee = '{}'
    ORDER BY timestamp DESC
    '''.format(hive_per_vest, delegator)
    result = hive_sql(SQLCommand, limit)
    df2 = pd.DataFrame(result)
    txt = '<h1>All past and current delegations to @{}.</h1>'.format(delegator)
    st.markdown(txt, unsafe_allow_html=True)
    df2.rename(columns = {0:'Delegator', 1:'Delegatee', 2:'Vesting Shares', 3: 'Hive Power' ,4:'Timestamp'}, inplace=True)
    df2.index = df2.index + 1
    st.table(df2)

def get_delegations_active(delegator):
    hive_per_vest = get_hive_per_vest()
    limit = 1000
    SQLCommand = '''
    SELECT TxDelegateVestingShares.delegator,
            TxDelegateVestingShares.delegatee,
            TxDelegateVestingShares.vesting_shares,
            CAST(ROUND(TxDelegateVestingShares.vesting_shares * {},2,0) AS NUMERIC(10,2)),
            TxDelegateVestingShares.timestamp
    FROM TxDelegateVestingShares,
    (SELECT delegatee, MAX(timestamp) as new_timestamp
    FROM TxDelegateVestingShares
    WHERE delegator = '{}'
    GROUP BY delegatee) test
    WHERE TxDelegateVestingShares.delegatee = test.delegatee
    AND TxDelegateVestingShares.timestamp = test.new_timestamp
    AND TxDelegateVestingShares.vesting_shares > 0
    ORDER BY timestamp DESC;
    '''.format(hive_per_vest, delegator)
    result = hive_sql(SQLCommand, limit)
    if len(result) > 0:
        df = pd.DataFrame(result)
        txt = '<h1>Active delegations by @{}.</h1>'.format(delegator)
        st.markdown(txt, unsafe_allow_html=True)
        df.rename(columns = {0:'Delegator', 1:'Delegatee', 2:'Vesting Shares', 3:'Hive Power', 4:'Timestamp'}, inplace=True)
        df.index = df.index + 1
        total_delegations_hp = df['Hive Power'].sum()
        total_delegations_count = df['Hive Power'].count()
        st.table(df)
        txt = "<p>@{} is currently delegating total of {} Hive Power to {} accounts.</p>".format(delegator, total_delegations_hp, total_delegations_count)
        st.markdown(txt, unsafe_allow_html=True)
    else:
        st.markdown('<p>@{} currently has no active delegations.</p>'.format(delegator), unsafe_allow_html=True)
    
    SQLCommand = '''
    SELECT TxDelegateVestingShares.delegator,
            TxDelegateVestingShares.delegatee,
            TxDelegateVestingShares.vesting_shares,
            CAST(ROUND(TxDelegateVestingShares.vesting_shares * {},2,0) AS NUMERIC(10,2)),
            TxDelegateVestingShares.timestamp
    FROM TxDelegateVestingShares,
    (SELECT delegator, MAX(timestamp) as new_timestamp
    FROM TxDelegateVestingShares
    WHERE delegatee = '{}'
    GROUP BY delegator) test
    WHERE TxDelegateVestingShares.delegator = test.delegator
    AND TxDelegateVestingShares.delegatee = '{}'
    AND TxDelegateVestingShares.timestamp = test.new_timestamp
    AND TxDelegateVestingShares.vesting_shares > 0
    ORDER BY timestamp DESC;
    '''.format(hive_per_vest, delegator, delegator)
    result = hive_sql(SQLCommand, limit)

    if len(result) > 0:
        df = pd.DataFrame(result)
        txt = '<h1>Active delegations to @{}.</h1>'.format(delegator)
        st.markdown(txt, unsafe_allow_html=True)
        df.rename(columns = {0:'Delegator', 1:'Delegatee', 2:'Vesting Shares', 3:'Hive Power', 4:'Timestamp'}, inplace=True)
        df.index = df.index + 1
        total_delegations_hp = df['Hive Power'].sum()
        total_delegations_count = df['Hive Power'].count()
        st.table(df)
        txt = "<p>@{} is receiving total of {} Hive Power delegations from {} accounts.</p>".format(delegator, total_delegations_hp, total_delegations_count)
        st.markdown(txt, unsafe_allow_html=True)
    else:
        st.markdown('<p>@{} is currently not receiving any active delegations.</p>'.format(delegator), unsafe_allow_html=True)

def get_dgp():
    limit = 100
    SQLCommand = '''
    SELECT COLUMN_NAME
    FROM INFORMATION_SCHEMA.COLUMNS
    WHERE TABLE_NAME = 'DynamicGlobalProperties'
    '''
    result = hive_sql(SQLCommand, limit)
    col_names = []
    for col_name in result:
        col_names.append(col_name[0])
    SQLCommand = '''
    SELECT *
    FROM DynamicGlobalProperties
    '''
    result = hive_sql(SQLCommand, limit)
    col_values = []
    for col_value in result[0]:
        col_values.append(col_value)
    dgp = dict(zip(col_names, col_values))

    header_row = ['Count','Dynamic Global Properties', 'Values']

    def make_table(rows, header_row):
        table = '<table>'
        th = '<tr>'
        for row in header_row:
            r = '<th>{}</th>'.format(row)
            th += r
        th += '</tr>'
        table += th
        count = 1
        data = ''
        for name, value in rows.items():
            td = '<tr><td>{}</td><td>{}</td><td>{}</td></tr>'.format(count, name, value)
            count +=1
            data += td
        table += data
        table += '</table>'
        return table

    table = make_table(dgp, header_row)
    st.markdown('Hive Blockchain Dynamic Global Properties')
    st.markdown(table, unsafe_allow_html=True)



def get_hive_claimed(acct, start, end, order_by):
    hive_per_vest = get_hive_per_vest()
    limit = 1000000
    SQLCommand = f'''
    SELECT reward_hive, timestamp
    FROM TxClaimRewardBalances
    WHERE account = '{acct}'
    AND reward_hive > 0
    AND timestamp BETWEEN '{start}' AND '{end}'
    ORDER BY timestamp {order_by}
    '''
    result = hive_sql(SQLCommand, limit)
    df = pd.DataFrame(result)
    df.rename(columns = {0: "Claimed Hive", 1: "Date"}, inplace=True)
    df.index = df.index + 1
    text = f'<h1>List of Hive claims by @{acct}</h1>'
    st.markdown(text, unsafe_allow_html=True)
    st.table(df)

 


def get_hbd_claimed(acct, start, end, order_by):
    hive_per_vest = get_hive_per_vest()
    limit = 1000000
    SQLCommand = f'''
    SELECT reward_hbd, timestamp
    FROM TxClaimRewardBalances
    WHERE account = '{acct}'
    AND reward_hbd > 0
    AND timestamp BETWEEN '{start}' AND '{end}'
    ORDER BY timestamp {order_by}
    '''
    result = hive_sql(SQLCommand, limit)
    df = pd.DataFrame(result)
    df.rename(columns = {0: "Claimed HBD", 1: "Date"}, inplace=True)
    df.index = df.index + 1
    text = f'<h1>List of HBD claims by @{acct}</h1>'
    st.markdown(text, unsafe_allow_html=True)
    st.table(df)


def get_vests_claimed(acct, start, end, order_by):
    hive_per_vest = get_hive_per_vest()
    limit = 1000000
    SQLCommand = f'''
    SELECT CAST(ROUND(reward_vests * {hive_per_vest},2,0) AS NUMERIC(10,2)), timestamp
    FROM TxClaimRewardBalances
    WHERE account = '{acct}'
    AND reward_vests > 0
    AND timestamp BETWEEN '{start}' AND '{end}'
    ORDER BY timestamp {order_by}
    '''
    result = hive_sql(SQLCommand, limit)
    df = pd.DataFrame(result)
    df.rename(columns = {0: "Claimed HP", 1: "Date"}, inplace=True)
    df.index = df.index + 1
    text = f'<h1>List of Hive Power claims by @{acct}</h1>'
    st.markdown(text, unsafe_allow_html=True)
    st.table(df)


def get_hbd_interest(acct, start, end, order_by):
    limit = 1000
    SQLCommand = f'''
    select interest, timestamp
    from VOInterests
    where owner = '{acct}'
    and timestamp between '{start}' and '{end}'
    order by timestamp {order_by}
    '''
    result = hive_sql(SQLCommand, limit)
    df = pd.DataFrame(result)
    df.rename(columns = {0: "HBD Interest Payments", 1: "Date"}, inplace=True)
    df.index = df.index + 1
    text = f'<h1>List of received HBD payments by @{acct}</h1>'
    st.markdown(text, unsafe_allow_html=True)
    st.table(df)

def get_curation_rewards(acct, start, end, order_by):
    hive_per_vest = get_hive_per_vest()
    limit = 1000000

    SQLCommand = f'''
    select CAST(ROUND(reward * {hive_per_vest},2,0) AS NUMERIC(10,2)), timestamp
    from VOCurationRewards 
    where curator = '{acct}' 
    and timestamp between '{start}' and '{end}'
    order by timestamp {order_by}
    '''

    result = hive_sql(SQLCommand, limit)
    df = pd.DataFrame(result)
    df.rename(columns = {0: "Curation Rewards", 1: "Date"}, inplace=True)
    df.index = df.index + 1
    text = f'<h1>List of curation rewards by @{acct}</h1>'
    st.markdown(text, unsafe_allow_html=True)
    st.table(df)


if __name__ == '__main__':
    #Initial page configuration
    st.set_page_config(
        page_title="Librarian",
        page_icon="hive_logo.png",
        layout="wide",
        initial_sidebar_state="expanded",
    )
    #Display Sidebar Logo
    logo = Image.open('lib_tr.png')
    st.sidebar.image(logo, width=300)
    #Sidebar Labels and Inputs
    conversations = st.sidebar.checkbox('Conversations', value=False)
    if conversations:
        u2 = st.sidebar.empty()
        u1 = st.sidebar.empty()
        user2 = u2.text_input('User-1:','ned')
        user1 = u1.text_input('User-2:','dan')
        get_conversations_button = st.sidebar.button('Get Conversations')
        conversations_info = '''<p>Result will output all historic responses by Hive User-1 to Hive User-2.</p> 
                                <p></p>
                                <p>If User-2 left blank the output will display contents of all posts by User-1.</p>
                            '''   
        st.sidebar.markdown(conversations_info, unsafe_allow_html=True)
        st.sidebar.markdown('<hr>', unsafe_allow_html=True)
        #When Get Conversations button is clicked
        if get_conversations_button:
            get_conversations(user1, user2)
    rich_list = st.sidebar.checkbox('Hive Rich List', value=False)
    if rich_list:
        number_of_users = st.sidebar.radio('Choose number of users in the list',('Top 10', 'Top 50', 'Top 100', 'Top 500', 'Top 1000', 'Top 10000'),0)
        asset_type = st.sidebar.radio('Choose the asset type to sort by', ('Owned HP', 'Delegated HP', 'Received HP', 'Total HP', 'Hive', 'HBD', 'Hive-Savings', 'HBD-Savings'),3)
        get_rich_list_button = st.sidebar.button('Get Rich List')
        st.sidebar.markdown('<hr>', unsafe_allow_html=True)
        #When Get Rich List button is clicked
        if get_rich_list_button:
            get_rich_list(number_of_users, asset_type)         
    delegations = st.sidebar.checkbox('HP Delegations', value=False)
    if delegations:
        delegator = st.sidebar.text_input('Hive Username:', 'freedom')
        type_of_data = st.sidebar.radio('Choose between all or active delegations', ('All delegations', 'Active delegations'),1)
        get_delegations_button = st.sidebar.button('Get Delegations')
        st.sidebar.markdown('<hr>', unsafe_allow_html=True)
        #When Delegations button is clicked
        if get_delegations_button:
            if type_of_data == 'All delegations':
                get_delegations_all(delegator)
            if type_of_data == 'Active delegations':
                get_delegations_active(delegator)


    dynamic_global_properties = st.sidebar.checkbox('Dynamic Global Properties', value=False)
    if dynamic_global_properties:
        dgp_button = st.sidebar.button('Get DGP')
        st.sidebar.markdown('<hr>', unsafe_allow_html=True)
        #When Get DGP button is clicked
        if dgp_button:
            get_dgp()        
    search_posts = st.sidebar.checkbox('Hive Posts Search', value=False)
    if search_posts:
        st.markdown('<a href="https://web-production-66a7.up.railway.app">Hive Search App</a>', unsafe_allow_html=True)

    hive_rewards = st.sidebar.checkbox('Hive Rewards', value=False)
    if hive_rewards:
        start_date = st.sidebar.empty()
        end_date = st.sidebar.empty()
        user_rewards = st.sidebar.empty()
        start = start_date.date_input(label='Start Date:')
        end = end_date.date_input(label='End Date:')
        acct = user_rewards.text_input(label='Account Name:',value='')
        order_by = st.sidebar.radio(label='Results Order:', options=["ASC", "DESC"], index=1)
        get_hbd_claimed_button = st.sidebar.button('Get Claimed HBD')
        get_hive_claimed_button = st.sidebar.button('Get Claimed Hive')
        get_vests_button = st.sidebar.button('Get Claimed Hive Power')
        get_hbd_interest_button = st.sidebar.button('Get HBD Interest')
        get_curation_rewards_button = st.sidebar.button('Get Curation Rewards')
    
        if get_hbd_claimed_button:
            get_hbd_claimed(acct, start, end, order_by)
        if get_hive_claimed_button:
            get_hive_claimed(acct, start, end, order_by)
        if get_hbd_interest_button:
            get_hbd_interest(acct, start, end, order_by)
        if get_vests_button:
            get_vests_claimed(acct, start, end, order_by)
        if get_curation_rewards_button:
            get_curation_rewards(acct, start, end, order_by)

#         date_today = dt.datetime.today()
#         date_before_today = date_today - dt.timedelta(days=10)
#         start_date = st.sidebar.date_input('Start Date:', date_before_today)
#         end_date = st.sidebar.date_input('End Date:', date_today)
#         get_posts_button = st.sidebar.button('Get Posts')
#         st.sidebar.markdown('<hr>', unsafe_allow_html=True)
#         #When Get Posts button is clicked
#         if get_posts_button:
#             get_posts()
            
    #Styling the page. Style variable is imported from a separate styles.py file
    st.sidebar.markdown(style, unsafe_allow_html=True)
