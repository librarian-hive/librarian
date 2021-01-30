import streamlit as st 
import pymssql
from PIL import Image
from styles import style
import os

def sql_command_responses(user1, user2):
    SQLCommand = '''
    SELECT body, 'https://hive.blog' + url, created
    FROM Comments
    WHERE author = '{}' 
    AND parent_author =  '{}'
    ORDER BY created DESC
    '''.format(user2, user1)
    return SQLCommand

def hive_sql(SQLCommand):
    db = os.environ['HIVESQL'].split()
    conn = pymssql.connect(server=db[0], user=db[1], password=db[2], database=db[3])
    cursor = conn.cursor()
    cursor.execute(SQLCommand)
    result = cursor.fetchmany(1000)
    conn.close()
    return result

def formatBody(txt):
    text_list = txt.split('\n\n')
    paragraphs = ''
    for i in text_list:
        if i != '' and i[0] == '>':
            i = i[1:]
            i = '<blockquote class = "quote">' + i + '</blockquote>'
        paragraph = '<p>{}</p>'.format(i)
        paragraphs = paragraphs + paragraph    
    return paragraphs

def get_responses(user1, user2):
    SQLCommand = sql_command_responses(user1, user2)
    result = hive_sql(SQLCommand)
    return result

def display_responses(result):
    count = 1
    total_res = len(result)
    total = '<div class="comment"> Total of {} responses from @{} to @{} on Hive Blockchain </div>'.format(str(total_res), user2, user1)
    st.markdown(total, unsafe_allow_html=True)
    for i in result:
        paragraphs = formatBody(i[0])
        starting = '<div class = "comment">'
        count_string = '<span class = "count_string">' + str(count) + ' - ' + '</span>'
        users = '@' + user2 + ' to @' + user1 + ' on '
        date_string = '<span class = "date_string">' + str(i[2]) + '</span><p></p>'
        body_string = '<span class = "body_string">' + paragraphs + '</span>'
        link_string = '<a class = " link_string" href = "'+ i[1] +'">(source)</a>'
        ending = '</div>'
        comment = starting + count_string + users+ date_string + body_string + link_string + ending
        st.markdown(comment, unsafe_allow_html=True)
        count += 1

if __name__ == '__main__':
    #Styling the page. Style variable is imported from a separate styles.py file
    st.markdown(style, unsafe_allow_html=True)

    #Sidebar Labels and Inputs
    user2 = st.sidebar.text_input('Hive User 1','ned')
    user1 = st.sidebar.text_input('Hive User 2','dan')
    start = st.sidebar.button('Get Responses')
    st.sidebar.write('---')
    info = 'Result will output all historic responses by Hive User-1 to Hive User-2. <p></p>'
    info += 'If User-2 left blank the output will display contents of all posts by User-1.'
    st.sidebar.markdown(info, unsafe_allow_html=True)

    #Main Content
    image = Image.open('hivesql.jpeg')
    st.image(image, use_column_width=True)

    #When Get Responses button clicked
    if start:
        result = get_responses(user1, user2)
        display_responses(result)
