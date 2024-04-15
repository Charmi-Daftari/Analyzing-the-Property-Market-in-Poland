import pandas as pd
from snowflake.sqlalchemy import URL
from sqlalchemy import create_engine
from snowflake.connector.pandas_tools import pd_writer
import time 
import gspread
from gspread_dataframe import get_as_dataframe, set_with_dataframe

# Record the start time before executing the main code
start_time = time.time()

# Create connection with Snowflake account 
engine = create_engine(URL(
    account = '######',
    user = 'charmidaftari',
    password = '#####',
    database = 'otodom',
    schema = 'public',
    warehouse = 'otodom_wh'
))

# Establish a connection to the database engine
with engine.connect() as conn:
    try:
        query= """ SELECT RN, TITLE FROM otodom_data_flatten ORDER BY RN """

        df = pd.read_sql(sql=query, con = conn.connection)

        # initializes a connection to Google Sheets
        gc = gspread.service_account()

        loop_counter = 0
        # Select a chunk of rows from the DataFrame
        chunk_size = 200
        file_name = 'OTODOM_ANALYSIS_'
        user_email = 'charmidaftari1@gmail.com'

        for i in range(0, len(df), chunk_size):
            loop_counter +=1
            df_in = df.iloc[i:(i+chunk_size), :]

            spreadsheet_title = file_name + str(loop_counter)
            try: 
                # locals() returns a dictionary containing the local symbol table.
                locals()['sh'+str(loop_counter)] = gc.open(spreadsheet_title)
            except gspread.SpreadsheetNotFound:
                 locals()['sh'+str(loop_counter)] = gc.create(spreadsheet_title)

            # Share the Google Sheets spreadsheet with the specified user email address
            # Giving the user 'writer' permission
            locals()['sh'+str(loop_counter)].share(user_email, perm_type='user', role='writer')

            # Get the first worksheet (index 0) from the Google Sheets spreadsheet
            wks = locals()['sh'+str(loop_counter)].get_worksheet(0)

            # Resize the worksheet to accommodate the chunk of data plus one additional row for headers
            wks.resize(len(df_in) + 1)

            # Write the chunk of data from the DataFrame `df_in` to the worksheet
            set_with_dataframe(wks, df_in)

            column = 'C'
            start_row = 2
            end_row = wks.row_count
            cell_range = f'{column}{start_row}:{column}{end_row}'
            curr_row = start_row
            cell_list = wks.range(cell_range)

            for cell in cell_list:
                cell.value = f'=GOOGLETRANSLATE(B{curr_row}, "pl", "en")'
                curr_row +=1

            # Update the worksheet with the modified cells
            wks.update_cells(cell_list, value_input_option='USER_ENTERED')  

            print(f'Spreadsheet {spreadsheet_title} created!')

    except Exception as e:
        print('--- Error --- ',e)
    finally:
        conn.close()
engine.dispose()

print("--- %s seconds ---" % (time.time() - start_time))




