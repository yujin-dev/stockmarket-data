import win32com.client as win32


def update_dataguide(file_name, macro_name, set_date = None):

    """ DatagGuide 엑셀 최신 데이터 vba 자동 갱신 """

    excel = win32.Dispatch("Excel.Application")  # create an instance of Excel

    book = excel.Workbooks.Open(Filename=file_name) 
           
    if set_date is not None:
        for sheet in book.Sheets:
            excel.Worksheets(sheet.name).Activate()
            ws = excel.ActiveSheet
            ws.Cells(8,2).Value = str(set_date)
            
    excel.Run(macro_name)
    book.Save()
    book.Close()
    excel.Quit()
